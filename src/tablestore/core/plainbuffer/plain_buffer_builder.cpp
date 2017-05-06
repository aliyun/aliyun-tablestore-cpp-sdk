/* 
BSD 3-Clause License

Copyright (c) 2017, Alibaba Cloud
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
#include "plain_buffer_builder.hpp"
#include "plain_buffer_crc8.hpp"
#include "../impl/ots_exception.hpp"
#include <list>
#include <string>

using namespace std;

namespace aliyun {
namespace tablestore {
namespace core {

int32_t PlainBufferBuilder::ComputePrimaryKeyValueSize(const PrimaryKeyValue& value)
{
    int32_t size = 1;   // TAG_CELL_VALUE
    size += LITTLE_ENDIAN_32_SIZE + 1;  // length + type
    switch (value.category()) {
    case PrimaryKeyValue::NONE:
        OTS_ASSERT(false)(value);
    case PrimaryKeyValue::INF_MIN: case PrimaryKeyValue::INF_MAX: case PrimaryKeyValue::AUTO_INCR:
        size += 1;
        break;
    case PrimaryKeyValue::INTEGER:
        size += sizeof(int64_t); // value size
        break;
    case PrimaryKeyValue::STRING:
        size += LITTLE_ENDIAN_32_SIZE; // length size
        size += static_cast<int32_t>(value.str().length()); // value size
        break;
    case PrimaryKeyValue::BINARY:
        size += LITTLE_ENDIAN_32_SIZE; // length size
        size += static_cast<int32_t>(value.blob().length()); // value size
        break;
    }
    return size;
}

int32_t PlainBufferBuilder::ComputeVariantValueSize(const PrimaryKeyValue& value)
{
    // no TAG_CELL_VALUE and length
    return ComputePrimaryKeyValueSize(value) - LITTLE_ENDIAN_32_SIZE - 1;   
}

int32_t PlainBufferBuilder::ComputePrimaryKeyColumnSize(const PrimaryKeyColumn& pkColumn)
{
    int32_t size = 1;   // TAG_CELL
    size += 1 + LITTLE_ENDIAN_32_SIZE;  // TAG_CELL_NAME + length
    size += static_cast<int32_t>(pkColumn.name().size());
    size += ComputePrimaryKeyValueSize(pkColumn.value());
    size += 2;  // TAG_CELL_CHECKSUM + checksum
    return size;
}

int32_t PlainBufferBuilder::ComputeColumnValueSize(const AttributeValue& value)
{
    int32_t size = 1;   // TAG_CELL_VALUE
    size += LITTLE_ENDIAN_32_SIZE + 1;  // length + type
    switch (value.category()) {
    case AttributeValue::NONE:
        break;
    case AttributeValue::INTEGER:
        size += sizeof(int64_t); // value size
        break;
    case AttributeValue::STRING:
        size += LITTLE_ENDIAN_32_SIZE; // length size
        size += static_cast<int32_t>(value.str().length()); // value size
        break;
    case AttributeValue::BINARY:
        size += LITTLE_ENDIAN_32_SIZE; // length size
        size += static_cast<int32_t>(value.blob().length()); // value size
        break;
    case AttributeValue::BOOLEAN:
        size += 1;
        break;
    case AttributeValue::FLOATING_POINT:
        size += LITTLE_ENDIAN_64_SIZE;
        break;
    }
    return size;
}

int32_t PlainBufferBuilder::ComputeVariantValueSize(const AttributeValue& value)
{
    // no TAG_CELL_VALUE and length
    return ComputeColumnValueSize(value) - LITTLE_ENDIAN_32_SIZE - 1;   
}

int32_t PlainBufferBuilder::ComputeColumnSize(const Attribute& column)
{
    int32_t size = 1;   // TAG_CELL
    size += 1 + LITTLE_ENDIAN_32_SIZE;  // TAG_CELL_NAME + length
    size += static_cast<int32_t>(column.name().size());
    size += ComputeColumnValueSize(column.value());
    if (column.timestamp().present()) {
        size += 1 + LITTLE_ENDIAN_64_SIZE;  // TAG_CELL_TIMESTAMP + ts size
    }
    size += 2;  // TAG_CELL_CHECKSUM + checksum
    return size;
}

int32_t PlainBufferBuilder::ComputeColumnSize(
    const RowUpdateChange::Update& update)
{
    int32_t size = 1; // TAG_CELL
    size += 1; // TAG_CELL_NAME
    size += LITTLE_ENDIAN_32_SIZE; // length
    size += update.attrName().size();
    switch(update.type()) {
    case RowUpdateChange::Update::PUT:
        size += ComputeColumnValueSize(*update.attrValue());
        if (update.timestamp().present()) {
            size += 1; // TAG_CELL_TIMESTAMP
            size += LITTLE_ENDIAN_64_SIZE; // ts
        }
        break;
    case RowUpdateChange::Update::DELETE:
        size += 1 + LITTLE_ENDIAN_32_SIZE + 1;  // value placeholder
        if (update.timestamp().present()) {
            size += 1 + LITTLE_ENDIAN_64_SIZE;  // TAG_CELL_TIMESTAMP + ts size
        }
        size += 2;  // TAG_CELL_TYPE + type
        break;
    case RowUpdateChange::Update::DELETE_ALL:
        size += 1 + LITTLE_ENDIAN_32_SIZE + 1;  // value placeholder
        size += 2;  // TAG_CELL_TYPE + type
        break;
    }
    size += 2;  // TAG_CELL_CHECKSUM + checksum
    return size;
}

int32_t PlainBufferBuilder::ComputePrimaryKeySize(const PrimaryKey& primaryKey)
{
    int32_t size = 1;   // TAG_ROW_PK
    for(size_t i = 0, sz = primaryKey.size(); i < sz; ++i) {
        size += ComputePrimaryKeyColumnSize(primaryKey[i]);
    }
    return size;
}

int32_t PlainBufferBuilder::ComputeRowSize(const RowPutChange& rowChange)
{
    int32_t size = LITTLE_ENDIAN_32_SIZE;  // HEADER
    size += ComputePrimaryKeySize(rowChange.primaryKey());

    if (rowChange.attributes().size() > 0) {
        size += 1;  // TAG_ROW_DATA
        for(int64_t i = 0, sz = rowChange.attributes().size(); i < sz; ++i) {
            size += ComputeColumnSize(rowChange.attributes()[i]);
        }
    }
    size += 2;  // TAG_ROW_CHECKSUM + checksum

    return size;
}

int32_t PlainBufferBuilder::ComputeRowSize(const RowUpdateChange& rowChange)
{
    int32_t size = LITTLE_ENDIAN_32_SIZE;  // HEADER
    size += ComputePrimaryKeySize(rowChange.primaryKey());

    const IVector<RowUpdateChange::Update>& updates = rowChange.updates();
    if (updates.size() > 0) {
        size += 1;  // TAG_ROW_DATA
        for(int64_t i = 0, sz = updates.size(); i < sz; ++i) {
            size += ComputeColumnSize(updates[i]);
        }
    }
    size += 2;  // TAG_ROW_CHECKSUM + checksum

    return size;
}

int32_t PlainBufferBuilder::ComputeRowSize(const RowDeleteChange& rowChange)
{
    int32_t size = LITTLE_ENDIAN_32_SIZE;  // HEADER
    size += ComputePrimaryKeySize(rowChange.primaryKey());
    size += 3;  // TAG_DELETE_MARKER + TAG_ROW_CHECKSUM + checksum
    return size;
}

string PlainBufferBuilder::SerializePrimaryKeyValue(const PrimaryKeyValue& value)
{
    int32_t bufSize = ComputeVariantValueSize(value);
    PlainBufferOutputStream outputStream(bufSize);
    PlainBufferCodedOutputStream codedOutputStream(&outputStream);
    
    codedOutputStream.WritePrimaryKeyValue(value);
    return outputStream.GetBuffer();
}

string PlainBufferBuilder::SerializeColumnValue(const AttributeValue& value)
{
    int32_t bufSize = ComputeVariantValueSize(value);
    PlainBufferOutputStream outputStream(bufSize);
    PlainBufferCodedOutputStream codedOutputStream(&outputStream);
    
    codedOutputStream.WriteColumnValue(value);
    return outputStream.GetBuffer();
}

string PlainBufferBuilder::SerializePrimaryKey(const PrimaryKey& primaryKey)
{
    int32_t bufSize = LITTLE_ENDIAN_32_SIZE;    // HEADER
    bufSize += ComputePrimaryKeySize(primaryKey);
    bufSize += 2;   // TAG_ROW_CHECKSUM + checksum

    PlainBufferOutputStream outputStream(bufSize);
    PlainBufferCodedOutputStream codedOutputStream(&outputStream);

    int8_t rowChecksum = 0;
    codedOutputStream.WriteHeader();
    codedOutputStream.WritePrimaryKey(primaryKey, &rowChecksum);
    rowChecksum = PlainBufferCrc8::CrcInt8(rowChecksum, 0); // no DeleteMarker
    codedOutputStream.WriteRowChecksum(rowChecksum);
    return outputStream.GetBuffer();
}

string PlainBufferBuilder::SerializeForRow(const RowPutChange& rowChange)
{
    int32_t bufSize = ComputeRowSize(rowChange);
    PlainBufferOutputStream outputStream(bufSize);
    PlainBufferCodedOutputStream codedOutputStream(&outputStream);

    int8_t rowChecksum = 0;
    codedOutputStream.WriteHeader();
    codedOutputStream.WritePrimaryKey(rowChange.primaryKey(), &rowChecksum);
    codedOutputStream.WriteColumns(rowChange.attributes(), &rowChecksum);
    rowChecksum = PlainBufferCrc8::CrcInt8(rowChecksum, 0); // no DeleteMarker
    codedOutputStream.WriteRowChecksum(rowChecksum);
    return outputStream.GetBuffer();
}

string PlainBufferBuilder::SerializeForRow(const RowUpdateChange& rowChange)
{
    int32_t bufSize = ComputeRowSize(rowChange);
    PlainBufferOutputStream outputStream(bufSize);
    PlainBufferCodedOutputStream codedOutputStream(&outputStream);

    int8_t rowChecksum = 0;
    codedOutputStream.WriteHeader();
    codedOutputStream.WritePrimaryKey(rowChange.primaryKey(), &rowChecksum);
    codedOutputStream.WriteColumns(rowChange.updates(), &rowChecksum);
    rowChecksum = PlainBufferCrc8::CrcInt8(rowChecksum, 0); // no DeleteMarker
    codedOutputStream.WriteRowChecksum(rowChecksum);
    return outputStream.GetBuffer();
}

string PlainBufferBuilder::SerializeForRow(const RowDeleteChange& rowChange)
{
    int32_t bufSize = ComputeRowSize(rowChange);
    PlainBufferOutputStream outputStream(bufSize);
    PlainBufferCodedOutputStream codedOutputStream(&outputStream);

    int8_t rowChecksum = 0;
    codedOutputStream.WriteHeader();
    codedOutputStream.WritePrimaryKey(rowChange.primaryKey(), &rowChecksum);
    codedOutputStream.WriteDeleteMarker(&rowChecksum);
    codedOutputStream.WriteRowChecksum(rowChecksum);
    return outputStream.GetBuffer();
}

} // namespace core
} // namespace tablestore
} // namespace aliyun
