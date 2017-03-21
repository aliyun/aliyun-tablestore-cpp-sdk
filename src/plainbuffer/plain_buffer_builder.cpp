/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#include "plain_buffer_builder.h"
#include "plain_buffer_crc8.h"
#include "ots/ots_exception.h"
#include <list>
#include <string>

using namespace std;

namespace aliyun {
namespace tablestore {

int32_t PlainBufferBuilder::ComputePrimaryKeyValueSize(const PrimaryKeyValue& value)
{
    int32_t size = 1;   // TAG_CELL_VALUE
    size += LITTLE_ENDIAN_32_SIZE + 1;  // length + type
    if (value.IsInfMin() || value.IsInfMax() || value.IsPlaceholderForAutoIncrement()) {
        size += 1;
        return size;
    }
    switch (value.GetType()) {
        case PKT_INTEGER:
            size += sizeof(int64_t); // value size
            break;
        case PKT_STRING:
            size += LITTLE_ENDIAN_32_SIZE; // length size
            size += static_cast<int32_t>(value.AsString().size()); // value size
            break;
        case PKT_BINARY:
            size += LITTLE_ENDIAN_32_SIZE; // length size
            size += static_cast<int32_t>(value.AsBinary().Size()); // value size
            break;
        default:
            throw OTSClientException(
                    "Unsupported primary key type: " + 
                    OTSHelper::PrimaryKeyTypeToString(value.GetType()));
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
    size += static_cast<int32_t>(pkColumn.GetName().size());
    size += ComputePrimaryKeyValueSize(pkColumn.GetValue());
    size += 2;  // TAG_CELL_CHECKSUM + checksum
    return size;
}

int32_t PlainBufferBuilder::ComputeColumnValueSize(const ColumnValue& value)
{
    int32_t size = 1;   // TAG_CELL_VALUE
    size += LITTLE_ENDIAN_32_SIZE + 1;  // length + type
    switch (value.GetType()) {
        case CT_INTEGER:
            size += sizeof(int64_t); // value size
            break;
        case CT_STRING:
            size += LITTLE_ENDIAN_32_SIZE; // length size
            size += static_cast<int32_t>(value.AsString().size()); // value size
            break;
        case CT_BINARY:
            size += LITTLE_ENDIAN_32_SIZE; // length size
            size += static_cast<int32_t>(value.AsBinary().Size()); // value size
            break;
        case CT_BOOLEAN:
            size += 1;
            break;
        case CT_DOUBLE:
            size += LITTLE_ENDIAN_64_SIZE;
            break;
        default:
            throw OTSClientException(
                "Unsupported column type: " + OTSHelper::ColumnTypeToString(value.GetType()));
    }
    return size;
}

int32_t PlainBufferBuilder::ComputeVariantValueSize(const ColumnValue& value)
{
    // no TAG_CELL_VALUE and length
    return ComputeColumnValueSize(value) - LITTLE_ENDIAN_32_SIZE - 1;   
}

int32_t PlainBufferBuilder::ComputeColumnSize(const Column& column)
{
    int32_t size = 1;   // TAG_CELL
    size += 1 + LITTLE_ENDIAN_32_SIZE;  // TAG_CELL_NAME + length
    size += static_cast<int32_t>(column.GetName().size());
    if (column.HasValue()) {
        size += ComputeColumnValueSize(column.GetValue());
    }
    if (column.HasTimestamp()) {
        size += 1 + LITTLE_ENDIAN_64_SIZE;  // TAG_CELL_TIMESTAMP + ts size
    }
    size += 2;  // TAG_CELL_CHECKSUM + checksum
    return size;
}

int32_t PlainBufferBuilder::ComputeColumnSize(const Column& column, RowUpdateChange::UpdateType type)
{
    int32_t size = ComputeColumnSize(column);
    if (type == RowUpdateChange::DELETE || type == RowUpdateChange::DELETE_ALL) {
        size += 2;  // TAG_CELL_TYPE + type
    }
    return size;
}

int32_t PlainBufferBuilder::ComputePrimaryKeySize(const PrimaryKey& primaryKey)
{
    int32_t size = 1;   // TAG_ROW_PK
    for(size_t i = 0, sz = primaryKey.GetPrimaryKeyColumnsSize(); i < sz; ++i) {
        size += ComputePrimaryKeyColumnSize(primaryKey.GetColumn(i));
    }
    return size;
}

int32_t PlainBufferBuilder::ComputeRowSize(const RowPutChange& rowChange)
{
    int32_t size = LITTLE_ENDIAN_32_SIZE;  // HEADER
    size += ComputePrimaryKeySize(rowChange.GetPrimaryKey());

    // columns
    const list<Column>& columns = rowChange.GetColumns();
    if (!columns.empty()) {
        size += 1;  // TAG_ROW_DATA
        for (typeof(columns.begin()) iter = columns.begin(); iter != columns.end(); ++iter) {
            size += ComputeColumnSize(*iter);
        }
    }
    size += 2;  // TAG_ROW_CHECKSUM + checksum

    return size;
}

int32_t PlainBufferBuilder::ComputeRowSize(const RowUpdateChange& rowChange)
{
    int32_t size = LITTLE_ENDIAN_32_SIZE;  // HEADER
    size += ComputePrimaryKeySize(rowChange.GetPrimaryKey());

    // columns
    const list<Column>& columns = rowChange.GetColumns();
    const list<RowUpdateChange::UpdateType>& updateTypes = rowChange.GetUpdateTypes();
    if (!columns.empty()) {
        size += 1;  // TAG_ROW_DATA
        typeof(columns.begin()) columnIter = columns.begin();
        typeof(updateTypes.begin()) typeIter = updateTypes.begin();
        for (; columnIter != columns.end() && typeIter != updateTypes.end(); ++columnIter, ++typeIter) {
            size += ComputeColumnSize(*columnIter, *typeIter);
        }
    }
    size += 2;  // TAG_ROW_CHECKSUM + checksum

    return size;
}

int32_t PlainBufferBuilder::ComputeRowSize(const RowDeleteChange& rowChange)
{
    int32_t size = LITTLE_ENDIAN_32_SIZE;  // HEADER
    size += ComputePrimaryKeySize(rowChange.GetPrimaryKey());
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

string PlainBufferBuilder::SerializeColumnValue(const ColumnValue& value)
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
    codedOutputStream.WritePrimaryKey(rowChange.GetPrimaryKey(), &rowChecksum);
    codedOutputStream.WriteColumns(rowChange.GetColumns(), &rowChecksum);
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
    codedOutputStream.WritePrimaryKey(rowChange.GetPrimaryKey(), &rowChecksum);
    codedOutputStream.WriteColumns(rowChange.GetColumns(), rowChange.GetUpdateTypes(), &rowChecksum);
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
    codedOutputStream.WritePrimaryKey(rowChange.GetPrimaryKey(), &rowChecksum);
    codedOutputStream.WriteDeleteMarker(&rowChecksum);
    codedOutputStream.WriteRowChecksum(rowChecksum);
    return outputStream.GetBuffer();
}

} // end of tablestore
} // end of aliyun
