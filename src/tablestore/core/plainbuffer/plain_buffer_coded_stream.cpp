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
#include "plain_buffer_coded_stream.hpp"
#include "plain_buffer_crc8.hpp"
#include "tablestore/util/mempiece.hpp"
#include "tablestore/util/move.hpp"
#include "tablestore/util/prettyprint.hpp"
#include <iostream>

using namespace std;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {

namespace {

int64_t DoubleToRawBits(double value)
{
    union DblInt
    {
        int64_t intValue;
        double dblValue;
    };
    DblInt dblint;
    dblint.dblValue = value;
    return dblint.intValue;
}

} // namespace

PlainBufferCodedInputStream::PlainBufferCodedInputStream(PlainBufferInputStream* inputStream)
{
    OTS_ASSERT(inputStream != NULL).what("Bug: PlainBufferInputStream is null.");
    mInputStream = inputStream;
}

int32_t PlainBufferCodedInputStream::ReadTag()
{
    return mInputStream->ReadTag();
}

bool PlainBufferCodedInputStream::CheckLastTagWas(int32_t tag)
{
    return mInputStream->CheckLastTagWas(tag);
}

int32_t PlainBufferCodedInputStream::GetLastTag()
{
    return mInputStream->GetLastTag();
}

int32_t PlainBufferCodedInputStream::ReadHeader()
{
    return mInputStream->ReadInt32();
}

void PlainBufferCodedInputStream::ReadPrimaryKeyValue(
    IVector<string>* holder,
    PrimaryKeyValue* result,
    int8_t* cellChecksum)
{
    if (!CheckLastTagWas(TAG_CELL_VALUE)) {
        throw OTSClientException(
            "Expect TAG_CELL_VALUE but it was " + pp::prettyPrint(GetLastTag()));
    }
    mInputStream->ReadRawLittleEndian32();
    int8_t type = mInputStream->ReadRawByte();
    switch (type) {
    case VT_INTEGER: {
        int64_t int64Value = mInputStream->ReadInt64();
        *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_INTEGER);
        *cellChecksum = PlainBufferCrc8::CrcInt64(*cellChecksum, int64Value);
        ReadTag();
        *result = PrimaryKeyValue::toInteger(int64Value);
        break;
    }
    case VT_STRING: {
        int32_t valueSize = mInputStream->ReadInt32();
        holder->append() = mInputStream->ReadUTFString(valueSize);
        MemPiece val = MemPiece::from(holder->back());
        *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_STRING);
        *cellChecksum = PlainBufferCrc8::CrcInt32(*cellChecksum, valueSize);
        *cellChecksum = PlainBufferCrc8::CrcString(*cellChecksum, val);
        ReadTag();
        *result = PrimaryKeyValue::toStr(val);
        break;
    }
    case VT_BLOB: {
        int32_t valueSize = mInputStream->ReadInt32();
        holder->append() = mInputStream->ReadBytes(valueSize);
        MemPiece val = MemPiece::from(holder->back());
        *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_BLOB);
        *cellChecksum = PlainBufferCrc8::CrcInt32(*cellChecksum, valueSize);
        *cellChecksum = PlainBufferCrc8::CrcString(*cellChecksum, val);
        ReadTag();
        *result = PrimaryKeyValue::toBlob(val);
        break;
    }
    }
}

void PlainBufferCodedInputStream::ReadColumnValue(
    IVector<string>* holder,
    AttributeValue* column,
    int8_t* cellChecksum)
{
    if (!CheckLastTagWas(TAG_CELL_VALUE)) {
        throw OTSClientException(
            "Expect TAG_CELL_VALUE but it was " + pp::prettyPrint(GetLastTag()));
    }
    mInputStream->ReadRawLittleEndian32();
    int8_t type = mInputStream->ReadRawByte();
    switch (type) {
    case VT_INTEGER: {
        int64_t int64Value = mInputStream->ReadInt64();
        *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_INTEGER);
        *cellChecksum = PlainBufferCrc8::CrcInt64(*cellChecksum, int64Value);
        ReadTag();
        *column = AttributeValue::toInteger(int64Value);
        break;
    }
    case VT_STRING: {
        int32_t valueSize = mInputStream->ReadInt32();
        holder->append() = mInputStream->ReadUTFString(valueSize);
        MemPiece val = MemPiece::from(holder->back());
        *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_STRING);
        *cellChecksum = PlainBufferCrc8::CrcInt32(*cellChecksum, valueSize);
        *cellChecksum = PlainBufferCrc8::CrcString(*cellChecksum, val);
        ReadTag();
        *column = AttributeValue::toStr(val);
        break;
    }
    case VT_BLOB: {
        int32_t valueSize = mInputStream->ReadInt32();
        holder->append() = mInputStream->ReadBytes(valueSize);
        MemPiece val = MemPiece::from(holder->back());
        *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_BLOB);
        *cellChecksum = PlainBufferCrc8::CrcInt32(*cellChecksum, valueSize);
        *cellChecksum = PlainBufferCrc8::CrcString(*cellChecksum, val);
        ReadTag();
        *column = AttributeValue::toBlob(val);
        break;
    }
    case VT_BOOLEAN: {
        bool boolValue = mInputStream->ReadBoolean();
        *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_BOOLEAN);
        int8_t booleanInt8 = (boolValue) ? 1 : 0;
        *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, booleanInt8);
        ReadTag();
        *column = AttributeValue::toBoolean(boolValue);
        break;
    }
    case VT_DOUBLE: {
        double doubleValue = mInputStream->ReadDouble();
        int64_t doubleInt64 = DoubleToRawBits(doubleValue);
        *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_DOUBLE);
        *cellChecksum = PlainBufferCrc8::CrcInt64(*cellChecksum, doubleInt64);
        ReadTag();
        *column = AttributeValue::toFloatPoint(doubleValue);
        break;
    }
    }
}

void PlainBufferCodedInputStream::ReadPrimaryKeyColumn(
    IVector<string>* holder,
    PrimaryKeyColumn* column,
    int8_t* rowChecksum)
{
    if (!CheckLastTagWas(TAG_CELL)) {
        throw OTSClientException(
            "Expect TAG_CELL but it was " + pp::prettyPrint(GetLastTag()));
    }
    ReadTag();

    if (!CheckLastTagWas(TAG_CELL_NAME)) {
        throw OTSClientException(
            "Expect TAG_CELL_NAME but it was " + pp::prettyPrint(GetLastTag()));
    }
    int8_t cellChecksum = 0;
    int32_t nameSize = mInputStream->ReadRawLittleEndian32();
    string columnName = mInputStream->ReadUTFString(nameSize);
    cellChecksum = PlainBufferCrc8::CrcString(cellChecksum, MemPiece::from(columnName));
    ReadTag();

    if (!CheckLastTagWas(TAG_CELL_VALUE)) {
        throw OTSClientException(
            "Expect TAG_CELL_VALUE E but it was " + pp::prettyPrint(GetLastTag()));
    }
    PrimaryKeyValue primaryKeyValue;
    ReadPrimaryKeyValue(holder, &primaryKeyValue, &cellChecksum);
    PrimaryKeyColumn primaryKeyColumn;
    *primaryKeyColumn.mutableName() = columnName;
    moveAssign(primaryKeyColumn.mutableValue(), util::move(primaryKeyValue));

    if (GetLastTag() == TAG_CELL_CHECKSUM) {
        int8_t checksum = mInputStream->ReadRawByte();
        if (checksum != cellChecksum) {
            // add log
            throw OTSClientException("Checksum mismatch.");
        }
        ReadTag();
    } else {
        throw OTSClientException(
            "Expect TAG_CELL_CHECKSUM but it was " + pp::prettyPrint(GetLastTag()));
    }
    *rowChecksum = PlainBufferCrc8::CrcInt8(*rowChecksum, cellChecksum);

    moveAssign(column, util::move(primaryKeyColumn));
}

void PlainBufferCodedInputStream::ReadColumn(
    IVector<string>* holder, Attribute* column, int8_t* rowChecksum)
{
    if (!CheckLastTagWas(TAG_CELL)) {
        throw OTSClientException(
            "Expect TAG_CELL but it was " + pp::prettyPrint(GetLastTag()));
    }
    ReadTag();

    if (!CheckLastTagWas(TAG_CELL_NAME)) {
        throw OTSClientException(
            "Expect TAG_CELL_NAME but it was " + pp::prettyPrint(GetLastTag()));
    }
    int8_t cellChecksum = 0;
    int32_t nameSize = mInputStream->ReadRawLittleEndian32();
    string columnName = mInputStream->ReadUTFString(nameSize);
    Attribute tmpColumn;
    *tmpColumn.mutableName() = columnName;
    cellChecksum = PlainBufferCrc8::CrcString(cellChecksum, MemPiece::from(columnName));
    ReadTag();

    if (GetLastTag() == TAG_CELL_VALUE) {
        AttributeValue val;
        ReadColumnValue(holder, &val, &cellChecksum);
        moveAssign(tmpColumn.mutableValue(), util::move(val));
    }

    // skip CELL_TYPE
    if (GetLastTag() == TAG_CELL_TYPE) {
        int8_t cellType = mInputStream->ReadRawByte();
        cellChecksum = PlainBufferCrc8::CrcInt8(cellChecksum, cellType);
        ReadTag();
    }

    if (GetLastTag() == TAG_CELL_TIMESTAMP) {
        int64_t timestamp = mInputStream->ReadInt64();
        *tmpColumn.mutableTimestamp() = UtcTime::fromMsec(timestamp);
        cellChecksum = PlainBufferCrc8::CrcInt64(cellChecksum, timestamp);
        ReadTag(); 
    }

    if (GetLastTag() == TAG_CELL_CHECKSUM) {
        int8_t checksum = mInputStream->ReadRawByte();
        if (checksum != cellChecksum) {
            // add log
            throw OTSClientException("Checksum mismatch.");
        }
        ReadTag();
    } else {
        throw OTSClientException(
            "Expect TAG_CELL_CHECKSUM but it was " + pp::prettyPrint(GetLastTag()));
    }
    *rowChecksum = PlainBufferCrc8::CrcInt8(*rowChecksum, cellChecksum);

    moveAssign(column, util::move(tmpColumn));
}

void PlainBufferCodedInputStream::ReadRowWithoutHeader(
    IVector<string>* holder,
    Row* row)
{
    row->reset();
    int8_t rowChecksum = 0;

    // parse primary key.
    if (!CheckLastTagWas(TAG_ROW_PK)) {
        throw OTSClientException(
            "Expect TAG_ROW_PK but it was " + pp::prettyPrint(GetLastTag()));
    }
    ReadTag();
    while (CheckLastTagWas(TAG_CELL)) {
        PrimaryKeyColumn primaryKeyColumn;
        ReadPrimaryKeyColumn(holder, &primaryKeyColumn, &rowChecksum);
        moveAssign(&row->mutablePrimaryKey()->append(), util::move(primaryKeyColumn));
    }

    // parse columns.
    if (CheckLastTagWas(TAG_ROW_DATA)) {
        ReadTag();
        while (CheckLastTagWas(TAG_CELL)) {
            Attribute column;
            ReadColumn(holder, &column, &rowChecksum);
            moveAssign(&row->mutableAttributes()->append(), util::move(column));
        }
    }

    // skip row delete marker.
    if (CheckLastTagWas(TAG_DELETE_ROW_MARKER)) {
        ReadTag();
        rowChecksum = PlainBufferCrc8::CrcInt8(rowChecksum, 1);
    } else {
        rowChecksum = PlainBufferCrc8::CrcInt8(rowChecksum, 0);
    }

    if (CheckLastTagWas(TAG_ROW_CHECKSUM)) {
        int8_t checksum = mInputStream->ReadRawByte();
        if (checksum != rowChecksum) {
            // add log
            throw OTSClientException("Checksum is mismatch.");
        }
        ReadTag();
    } else {
        throw OTSClientException(
            "Expect TAG_ROW_CHECKSUM but it was " + pp::prettyPrint(GetLastTag()));
    }
}

void PlainBufferCodedInputStream::ReadRow(IVector<string>* holder, Row* row)
{
    if (ReadHeader() != HEADER) {
        throw OTSClientException("Invalid header from plain buffer.");
    }
    ReadTag();
    ReadRowWithoutHeader(holder, row);
}

void PlainBufferCodedInputStream::ReadRows(
    IVector<string>* holder,
    IVector<Row>* rows)
{
    if (ReadHeader() != HEADER) {
        // add log
        throw OTSClientException("Invalid header from plain buffer.");
    }
    ReadTag();

    // read all rows.
    while (!mInputStream->IsAtEnd()) {
        Row row;
        ReadRowWithoutHeader(holder, &row);
        moveAssign(&rows->append(), util::move(row));
    }
}


PlainBufferCodedOutputStream::PlainBufferCodedOutputStream(
    PlainBufferOutputStream* outputStream)
{
    if (outputStream == NULL) {
        throw OTSClientException("The PlainBufferOutputStream is null.");
    }
    mOutputStream = outputStream;
}

void PlainBufferCodedOutputStream::WriteHeader()
{
    mOutputStream->WriteRawLittleEndian32(HEADER);
}

void PlainBufferCodedOutputStream::WriteTag(int8_t tag)
{
    mOutputStream->WriteRawByte(tag);
}

void PlainBufferCodedOutputStream::WriteCellName(
    const string& name,
    int8_t* cellChecksum)
{
    WriteTag(TAG_CELL_NAME);
    mOutputStream->WriteRawLittleEndian32(name.size());
    mOutputStream->WriteBytes(MemPiece::from(name));
    *cellChecksum = PlainBufferCrc8::CrcString(*cellChecksum, MemPiece::from(name));
}

void PlainBufferCodedOutputStream::WritePrimaryKeyValue(
    const PrimaryKeyValue& value,
    int8_t* cellChecksum)
{
    WriteTag(TAG_CELL_VALUE);

    switch (value.category()) {
    case PrimaryKeyValue::NONE:
        OTS_ASSERT(false)(value);
    case PrimaryKeyValue::INF_MIN:
        mOutputStream->WriteRawLittleEndian32(1);
        mOutputStream->WriteRawByte(VT_INF_MIN);
        *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_INF_MIN);
        break;
    case PrimaryKeyValue::INF_MAX:
        mOutputStream->WriteRawLittleEndian32(1);
        mOutputStream->WriteRawByte(VT_INF_MAX);
        *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_INF_MAX);
        break;
    case PrimaryKeyValue::AUTO_INCR:
        mOutputStream->WriteRawLittleEndian32(1);
        mOutputStream->WriteRawByte(VT_AUTO_INCREMENT);
        *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_AUTO_INCREMENT);
        break;
    case PrimaryKeyValue::INTEGER: 
        mOutputStream->WriteRawLittleEndian32(1 + LITTLE_ENDIAN_64_SIZE);
        mOutputStream->WriteRawByte(VT_INTEGER);
        mOutputStream->WriteRawLittleEndian64(value.integer());
        *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_INTEGER);
        *cellChecksum = PlainBufferCrc8::CrcInt64(*cellChecksum, value.integer());
        break;
    case PrimaryKeyValue::STRING: {
        const MemPiece& stringValue = value.str();
        int32_t prefixLength = LITTLE_ENDIAN_32_SIZE + 1; // length + type
        mOutputStream->WriteRawLittleEndian32(prefixLength + stringValue.length());  // length + type + value
        mOutputStream->WriteRawByte(VT_STRING);
        mOutputStream->WriteRawLittleEndian32(stringValue.length());
        mOutputStream->WriteBytes(stringValue);
        *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_STRING);
        *cellChecksum = PlainBufferCrc8::CrcInt32(*cellChecksum, static_cast<int32_t>(stringValue.length()));
        *cellChecksum = PlainBufferCrc8::CrcString(*cellChecksum, stringValue);
        break;
    }
    case PrimaryKeyValue::BINARY: {
        const MemPiece& blob = value.blob();
        int32_t prefixLength = LITTLE_ENDIAN_32_SIZE + 1;    // length + type
        mOutputStream->WriteRawLittleEndian32(prefixLength + blob.length()); // length + type + value
        mOutputStream->WriteRawByte(VT_BLOB);
        mOutputStream->WriteRawLittleEndian32(blob.length());
        mOutputStream->WriteBytes(blob);
        *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_BLOB);
        *cellChecksum = PlainBufferCrc8::CrcInt32(*cellChecksum, static_cast<int32_t>(blob.length()));
        *cellChecksum = PlainBufferCrc8::CrcString(*cellChecksum, blob);
        break;
    }
    }
}

void PlainBufferCodedOutputStream::WritePrimaryKeyValue(const PrimaryKeyValue& value)
{
    switch (value.category()) {
    case PrimaryKeyValue::NONE:
        OTS_ASSERT(false)(value);
    case PrimaryKeyValue::INF_MIN:
        mOutputStream->WriteRawByte(VT_INF_MIN);
        break;
    case PrimaryKeyValue::INF_MAX:
        mOutputStream->WriteRawByte(VT_INF_MAX);
        break;
    case PrimaryKeyValue::AUTO_INCR:
        mOutputStream->WriteRawByte(VT_AUTO_INCREMENT);
        break;
    case PrimaryKeyValue::INTEGER: {
        mOutputStream->WriteRawByte(VT_INTEGER);
        mOutputStream->WriteRawLittleEndian64(value.integer());
        break;
    }
    case PrimaryKeyValue::STRING: {
        const MemPiece& stringValue = value.str();
        mOutputStream->WriteRawByte(VT_STRING);
        mOutputStream->WriteRawLittleEndian32(stringValue.length());
        mOutputStream->WriteBytes(stringValue);
        break;
    }
    case PrimaryKeyValue::BINARY: {
        const MemPiece& binaryValue = value.blob();
        mOutputStream->WriteRawByte(VT_BLOB);
        mOutputStream->WriteRawLittleEndian32(binaryValue.length());
        mOutputStream->WriteBytes(binaryValue);
        break;
    }
    }
}

void PlainBufferCodedOutputStream::WriteColumnValue(
    const AttributeValue& value,
    int8_t* cellChecksum)
{
    WriteTag(TAG_CELL_VALUE);
    switch (value.category()) {
    case AttributeValue::NONE:
        OTS_ASSERT(false)(value);
        break;
    case AttributeValue::INTEGER: {
        mOutputStream->WriteRawLittleEndian32(1 + LITTLE_ENDIAN_64_SIZE);
        mOutputStream->WriteRawByte(VT_INTEGER);
        mOutputStream->WriteRawLittleEndian64(value.integer());
        *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_INTEGER);
        *cellChecksum = PlainBufferCrc8::CrcInt64(*cellChecksum, value.integer());
        break;
    }
    case AttributeValue::STRING: {
        const MemPiece& stringValue = value.str();
        int32_t prefixLength = LITTLE_ENDIAN_32_SIZE + 1;   // length + type
        mOutputStream->WriteRawLittleEndian32(prefixLength + stringValue.length()); // length + type + value
        mOutputStream->WriteRawByte(VT_STRING);
        mOutputStream->WriteRawLittleEndian32(stringValue.length());
        mOutputStream->WriteBytes(stringValue);
        *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_STRING);
        *cellChecksum = PlainBufferCrc8::CrcInt32(*cellChecksum, static_cast<int32_t>(stringValue.length()));
        *cellChecksum = PlainBufferCrc8::CrcString(*cellChecksum, stringValue);
        break;
    }
    case AttributeValue::BINARY: {
        const MemPiece& binaryValue = value.blob();
        int32_t prefixLength = LITTLE_ENDIAN_32_SIZE + 1;   // length + type
        mOutputStream->WriteRawLittleEndian32(prefixLength + binaryValue.length()); // length + type
        mOutputStream->WriteRawByte(VT_BLOB);
        mOutputStream->WriteRawLittleEndian32(binaryValue.length());
        mOutputStream->WriteBytes(binaryValue);
        *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_BLOB);
        *cellChecksum = PlainBufferCrc8::CrcInt32(*cellChecksum, static_cast<int32_t>(binaryValue.length()));
        *cellChecksum = PlainBufferCrc8::CrcString(*cellChecksum, binaryValue);
        break;
    }
    case AttributeValue::BOOLEAN: {
        mOutputStream->WriteRawLittleEndian32(2);
        mOutputStream->WriteRawByte(VT_BOOLEAN);
        mOutputStream->WriteBoolean(value.boolean());
        *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_BOOLEAN);
        int8_t booleanInt8 = (value.boolean()) ? 1 : 0;
        *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, booleanInt8);
        break;
    }
    case AttributeValue::FLOATING_POINT: {
        mOutputStream->WriteRawLittleEndian32(1 + LITTLE_ENDIAN_64_SIZE);
        mOutputStream->WriteRawByte(VT_DOUBLE);
        mOutputStream->WriteDouble(value.floatPoint());
        int64_t doubleInt64 = DoubleToRawBits(value.floatPoint());
        *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_DOUBLE);
        *cellChecksum = PlainBufferCrc8::CrcInt64(*cellChecksum, doubleInt64);
        break;
    }
    }
}

void PlainBufferCodedOutputStream::WriteColumnValue(const AttributeValue& value)
{
    switch (value.category()) {
    case AttributeValue::NONE:
        OTS_ASSERT(false)(value);
        break;
    case AttributeValue::INTEGER: 
        mOutputStream->WriteRawByte(VT_INTEGER);
        mOutputStream->WriteRawLittleEndian64(value.integer());
        break;
    case AttributeValue::STRING: {
        const MemPiece& stringValue = value.str();
        mOutputStream->WriteRawByte(VT_STRING);
        mOutputStream->WriteRawLittleEndian32(stringValue.length());
        mOutputStream->WriteBytes(stringValue);
        break;
    }
    case AttributeValue::BINARY: {
        const MemPiece& blob = value.blob();
        mOutputStream->WriteRawByte(VT_BLOB);
        mOutputStream->WriteRawLittleEndian32(blob.length());
        mOutputStream->WriteBytes(blob);
        break;
    }
    case AttributeValue::BOOLEAN: {
        mOutputStream->WriteRawByte(VT_BOOLEAN);
        mOutputStream->WriteBoolean(value.boolean());
        break;
    }
    case AttributeValue::FLOATING_POINT: {
        mOutputStream->WriteRawByte(VT_DOUBLE);
        mOutputStream->WriteDouble(value.floatPoint());
        break;
    }
    }
}

void PlainBufferCodedOutputStream::WritePrimaryKeyColumn(
    const PrimaryKeyColumn& pkeycol,
    int8_t* rowChecksum)
{
    int8_t cellChecksum = 0;
    WriteTag(TAG_CELL);
    WriteCellName(pkeycol.name(), &cellChecksum);
    WritePrimaryKeyValue(pkeycol.value(), &cellChecksum);
    WriteTag(TAG_CELL_CHECKSUM);
    mOutputStream->WriteRawByte(cellChecksum);
    *rowChecksum = PlainBufferCrc8::CrcInt8(*rowChecksum, cellChecksum);
}

void PlainBufferCodedOutputStream::WriteColumn(
    const Attribute& column,
    int8_t* rowChecksum)
{
    int8_t cellChecksum = 0;
    WriteTag(TAG_CELL);
    WriteCellName(column.name(), &cellChecksum);
    WriteColumnValue(column.value(), &cellChecksum);
    if (column.timestamp().present()) {
        WriteTag(TAG_CELL_TIMESTAMP);
        mOutputStream->WriteRawLittleEndian64(column.timestamp()->toMsec());
        cellChecksum = PlainBufferCrc8::CrcInt64(cellChecksum,
            column.timestamp()->toMsec());
    }
    WriteTag(TAG_CELL_CHECKSUM);
    mOutputStream->WriteRawByte(cellChecksum);
    *rowChecksum = PlainBufferCrc8::CrcInt8(*rowChecksum, cellChecksum);
}

void PlainBufferCodedOutputStream::WriteColumn(
    const RowUpdateChange::Update& update,
    int8_t* rowChecksum)
{
    int8_t cellChecksum = 0;
    WriteTag(TAG_CELL);
    WriteCellName(update.attrName(), &cellChecksum);
    if (update.attrValue().present()) {
        WriteColumnValue(*update.attrValue(), &cellChecksum);
    }

    switch(update.type()) {
    case RowUpdateChange::Update::PUT:
        break;
    case RowUpdateChange::Update::DELETE:
        WriteTag(TAG_CELL_TYPE);
        mOutputStream->WriteRawByte(DELETE_ONE_VERSION);
        break;
    case RowUpdateChange::Update::DELETE_ALL:
        WriteTag(TAG_CELL_TYPE);
        mOutputStream->WriteRawByte(DELETE_ALL_VERSION);
        break;
    }

    if (update.timestamp().present()) {
        WriteTag(TAG_CELL_TIMESTAMP);
        mOutputStream->WriteRawLittleEndian64(update.timestamp()->toMsec());
    }

    // the order of fields is different with the order of crc
    if (update.timestamp().present()) {
        cellChecksum = PlainBufferCrc8::CrcInt64(
            cellChecksum, update.timestamp()->toMsec());
    }

    switch(update.type()) {
    case RowUpdateChange::Update::PUT:
        break;
    case RowUpdateChange::Update::DELETE:
        cellChecksum = PlainBufferCrc8::CrcInt8(cellChecksum, DELETE_ONE_VERSION);
        break;
    case RowUpdateChange::Update::DELETE_ALL:
        cellChecksum = PlainBufferCrc8::CrcInt8(cellChecksum, DELETE_ALL_VERSION);
        break;
    }

    WriteTag(TAG_CELL_CHECKSUM);
    mOutputStream->WriteRawByte(cellChecksum);
    *rowChecksum = PlainBufferCrc8::CrcInt8(*rowChecksum, cellChecksum);
}

void PlainBufferCodedOutputStream::WritePrimaryKey(
    const PrimaryKey& pkey,
    int8_t* rowChecksum)
{
    WriteTag(TAG_ROW_PK);
    for(int64_t i = 0, sz = pkey.size(); i < sz; ++i) {
        WritePrimaryKeyColumn(pkey[i], rowChecksum);
    }
}

void PlainBufferCodedOutputStream::WriteColumns(
    const IVector<Attribute>& columns,
    int8_t* rowChecksum)
{
    if (columns.size() > 0) {
        WriteTag(TAG_ROW_DATA);
        for(int64_t i = 0, sz = columns.size(); i < sz; ++i) {
            WriteColumn(columns[i], rowChecksum);
        }
    }
}

void PlainBufferCodedOutputStream::WriteColumns(
    const IVector<RowUpdateChange::Update>& attrs,
    int8_t* rowChecksum)
{
    if (attrs.size() > 0) {
        WriteTag(TAG_ROW_DATA);
        for(int64_t i = 0, sz = attrs.size(); i < sz; ++i) {
            WriteColumn(attrs[i], rowChecksum);
        }
    }
}

void PlainBufferCodedOutputStream::WriteDeleteMarker(int8_t* rowChecksum)
{
    WriteTag(TAG_DELETE_ROW_MARKER);
    *rowChecksum = PlainBufferCrc8::CrcInt8(*rowChecksum, 1);
}

void PlainBufferCodedOutputStream::WriteRowChecksum(int8_t rowChecksum)
{
    WriteTag(TAG_ROW_CHECKSUM);
    mOutputStream->WriteRawByte(rowChecksum);
}

} // namespace core
} // namespace tablestore
} // namespace aliyun
