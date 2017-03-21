/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#include "plain_buffer_coded_stream.h"
#include "plain_buffer_crc8.h"
#include <iostream>

using namespace std;

namespace aliyun {
namespace tablestore {

// PlainBufferCodedInputStream

PlainBufferCodedInputStream::PlainBufferCodedInputStream(PlainBufferInputStream* inputStream)
{
    if (inputStream == NULL) {
        throw OTSClientException("Bug: PlainBufferInputStream is null.");
    }
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

PrimaryKeyValue PlainBufferCodedInputStream::ReadPrimaryKeyValue(int8_t* cellChecksum)
{
    if (!CheckLastTagWas(TAG_CELL_VALUE)) {
        throw OTSClientException(
            "Expect TAG_CELL_VALUE but it was " + OTSHelper::TagTypeToString(GetLastTag()));
    }
    mInputStream->ReadRawLittleEndian32();
    int8_t type = mInputStream->ReadRawByte();
    switch (type) {
        case VT_INTEGER: {
            int64_t int64Value = mInputStream->ReadInt64();
            *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_INTEGER);
            *cellChecksum = PlainBufferCrc8::CrcInt64(*cellChecksum, int64Value);
            ReadTag();
            return PrimaryKeyValue(int64Value);
        }
        case VT_STRING: {
            int32_t valueSize = mInputStream->ReadInt32();
            string stringValue = mInputStream->ReadUTFString(valueSize);
            *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_STRING);
            *cellChecksum = PlainBufferCrc8::CrcInt32(*cellChecksum, valueSize);
            *cellChecksum = PlainBufferCrc8::CrcString(*cellChecksum, stringValue);
            ReadTag();
            return PrimaryKeyValue(stringValue);
        }
        case VT_BLOB: {
            int32_t valueSize = mInputStream->ReadInt32();
            string binaryValue = mInputStream->ReadBytes(valueSize);
            *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_BLOB);
            *cellChecksum = PlainBufferCrc8::CrcInt32(*cellChecksum, valueSize);
            *cellChecksum = PlainBufferCrc8::CrcString(*cellChecksum, binaryValue);
            ReadTag();
            return PrimaryKeyValue(Blob(binaryValue));
        }
        default:
            throw OTSClientException(
                "Unsupported primary key type: " + OTSHelper::VariantTypeToString(type));
    }
}

ColumnValue PlainBufferCodedInputStream::ReadColumnValue(int8_t* cellChecksum)
{
    if (!CheckLastTagWas(TAG_CELL_VALUE)) {
        throw OTSClientException(
            "Expect TAG_CELL_VALUE but it was " + OTSHelper::TagTypeToString(GetLastTag()));
    }
    mInputStream->ReadRawLittleEndian32();
    int8_t type = mInputStream->ReadRawByte();
    switch (type) {
        case VT_INTEGER: {
            int64_t int64Value = mInputStream->ReadInt64();
            *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_INTEGER);
            *cellChecksum = PlainBufferCrc8::CrcInt64(*cellChecksum, int64Value);
            ReadTag();
            return ColumnValue(int64Value);
        }
        case VT_STRING: {
            int32_t valueSize = mInputStream->ReadInt32();
            string stringValue = mInputStream->ReadUTFString(valueSize);
            *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_STRING);
            *cellChecksum = PlainBufferCrc8::CrcInt32(*cellChecksum, valueSize);
            *cellChecksum = PlainBufferCrc8::CrcString(*cellChecksum, stringValue);
            ReadTag();
            return ColumnValue(stringValue);
        }
        case VT_BLOB: {
            int32_t valueSize = mInputStream->ReadInt32();
            string binaryValue = mInputStream->ReadBytes(valueSize);
            *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_BLOB);
            *cellChecksum = PlainBufferCrc8::CrcInt32(*cellChecksum, valueSize);
            *cellChecksum = PlainBufferCrc8::CrcString(*cellChecksum, binaryValue);
            ReadTag();
            return ColumnValue(Blob(binaryValue));
        }
        case VT_BOOLEAN: {
            bool boolValue = mInputStream->ReadBoolean();
            *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_BOOLEAN);
            int8_t booleanInt8 = (boolValue) ? 1 : 0;
            *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, booleanInt8);
            ReadTag();
            return ColumnValue(boolValue);
        }
        case VT_DOUBLE: {
            double doubleValue = mInputStream->ReadDouble();
            int64_t doubleInt64 = DoubleToRawBits(doubleValue);
            *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_DOUBLE);
            *cellChecksum = PlainBufferCrc8::CrcInt64(*cellChecksum, doubleInt64);
            ReadTag();
            return ColumnValue(doubleValue);
        }
        default:
            throw OTSClientException(
                "Unsupported column type: " + OTSHelper::VariantTypeToString(type));
    }
}

PrimaryKeyColumn PlainBufferCodedInputStream::ReadPrimaryKeyColumn(int8_t* rowChecksum)
{
    if (!CheckLastTagWas(TAG_CELL)) {
        throw OTSClientException(
            "Expect TAG_CELL but it was " + OTSHelper::TagTypeToString(GetLastTag()));
    }
    ReadTag();

    if (!CheckLastTagWas(TAG_CELL_NAME)) {
        throw OTSClientException(
            "Expect TAG_CELL_NAME but it was " + OTSHelper::TagTypeToString(GetLastTag()));
    }
    int8_t cellChecksum = 0;
    int32_t nameSize = mInputStream->ReadRawLittleEndian32();
    string columnName = mInputStream->ReadUTFString(nameSize);
    cellChecksum = PlainBufferCrc8::CrcString(cellChecksum, columnName);
    ReadTag();

    if (!CheckLastTagWas(TAG_CELL_VALUE)) {
        throw OTSClientException(
            "Expect TAG_CELL_VALUE E but it was " + OTSHelper::TagTypeToString(GetLastTag()));
    }
    PrimaryKeyValue primaryKeyValue = ReadPrimaryKeyValue(&cellChecksum);
    PrimaryKeyColumn primaryKeyColumn(columnName, primaryKeyValue);

    if (GetLastTag() == TAG_CELL_CHECKSUM) {
        int8_t checksum = mInputStream->ReadRawByte();
        if (checksum != cellChecksum) {
            // add log
            throw OTSClientException("Checksum mismatch.");
        }
        ReadTag();
    } else {
        throw OTSClientException(
            "Expect TAG_CELL_CHECKSUM but it was " + OTSHelper::TagTypeToString(GetLastTag()));
    }
    *rowChecksum = PlainBufferCrc8::CrcInt8(*rowChecksum, cellChecksum);

    return primaryKeyColumn;
}

Column PlainBufferCodedInputStream::ReadColumn(int8_t* rowChecksum)
{
    if (!CheckLastTagWas(TAG_CELL)) {
        throw OTSClientException(
            "Expect TAG_CELL but it was " + OTSHelper::TagTypeToString(GetLastTag()));
    }
    ReadTag();

    if (!CheckLastTagWas(TAG_CELL_NAME)) {
        throw OTSClientException(
            "Expect TAG_CELL_NAME but it was " + OTSHelper::TagTypeToString(GetLastTag()));
    }
    int8_t cellChecksum = 0;
    int32_t nameSize = mInputStream->ReadRawLittleEndian32();
    string columnName = mInputStream->ReadUTFString(nameSize);
    Column column(columnName);
    cellChecksum = PlainBufferCrc8::CrcString(cellChecksum, columnName);
    ReadTag();

    if (GetLastTag() == TAG_CELL_VALUE) {
        column.SetValue(ReadColumnValue(&cellChecksum));
    }

    // skip CELL_TYPE
    if (GetLastTag() == TAG_CELL_TYPE) {
        int8_t cellType = mInputStream->ReadRawByte();
        cellChecksum = PlainBufferCrc8::CrcInt8(cellChecksum, cellType);
        ReadTag();
    }

    if (GetLastTag() == TAG_CELL_TIMESTAMP) {
        int64_t timestamp = mInputStream->ReadInt64();
        column.SetTimestamp(timestamp);
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
            "Expect TAG_CELL_CHECKSUM but it was " + OTSHelper::TagTypeToString(GetLastTag()));
    }
    *rowChecksum = PlainBufferCrc8::CrcInt8(*rowChecksum, cellChecksum);

    return column;
}

RowPtr PlainBufferCodedInputStream::ReadRowWithoutHeader()
{
    RowPtr rowPtr(new Row());
    int8_t rowChecksum = 0;

    // parse primary key.
    if (!CheckLastTagWas(TAG_ROW_PK)) {
        throw OTSClientException(
            "Expect TAG_ROW_PK but it was " + OTSHelper::TagTypeToString(GetLastTag()));
    }
    ReadTag();
    while (CheckLastTagWas(TAG_CELL)) {
        PrimaryKeyColumn primaryKeyColumn = ReadPrimaryKeyColumn(&rowChecksum);
        rowPtr->AddPrimaryKeyColumn(primaryKeyColumn);
    }

    // parse columns.
    if (CheckLastTagWas(TAG_ROW_DATA)) {
        ReadTag();
        while (CheckLastTagWas(TAG_CELL)) {
            Column column = ReadColumn(&rowChecksum);
            rowPtr->AddColumn(column);
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
            "Expect TAG_ROW_CHECKSUM but it was " + OTSHelper::TagTypeToString(GetLastTag()));
    }

    return rowPtr;
}

RowPtr PlainBufferCodedInputStream::ReadRow()
{
    if (ReadHeader() != HEADER) {
        throw OTSClientException("Invalid header from plain buffer.");
    }
    ReadTag();
    return ReadRowWithoutHeader();
}

list<RowPtr> PlainBufferCodedInputStream::ReadRows()
{
    if (ReadHeader() != HEADER) {
        // add log
        throw OTSClientException("Invalid header from plain buffer.");
    }
    ReadTag();

    // read all rows.
    list<RowPtr> rowPtrs;
    while (!mInputStream->IsAtEnd()) {
        RowPtr rowPtr = ReadRowWithoutHeader();
        rowPtrs.push_back(rowPtr);
    }
    return rowPtrs;
}

// PlainBufferCodedOutputStream

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
    mOutputStream->WriteBytes(name);
    *cellChecksum = PlainBufferCrc8::CrcString(*cellChecksum, name);
}

void PlainBufferCodedOutputStream::WritePrimaryKeyValue(
    const PrimaryKeyValue& value,
    int8_t* cellChecksum)
{
    WriteTag(TAG_CELL_VALUE);

    if (value.IsInfMin()) {
        mOutputStream->WriteRawLittleEndian32(1);
        mOutputStream->WriteRawByte(VT_INF_MIN);
        *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_INF_MIN);
        return;
    }
    if (value.IsInfMax()) {
        mOutputStream->WriteRawLittleEndian32(1);
        mOutputStream->WriteRawByte(VT_INF_MAX);
        *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_INF_MAX);
        return;
    }
    if (value.IsPlaceholderForAutoIncrement()) {
        mOutputStream->WriteRawLittleEndian32(1);
        mOutputStream->WriteRawByte(VT_AUTO_INCREMENT);
        *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_AUTO_INCREMENT);
        return;
    }

    switch (value.GetType()) {
        case PKT_INTEGER: {
            mOutputStream->WriteRawLittleEndian32(1 + LITTLE_ENDIAN_64_SIZE);
            mOutputStream->WriteRawByte(VT_INTEGER);
            mOutputStream->WriteRawLittleEndian64(value.AsInteger());
            *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_INTEGER);
            *cellChecksum = PlainBufferCrc8::CrcInt64(*cellChecksum, value.AsInteger());
            break;
        }
        case PKT_STRING: {
            const string& stringValue = value.AsString();
            int32_t prefixLength = LITTLE_ENDIAN_32_SIZE + 1; // length + type
            mOutputStream->WriteRawLittleEndian32(prefixLength + stringValue.size());  // length + type + value
            mOutputStream->WriteRawByte(VT_STRING);
            mOutputStream->WriteRawLittleEndian32(stringValue.size());
            mOutputStream->WriteBytes(stringValue);
            *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_STRING);
            *cellChecksum = PlainBufferCrc8::CrcInt32(*cellChecksum, static_cast<int32_t>(stringValue.size()));
            *cellChecksum = PlainBufferCrc8::CrcString(*cellChecksum, stringValue);
            break;
        }
        case PKT_BINARY: {
            const Blob& rawData = value.AsBinary();
            const string& binaryValue = rawData.GetValue();
            int32_t prefixLength = LITTLE_ENDIAN_32_SIZE + 1;    // length + type
            mOutputStream->WriteRawLittleEndian32(prefixLength + binaryValue.size()); // length + type + value
            mOutputStream->WriteRawByte(VT_BLOB);
            mOutputStream->WriteRawLittleEndian32(binaryValue.size());
            mOutputStream->WriteBytes(binaryValue);
            *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_BLOB);
            *cellChecksum = PlainBufferCrc8::CrcInt32(*cellChecksum, static_cast<int32_t>(binaryValue.size()));
            *cellChecksum = PlainBufferCrc8::CrcString(*cellChecksum, binaryValue);
            break;
        }
        default:
            throw OTSClientException(
                "Unsupported primary key type: " + OTSHelper::PrimaryKeyTypeToString(value.GetType()));
    }
}

void PlainBufferCodedOutputStream::WritePrimaryKeyValue(const PrimaryKeyValue& value)
{
    if (value.IsInfMin()) {
        mOutputStream->WriteRawByte(VT_INF_MIN);
        return;
    }
    if (value.IsInfMax()) {
        mOutputStream->WriteRawByte(VT_INF_MAX);
        return;
    }

    if (value.IsPlaceholderForAutoIncrement()) {
        mOutputStream->WriteRawByte(VT_AUTO_INCREMENT);
        return;
    }

    switch (value.GetType()) {
        case PKT_INTEGER: {
            mOutputStream->WriteRawByte(VT_INTEGER);
            mOutputStream->WriteRawLittleEndian64(value.AsInteger());
            break;
        }
        case PKT_STRING: {
            const string& stringValue = value.AsString();
            mOutputStream->WriteRawByte(VT_STRING);
            mOutputStream->WriteRawLittleEndian32(stringValue.size());
            mOutputStream->WriteBytes(stringValue);
            break;
        }
        case PKT_BINARY: {
            const Blob& rawData = value.AsBinary();
            const string& binaryValue = rawData.GetValue();
            mOutputStream->WriteRawByte(VT_BLOB);
            mOutputStream->WriteRawLittleEndian32(binaryValue.size());
            mOutputStream->WriteBytes(binaryValue);
            break;
        }
        default:
            throw OTSClientException(
                "Unsupported primary key type: " + OTSHelper::PrimaryKeyTypeToString(value.GetType()));
    }
}

void PlainBufferCodedOutputStream::WriteColumnValue(
    const ColumnValue& value,
    int8_t* cellChecksum)
{
    WriteTag(TAG_CELL_VALUE);
    switch (value.GetType()) {
        case CT_INTEGER: {
            mOutputStream->WriteRawLittleEndian32(1 + LITTLE_ENDIAN_64_SIZE);
            mOutputStream->WriteRawByte(VT_INTEGER);
            mOutputStream->WriteRawLittleEndian64(value.AsInteger());
            *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_INTEGER);
            *cellChecksum = PlainBufferCrc8::CrcInt64(*cellChecksum, value.AsInteger());
            break;
        }
        case CT_STRING: {
            const string& stringValue = value.AsString();
            int32_t prefixLength = LITTLE_ENDIAN_32_SIZE + 1;   // length + type
            mOutputStream->WriteRawLittleEndian32(prefixLength + stringValue.size()); // length + type + value
            mOutputStream->WriteRawByte(VT_STRING);
            mOutputStream->WriteRawLittleEndian32(stringValue.size());
            mOutputStream->WriteBytes(stringValue);
            *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_STRING);
            *cellChecksum = PlainBufferCrc8::CrcInt32(*cellChecksum, static_cast<int32_t>(stringValue.size()));
            *cellChecksum = PlainBufferCrc8::CrcString(*cellChecksum, stringValue);
            break;
        }
        case CT_BINARY: {
            Blob rawData = value.AsBinary();
            const string& binaryValue = rawData.GetValue();
            int32_t prefixLength = LITTLE_ENDIAN_32_SIZE + 1;   // length + type
            mOutputStream->WriteRawLittleEndian32(prefixLength + binaryValue.size()); // length + type
            mOutputStream->WriteRawByte(VT_BLOB);
            mOutputStream->WriteRawLittleEndian32(binaryValue.size());
            mOutputStream->WriteBytes(binaryValue);
            *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_BLOB);
            *cellChecksum = PlainBufferCrc8::CrcInt32(*cellChecksum, static_cast<int32_t>(binaryValue.size()));
            *cellChecksum = PlainBufferCrc8::CrcString(*cellChecksum, binaryValue);
            break;
        }
        case CT_BOOLEAN: {
            mOutputStream->WriteRawLittleEndian32(2);
            mOutputStream->WriteRawByte(VT_BOOLEAN);
            mOutputStream->WriteBoolean(value.AsBoolean());
            *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_BOOLEAN);
            int8_t booleanInt8 = (value.AsBoolean()) ? 1 : 0;
            *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, booleanInt8);
            break;
        }
        case CT_DOUBLE: {
            mOutputStream->WriteRawLittleEndian32(1 + LITTLE_ENDIAN_64_SIZE);
            mOutputStream->WriteRawByte(VT_DOUBLE);
            mOutputStream->WriteDouble(value.AsDouble());
            int64_t doubleInt64 = DoubleToRawBits(value.AsDouble());
            *cellChecksum = PlainBufferCrc8::CrcInt8(*cellChecksum, VT_DOUBLE);
            *cellChecksum = PlainBufferCrc8::CrcInt64(*cellChecksum, doubleInt64);
            break;
        }
        default:
            throw OTSClientException(
                "Unsupported column type: " + OTSHelper::ColumnTypeToString(value.GetType()));
    }
}

void PlainBufferCodedOutputStream::WriteColumnValue(const ColumnValue& value)
{
    switch (value.GetType()) {
        case CT_INTEGER: {
            mOutputStream->WriteRawByte(VT_INTEGER);
            mOutputStream->WriteRawLittleEndian64(value.AsInteger());
            break;
        }
        case CT_STRING: {
            const string& stringValue = value.AsString();
            mOutputStream->WriteRawByte(VT_STRING);
            mOutputStream->WriteRawLittleEndian32(stringValue.size());
            mOutputStream->WriteBytes(stringValue);
            break;
        }
        case CT_BINARY: {
            Blob rawData = value.AsBinary();
            const string& binaryValue = rawData.GetValue();
            mOutputStream->WriteRawByte(VT_BLOB);
            mOutputStream->WriteRawLittleEndian32(binaryValue.size());
            mOutputStream->WriteBytes(binaryValue);
            break;
        }
        case CT_BOOLEAN: {
            mOutputStream->WriteRawByte(VT_BOOLEAN);
            mOutputStream->WriteBoolean(value.AsBoolean());
            break;
        }
        case CT_DOUBLE: {
            mOutputStream->WriteRawByte(VT_DOUBLE);
            mOutputStream->WriteDouble(value.AsDouble());
            break;
        }
        default:
            throw OTSClientException(
                "Unsupported column type: " + OTSHelper::ColumnTypeToString(value.GetType()));
    }
}

void PlainBufferCodedOutputStream::WritePrimaryKeyColumn(
    const PrimaryKeyColumn& primaryKeyColumn,
    int8_t* rowChecksum)
{
    int8_t cellChecksum = 0;
    WriteTag(TAG_CELL);
    WriteCellName(primaryKeyColumn.GetName(), &cellChecksum);
    WritePrimaryKeyValue(primaryKeyColumn.GetValue(), &cellChecksum);
    WriteTag(TAG_CELL_CHECKSUM);
    mOutputStream->WriteRawByte(cellChecksum);
    *rowChecksum = PlainBufferCrc8::CrcInt8(*rowChecksum, cellChecksum);
}

void PlainBufferCodedOutputStream::WriteColumn(
    const Column& column,
    int8_t* rowChecksum)
{
    int8_t cellChecksum = 0;
    WriteTag(TAG_CELL);
    WriteCellName(column.GetName(), &cellChecksum);
    WriteColumnValue(column.GetValue(), &cellChecksum);
    if (column.HasTimestamp()) {
        WriteTag(TAG_CELL_TIMESTAMP);
        mOutputStream->WriteRawLittleEndian64(column.GetTimestamp());
        cellChecksum = PlainBufferCrc8::CrcInt64(cellChecksum, column.GetTimestamp());
    }
    WriteTag(TAG_CELL_CHECKSUM);
    mOutputStream->WriteRawByte(cellChecksum);
    *rowChecksum = PlainBufferCrc8::CrcInt8(*rowChecksum, cellChecksum);
}

void PlainBufferCodedOutputStream::WriteColumn(
    const Column& column,
    RowUpdateChange::UpdateType type,
    int8_t* rowChecksum)
{
    int8_t cellChecksum = 0;
    WriteTag(TAG_CELL);
    WriteCellName(column.GetName(), &cellChecksum);
    if (column.HasValue()) {
        WriteColumnValue(column.GetValue(), &cellChecksum);
    }
    if (type == RowUpdateChange::DELETE) {
        WriteTag(TAG_CELL_TYPE);
        mOutputStream->WriteRawByte(DELETE_ONE_VERSION);
    }
    if (type == RowUpdateChange::DELETE_ALL) {
        WriteTag(TAG_CELL_TYPE);
        mOutputStream->WriteRawByte(DELETE_ALL_VERSION);
    }
    if (column.HasTimestamp()) {
        WriteTag(TAG_CELL_TIMESTAMP);
        mOutputStream->WriteRawLittleEndian64(column.GetTimestamp());
    }
    // the order of fields is different with the order of crc
    if (column.HasTimestamp()) {
        cellChecksum = PlainBufferCrc8::CrcInt64(cellChecksum, column.GetTimestamp());
    }
    if (type == RowUpdateChange::DELETE) {
        cellChecksum = PlainBufferCrc8::CrcInt8(cellChecksum, DELETE_ONE_VERSION);
    }
    if (type == RowUpdateChange::DELETE_ALL) {
        cellChecksum = PlainBufferCrc8::CrcInt8(cellChecksum, DELETE_ALL_VERSION);
    }
    WriteTag(TAG_CELL_CHECKSUM);
    mOutputStream->WriteRawByte(cellChecksum);
    *rowChecksum = PlainBufferCrc8::CrcInt8(*rowChecksum, cellChecksum);
}

void PlainBufferCodedOutputStream::WritePrimaryKey(
    const PrimaryKey& primaryKey,
    int8_t* rowChecksum)
{
    WriteTag(TAG_ROW_PK);
    const list<PrimaryKeyColumn>& pkColumns = primaryKey.GetPrimaryKeyColumns();
    for (typeof(pkColumns.begin()) iter = pkColumns.begin(); iter != pkColumns.end(); ++iter) {
        WritePrimaryKeyColumn(*iter, rowChecksum);
    }
}

void PlainBufferCodedOutputStream::WriteColumns(
    const std::list<Column>& columns,
    int8_t* rowChecksum)
{
    if (!columns.empty()) {
        WriteTag(TAG_ROW_DATA);
        for (typeof(columns.begin()) iter = columns.begin(); iter != columns.end(); ++iter) {
            WriteColumn(*iter, rowChecksum);
        }
    }
}

void PlainBufferCodedOutputStream::WriteColumns(
    const std::list<Column>& columns,
    const std::list<RowUpdateChange::UpdateType>& updateTypes,
    int8_t* rowChecksum)
{
    if (columns.size() != updateTypes.size()) {
        throw OTSClientException(
            "Bug: the size of columns is not equal to that of update types.");
    }
    if (!columns.empty()) {
        WriteTag(TAG_ROW_DATA);
        typeof(columns.begin()) columnIter = columns.begin();
        typeof(updateTypes.begin()) typeIter = updateTypes.begin();
        for (; columnIter != columns.end() && typeIter != updateTypes.end(); ++columnIter, ++typeIter) {
            WriteColumn(*columnIter, *typeIter, rowChecksum);
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

int64_t DoubleToRawBits(double value)
{
    int64_t doubleInt64 = 0;
    memcpy(&doubleInt64, &value, sizeof(doubleInt64));
    return doubleInt64;
}

} // end of tablestore
} // end of aliyun
