/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#ifndef PLAIN_BUFFER_CODED_STREAM_H
#define PLAIN_BUFFER_CODED_STREAM_H

#include "plain_buffer_stream.h"
#include "plain_buffer_consts.h"
#include "ots/ots_exception.h"
#include "ots/ots_request.h"
#include "ots/ots_types.h"
#include <string>
#include <list>

namespace aliyun {
namespace tablestore {

/**
 * @brief PlainBufferCodedInputStream
 *
 * 从字节流解析PlainBuffer对象。
 */
class PlainBufferCodedInputStream
{
public:

    PlainBufferCodedInputStream(PlainBufferInputStream* inputStream);

    int32_t ReadTag();

    bool CheckLastTagWas(int32_t tag);

    int32_t GetLastTag();

    int32_t ReadHeader();

    PrimaryKeyValue ReadPrimaryKeyValue(int8_t* cellChecksum);

    ColumnValue ReadColumnValue(int8_t* cellChecksum);

    PrimaryKeyColumn ReadPrimaryKeyColumn(int8_t* rowChecksum);

    Column ReadColumn(int8_t* rowChecksum);

    RowPtr ReadRowWithoutHeader();

    RowPtr ReadRow();

    std::list<RowPtr> ReadRows();

private:

    PlainBufferInputStream* mInputStream;
};

 // PlainBufferCodedOutputStream
 
class PlainBufferCodedOutputStream
{
public:

    PlainBufferCodedOutputStream(
        PlainBufferOutputStream* outputStream);

    void WriteHeader();

    void WriteTag(int8_t tag);

    void WriteCellName(
        const std::string& name,
        int8_t* cellChecksum);

    void WritePrimaryKeyValue(
        const PrimaryKeyValue& value,
        int8_t* cellChecksum);

    void WritePrimaryKeyValue(const PrimaryKeyValue& value);

    void WriteColumnValue(
        const ColumnValue& value,
        int8_t* cellChecksum);

    void WriteColumnValue(const ColumnValue& value);

    void WritePrimaryKeyColumn(
        const PrimaryKeyColumn& pkColumn,
        int8_t* rowChecksum);

    void WriteColumn(
        const Column& column,
        int8_t* rowChecksum);

    void WriteColumn(
        const Column& column,
        RowUpdateChange::UpdateType type,
        int8_t* rowChecksum);

    void WritePrimaryKey(
        const PrimaryKey& primaryKey,
        int8_t* rowChecksum);

    void WriteColumns(
        const std::list<Column>& columns,
        int8_t* rowChecksum);

    void WriteColumns(
        const std::list<Column>& columns,
        const std::list<RowUpdateChange::UpdateType>& updateTypes,
        int8_t* rowChecksum);

    void WriteDeleteMarker(int8_t* rowChecksum);

    void WriteRowChecksum(int8_t rowChecksum);

private:

    PlainBufferOutputStream* mOutputStream; 
};

int64_t DoubleToRawBits(double value);

} // end of tablestore
} // end of aliyun

#endif
