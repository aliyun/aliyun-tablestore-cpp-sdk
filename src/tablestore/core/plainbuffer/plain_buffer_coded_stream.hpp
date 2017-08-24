#pragma once
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

#include "plain_buffer_stream.hpp"
#include "plain_buffer_consts.hpp"
#include "../impl/ots_exception.hpp"
#include "tablestore/core/types.hpp"
#include <string>
#include <list>

namespace aliyun {
namespace tablestore {
namespace core {

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

    void ReadPrimaryKeyValue(PrimaryKeyValue*, int8_t* cellChecksum);
    void ReadColumnValue(AttributeValue*, int8_t* cellChecksum);
    void ReadPrimaryKeyColumn(PrimaryKeyColumn*, int8_t* rowChecksum);
    void ReadColumn(Attribute*, int8_t* rowChecksum);
    void ReadRowWithoutHeader(Row*);
    void ReadRow(Row*);
    void ReadRows(IVector<Row>*);

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
        const AttributeValue& value,
        int8_t* cellChecksum);

    void WriteColumnValue(const AttributeValue& value);

    void WritePrimaryKeyColumn(
        const PrimaryKeyColumn& pkColumn,
        int8_t* rowChecksum);

    void WriteColumn(
        const Attribute& column,
        int8_t* rowChecksum);

    void WriteColumn(
        const RowUpdateChange::Update& update,
        int8_t* rowChecksum);

    void WritePrimaryKey(
        const PrimaryKey& primaryKey,
        int8_t* rowChecksum);

    void WriteColumns(
        const IVector<Attribute>& columns,
        int8_t* rowChecksum);

    void WriteColumns(
        const IVector<RowUpdateChange::Update>& attrs,
        int8_t* rowChecksum);

    void WriteDeleteMarker(int8_t* rowChecksum);

    void WriteRowChecksum(int8_t rowChecksum);

private:
    PlainBufferOutputStream* mOutputStream; 
};

} // namespace core
} // namespace tablestore
} // namespace aliyun

