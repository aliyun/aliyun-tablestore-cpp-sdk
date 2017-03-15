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
#pragma once

#include "core/types.hpp"
#include "plain_buffer_coded_stream.hpp"
#include <string>

namespace aliyun {
namespace tablestore {
namespace core {

struct PlainBufferBuilder
{
    static int32_t ComputePrimaryKeyValueSize(const PrimaryKeyValue& value);
    static int32_t ComputeVariantValueSize(const PrimaryKeyValue& value);
    static int32_t ComputePrimaryKeyColumnSize(const PrimaryKeyColumn& pkColumn);
    static int32_t ComputeColumnValueSize(const AttributeValue& value);
    static int32_t ComputeVariantValueSize(const AttributeValue& value);
    static int32_t ComputeColumnSize(const Attribute& column);
    static int32_t ComputeColumnSize(const RowUpdateChange::Update&);
    static int32_t ComputePrimaryKeySize(const PrimaryKey& primaryKey);
    static int32_t ComputeRowSize(const RowPutChange& rowChange);
    static int32_t ComputeRowSize(const RowUpdateChange& rowChange);
    static int32_t ComputeRowSize(const RowDeleteChange& rowChange);
    static std::string SerializePrimaryKeyValue(const PrimaryKeyValue& value);
    static std::string SerializeColumnValue(const AttributeValue& value);
    static std::string SerializePrimaryKey(const PrimaryKey& primaryKey);
    static std::string SerializeForRow(const RowPutChange& rowChange); 
    static std::string SerializeForRow(const RowUpdateChange& rowChange); 
    static std::string SerializeForRow(const RowDeleteChange& rowChange); 
};

} // namespace core
} // namespace tablestore
} // namespace aliyun

