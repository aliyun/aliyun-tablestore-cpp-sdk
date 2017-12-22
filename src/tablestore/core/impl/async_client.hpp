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
#include "tablestore/core/client.hpp"
#include <tr1/memory>
#include <tr1/functional>

namespace aliyun {
namespace tablestore {
namespace core {
namespace impl {
class SyncClient;
class AsyncClientBase;

class AsyncClient: public core::AsyncClient
{
public:
    explicit AsyncClient(AsyncClientBase*);
    explicit AsyncClient(SyncClient&);

    void createTable(
        CreateTableRequest&,
        const std::tr1::function<void(
            CreateTableRequest&, util::Optional<OTSError>&, CreateTableResponse&)>&);
    void deleteTable(
        DeleteTableRequest&,
        const std::tr1::function<void(
            DeleteTableRequest&, util::Optional<OTSError>&, DeleteTableResponse&)>&);
    void listTable(
        ListTableRequest&,
        const std::tr1::function<void(
            ListTableRequest&, util::Optional<OTSError>&, ListTableResponse&)>&);
    void describeTable(
        DescribeTableRequest&,
        const std::tr1::function<void(
            DescribeTableRequest&, util::Optional<OTSError>&, DescribeTableResponse&)>&);
    void updateTable(
        UpdateTableRequest&,
        const std::tr1::function<void(
            UpdateTableRequest&, util::Optional<OTSError>&, UpdateTableResponse&)>&);
    void getRange(
        GetRangeRequest&,
        const std::tr1::function<void(
            GetRangeRequest&, util::Optional<OTSError>&, GetRangeResponse&)>&);
    void putRow(
        PutRowRequest&,
        const std::tr1::function<void(
            PutRowRequest&, util::Optional<OTSError>&, PutRowResponse&)>&);
    void getRow(
        GetRowRequest&,
        const std::tr1::function<void(
            GetRowRequest&, util::Optional<OTSError>&, GetRowResponse&)>&);
    void updateRow(
        UpdateRowRequest&,
        const std::tr1::function<void(
            UpdateRowRequest&, util::Optional<OTSError>&, UpdateRowResponse&)>&);
    void deleteRow(
        DeleteRowRequest&,
        const std::tr1::function<void(
            DeleteRowRequest&, util::Optional<OTSError>&, DeleteRowResponse&)>&);
    void batchGetRow(
        BatchGetRowRequest&,
        const std::tr1::function<void(
            BatchGetRowRequest&, util::Optional<OTSError>&, BatchGetRowResponse&)>&);
    void batchWriteRow(
        BatchWriteRowRequest&,
        const std::tr1::function<void(
            BatchWriteRowRequest&, util::Optional<OTSError>&, BatchWriteRowResponse&)>&);
    void computeSplitsBySize(
        ComputeSplitsBySizeRequest&,
        const std::tr1::function<void(
            ComputeSplitsBySizeRequest&, util::Optional<OTSError>&, ComputeSplitsBySizeResponse&)>&);

private:
    std::tr1::shared_ptr<AsyncClientBase> mAsyncClient;

    friend class impl::SyncClient;
};


} // namespace impl
} // namespace core
} // namespace tablestore
} // namespace aliyun


