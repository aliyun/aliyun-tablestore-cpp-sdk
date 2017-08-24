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

namespace aliyun {
namespace tablestore {
namespace core {
namespace impl {
class AsyncClient;
class AsyncClientBase;

class SyncClient: public core::SyncClient
{
public:
    explicit SyncClient(AsyncClientBase*);
    explicit SyncClient(AsyncClient&);

    util::Optional<OTSError> listTable(ListTableResponse&, const ListTableRequest&);
    util::Optional<OTSError> createTable(
        CreateTableResponse&, const CreateTableRequest&);
    util::Optional<OTSError> deleteTable(
        DeleteTableResponse&, const DeleteTableRequest&);
    util::Optional<OTSError> describeTable(
        DescribeTableResponse&, const DescribeTableRequest&);
    util::Optional<OTSError> updateTable(UpdateTableResponse&, const UpdateTableRequest&);
    util::Optional<OTSError> computeSplitsBySize(ComputeSplitsBySizeResponse&,
        const ComputeSplitsBySizeRequest&);
    util::Optional<OTSError> putRow(PutRowResponse&, const PutRowRequest&);
    util::Optional<OTSError> getRow(GetRowResponse&, const GetRowRequest&);
    util::Optional<OTSError> getRange(GetRangeResponse&, const GetRangeRequest&);
    util::Optional<OTSError> updateRow(UpdateRowResponse&, const UpdateRowRequest&);
    util::Optional<OTSError> deleteRow(DeleteRowResponse&, const DeleteRowRequest&);
    util::Optional<OTSError> batchGetRow(
        BatchGetRowResponse&, const BatchGetRowRequest&);
    util::Optional<OTSError> batchWriteRow(
        BatchWriteRowResponse&, const BatchWriteRowRequest&);

private:
    std::tr1::shared_ptr<AsyncClientBase> mAsyncClient;

    friend class impl::AsyncClient;
};

} // namespace impl
} // namespace core
} // namespace tablestore
} // namespace aliyun

