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
#include "sync_client.hpp"
#include "async_client.hpp"
#include "async_client_base.hpp"
#include "api_traits.hpp"
#include "tablestore/util/try.hpp"
#include "tablestore/util/threading.hpp"
#include <boost/ref.hpp>
#include <tr1/functional>

using namespace std;
using namespace std::tr1;
using namespace std::tr1::placeholders;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {
namespace impl {

namespace {

template<Action kAction>
void callback(
    Semaphore& sem,
    Optional<OTSError>& outErr,
    typename impl::ApiTraits<kAction>::ApiResponse& outResp,
    Optional<OTSError>& inErr,
    typename impl::ApiTraits<kAction>::ApiResponse& inResp)
{
    if (inErr.present()) {
        moveAssign(outErr, util::move(inErr));
    } else {
        moveAssign(outResp, util::move(inResp));
    }
    sem.post();
}


template<Action kAction>
Optional<OTSError> go(
    typename impl::ApiTraits<kAction>::ApiResponse& resp,
    const typename impl::ApiTraits<kAction>::ApiRequest& req,
    impl::AsyncClientBase& ac)
{
    typedef impl::AsyncClientBase::Context<kAction> Context;
    typedef typename impl::ApiTraits<kAction>::ApiResponse Response;

    Tracker tracker(Tracker::create());
    Semaphore sem(0);
    Optional<OTSError> err;
    function<void(Optional<OTSError>&, Response&)> cb =
        bind(&callback<kAction>,
            boost::ref(sem),
            boost::ref(err),
            boost::ref(resp),
            _1, _2);
    auto_ptr<Context> ctx(new Context(ac, tracker));
    TRY(ctx->build(req, cb));
    ctx.release()->issue();
    sem.wait();
    return err;
}

} // namespace

SyncClient::SyncClient(impl::AsyncClientBase* ac)
  : mAsyncClient(ac)
{}

SyncClient::SyncClient(AsyncClient& client)
  : mAsyncClient(client.mAsyncClient)
{}

Optional<OTSError> SyncClient::listTable(
    ListTableResponse& resp, const ListTableRequest& req)
{
    return go<kApi_ListTable>(resp, req, *mAsyncClient);
}

Optional<OTSError> SyncClient::createTable(
    CreateTableResponse& resp, const CreateTableRequest& req)
{
    return go<kApi_CreateTable>(resp, req, *mAsyncClient);
}

Optional<OTSError> SyncClient::deleteTable(
    DeleteTableResponse& resp, const DeleteTableRequest& req)
{
    return go<kApi_DeleteTable>(resp, req, *mAsyncClient);
}

Optional<OTSError> SyncClient::describeTable(
    DescribeTableResponse& resp, const DescribeTableRequest& req)
{
    return go<kApi_DescribeTable>(resp, req, *mAsyncClient);
}

Optional<OTSError> SyncClient::updateTable(
    UpdateTableResponse& resp, const UpdateTableRequest& req)
{
    return go<kApi_UpdateTable>(resp, req, *mAsyncClient);
}

Optional<OTSError> SyncClient::computeSplitsBySize(
    ComputeSplitsBySizeResponse& resp, const ComputeSplitsBySizeRequest& req)
{
    return go<kApi_ComputeSplitsBySize>(resp, req, *mAsyncClient);
}

Optional<OTSError> SyncClient::putRow(PutRowResponse& resp, const PutRowRequest& req)
{
    return go<kApi_PutRow>(resp, req, *mAsyncClient);
}

Optional<OTSError> SyncClient::getRow(GetRowResponse& resp, const GetRowRequest& req)
{
    return go<kApi_GetRow>(resp, req, *mAsyncClient);
}

Optional<OTSError> SyncClient::getRange(GetRangeResponse& resp, const GetRangeRequest& req)
{
    return go<kApi_GetRange>(resp, req, *mAsyncClient);
}

Optional<OTSError> SyncClient::updateRow(
    UpdateRowResponse& resp, const UpdateRowRequest& req)
{
    return go<kApi_UpdateRow>(resp, req, *mAsyncClient);
}

Optional<OTSError> SyncClient::deleteRow(
    DeleteRowResponse& resp, const DeleteRowRequest& req)
{
    return go<kApi_DeleteRow>(resp, req, *mAsyncClient);
}

Optional<OTSError> SyncClient::batchGetRow(
    BatchGetRowResponse& resp, const BatchGetRowRequest& req)
{
    return go<kApi_BatchGetRow>(resp, req, *mAsyncClient);
}

Optional<OTSError> SyncClient::batchWriteRow(
    BatchWriteRowResponse& resp, const BatchWriteRowRequest& req)
{
    return go<kApi_BatchWriteRow>(resp, req, *mAsyncClient);
}

} // namespace impl
} // namespace core
} // namespace tablestore
} // namespace aliyun
