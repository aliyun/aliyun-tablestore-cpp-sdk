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
#include "async_client.hpp"
#include "sync_client.hpp"
#include "async_client_base.hpp"
#include "api_traits.hpp"

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
struct Context: public AsyncClientBase::Context<kAction>
{
    typedef typename ApiTraits<kAction>::ApiRequest ApiRequest;
    typedef typename ApiTraits<kAction>::ApiResponse ApiResponse;

    explicit Context(
        AsyncClientBase& base,
        const Tracker& tracker,
        ApiRequest& req,
        const function<void(ApiRequest&, Optional<OTSError>&, ApiResponse&)>& userCb)
      : AsyncClientBase::Context<kAction>(base, tracker),
        mUserCb(userCb)
    {
        moveAssign(mApiRequest, util::move(req));
    }

    void wrapCallback(
        Optional<OTSError>& err,
        ApiResponse& resp)
    {
        mUserCb(mApiRequest, err, resp);
    }

    ApiRequest mApiRequest;
    function<void(ApiRequest&, Optional<OTSError>&, ApiResponse&)> mUserCb;
};

template<Action kAction>
void go(
    AsyncClientBase& base,
    typename ApiTraits<kAction>::ApiRequest& req,
    const function<void(
        typename ApiTraits<kAction>::ApiRequest&,
        Optional<OTSError>&, 
        typename ApiTraits<kAction>::ApiResponse&)>& cb)
{
    typedef Context<kAction> Ctx;

    Tracker tracker(Tracker::create());
    auto_ptr<Ctx> ctx(new Ctx(base, tracker, req, cb));
    Optional<OTSError> err = ctx->build(
        ctx->mApiRequest,
        bind(&Ctx::wrapCallback, &*ctx, _1, _2));
    if (err.present()) {
        typename ApiTraits<kAction>::ApiResponse resp;
        cb(ctx->mApiRequest, err, resp);
    } else {
        ctx.release()->issue();
    }
}

} // namespace

AsyncClient::AsyncClient(impl::AsyncClientBase* ac)
  : mAsyncClient(ac)
{}

AsyncClient::AsyncClient(SyncClient& client)
  : mAsyncClient(client.mAsyncClient)
{}

void AsyncClient::createTable(
    CreateTableRequest& req,
    const function<void(
        CreateTableRequest&, Optional<OTSError>&, CreateTableResponse&)>& cb)
{
    go<kApi_CreateTable>(*mAsyncClient, req, cb);
}

void AsyncClient::deleteTable(
    DeleteTableRequest& req,
    const function<void(
        DeleteTableRequest&, Optional<OTSError>&, DeleteTableResponse&)>& cb)
{
    go<kApi_DeleteTable>(*mAsyncClient, req, cb);
}

void AsyncClient::listTable(
    ListTableRequest& req,
    const function<void(
        ListTableRequest&, Optional<OTSError>&, ListTableResponse&)>& cb)
{
    go<kApi_ListTable>(*mAsyncClient, req, cb);
}

void AsyncClient::describeTable(
    DescribeTableRequest& req,
    const function<void(
        DescribeTableRequest&, Optional<OTSError>&, DescribeTableResponse&)>& cb)
{
    go<kApi_DescribeTable>(*mAsyncClient, req, cb);
}

void AsyncClient::updateTable(
    UpdateTableRequest& req,
    const function<void(
        UpdateTableRequest&, Optional<OTSError>&, UpdateTableResponse&)>& cb)
{
    go<kApi_UpdateTable>(*mAsyncClient, req, cb);
}

void AsyncClient::getRange(
    GetRangeRequest& req,
    const function<void(
        GetRangeRequest&, Optional<OTSError>&, GetRangeResponse&)>& cb)
{
    go<kApi_GetRange>(*mAsyncClient, req, cb);
}

void AsyncClient::putRow(
    PutRowRequest& req,
    const function<void(
        PutRowRequest&, Optional<OTSError>&, PutRowResponse&)>& cb)
{
    go<kApi_PutRow>(*mAsyncClient, req, cb);
}

void AsyncClient::getRow(
    GetRowRequest& req,
    const function<void(
        GetRowRequest&, Optional<OTSError>&, GetRowResponse&)>& cb)
{
    go<kApi_GetRow>(*mAsyncClient, req, cb);
}

void AsyncClient::updateRow(
    UpdateRowRequest& req,
    const function<void(
        UpdateRowRequest&, Optional<OTSError>&, UpdateRowResponse&)>& cb)
{
    go<kApi_UpdateRow>(*mAsyncClient, req, cb);
}

void AsyncClient::deleteRow(
    DeleteRowRequest& req,
    const function<void(
        DeleteRowRequest&, Optional<OTSError>&, DeleteRowResponse&)>& cb)
{
    go<kApi_DeleteRow>(*mAsyncClient, req, cb);
}

void AsyncClient::batchGetRow(
    BatchGetRowRequest& req,
    const function<void(
        BatchGetRowRequest&, Optional<OTSError>&, BatchGetRowResponse&)>& cb)
{
    go<kApi_BatchGetRow>(*mAsyncClient, req, cb);
}

void AsyncClient::batchWriteRow(
    BatchWriteRowRequest& req,
    const function<void(
        BatchWriteRowRequest&, Optional<OTSError>&, BatchWriteRowResponse&)>& cb)
{
    go<kApi_BatchWriteRow>(*mAsyncClient, req, cb);
}

void AsyncClient::computeSplitsBySize(
    ComputeSplitsBySizeRequest& req,
    const function<void(
        ComputeSplitsBySizeRequest&, Optional<OTSError>&, ComputeSplitsBySizeResponse&)>& cb)
{
    go<kApi_ComputeSplitsBySize>(*mAsyncClient, req, cb);
}

} // namespace impl
} // namespace core
} // namespace tablestore
} // namespace aliyun
