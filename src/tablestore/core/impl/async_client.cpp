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

    explicit Context(
        AsyncClientBase& base,
        const Tracker& tracker,
        ApiRequest& req)
      : AsyncClientBase::Context<kAction>(base, tracker)
    {
        moveAssign(mApiRequest, util::move(req));
    }

    ApiRequest mApiRequest;
};

template<Action kAction>
void go(
    AsyncClientBase& base,
    typename ApiTraits<kAction>::ApiRequest& req,
    const function<void(Optional<Error>&, 
        typename ApiTraits<kAction>::ApiResponse&)>& cb)
{
    Tracker tracker(Tracker::create());
    auto_ptr<Context<kAction> > ctx(new Context<kAction>(base, tracker, req));
    Optional<Error> err = ctx->build(ctx->mApiRequest, cb);
    if (err.present()) {
        typename ApiTraits<kAction>::ApiResponse resp;
        cb(err, resp);
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
    const function<void(Optional<Error>&, CreateTableResponse&)>& cb)
{
    go<kApi_CreateTable>(*mAsyncClient, req, cb);
}

void AsyncClient::deleteTable(
    DeleteTableRequest& req,
    const function<void(Optional<Error>&, DeleteTableResponse&)>& cb)
{
    go<kApi_DeleteTable>(*mAsyncClient, req, cb);
}

void AsyncClient::listTable(
    ListTableRequest& req,
    const function<void(Optional<Error>&, ListTableResponse&)>& cb)
{
    go<kApi_ListTable>(*mAsyncClient, req, cb);
}

void AsyncClient::describeTable(
    DescribeTableRequest& req,
    const function<void(Optional<Error>&, DescribeTableResponse&)>& cb)
{
    go<kApi_DescribeTable>(*mAsyncClient, req, cb);
}

void AsyncClient::updateTable(
    UpdateTableRequest& req,
    const function<void(Optional<Error>&, UpdateTableResponse&)>& cb)
{
    go<kApi_UpdateTable>(*mAsyncClient, req, cb);
}

void AsyncClient::getRange(
    GetRangeRequest& req,
    const function<void(Optional<Error>&, GetRangeResponse&)>& cb)
{
    go<kApi_GetRange>(*mAsyncClient, req, cb);
}

void AsyncClient::putRow(
    PutRowRequest& req,
    const function<void(Optional<Error>&, PutRowResponse&)>& cb)
{
    go<kApi_PutRow>(*mAsyncClient, req, cb);
}

void AsyncClient::getRow(
    GetRowRequest& req,
    const function<void(Optional<Error>&, GetRowResponse&)>& cb)
{
    go<kApi_GetRow>(*mAsyncClient, req, cb);
}

void AsyncClient::updateRow(
    UpdateRowRequest& req,
    const function<void(Optional<Error>&, UpdateRowResponse&)>& cb)
{
    go<kApi_UpdateRow>(*mAsyncClient, req, cb);
}

void AsyncClient::deleteRow(
    DeleteRowRequest& req,
    const function<void(Optional<Error>&, DeleteRowResponse&)>& cb)
{
    go<kApi_DeleteRow>(*mAsyncClient, req, cb);
}

void AsyncClient::batchGetRow(
    BatchGetRowRequest& req,
    const function<void(Optional<Error>&, BatchGetRowResponse&)>& cb)
{
    go<kApi_BatchGetRow>(*mAsyncClient, req, cb);
}

void AsyncClient::batchWriteRow(
    BatchWriteRowRequest& req,
    const function<void(Optional<Error>&, BatchWriteRowResponse&)>& cb)
{
    go<kApi_BatchWriteRow>(*mAsyncClient, req, cb);
}

void AsyncClient::computeSplitsBySize(
    ComputeSplitsBySizeRequest& req,
    const function<void(Optional<Error>&, ComputeSplitsBySizeResponse&)>& cb)
{
    go<kApi_ComputeSplitsBySize>(*mAsyncClient, req, cb);
}

} // namespace impl
} // namespace core
} // namespace tablestore
} // namespace aliyun
