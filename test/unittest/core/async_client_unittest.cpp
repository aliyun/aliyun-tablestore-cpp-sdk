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
#include "src/tablestore/core/impl/async_client_base.hpp"
#include "src/tablestore/core/impl/ots_constants.hpp"
#include "src/tablestore/core/http/asio.hpp"
#include "src/tablestore/core/http/client.hpp"
#include "src/tablestore/core/protocol/table_store.pb.h"
#include "common.hpp"
#include "tablestore/core/error.hpp"
#include "tablestore/core/retry.hpp"
#include "tablestore/util/security.hpp"
#include "tablestore/util/optional.hpp"
#include "tablestore/util/threading.hpp"
#include "tablestore/util/assert.hpp"
#include "tablestore/util/logging.hpp"
#include "tablestore/util/foreach.hpp"
#include "testa/testa.hpp"
#include <boost/ref.hpp>
#include <tr1/functional>
#include <tr1/memory>
#include <string>

using namespace std;
using namespace std::tr1;
using namespace std::tr1::placeholders;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {

namespace {

class TimerMock;
boost::atomic<TimerMock*> sTimer(NULL);

class TimerMock: public http::Timer
{
public:
    static const string kStart;
    static const string kCancel;

    explicit TimerMock(
        Logger& logger,
        Actor& actor,
        Slave& slave,
        const function<void()>& cb)
      : mLogger(&logger),
        mActor(actor),
        mSlave(slave),
        mClose(false),
        mCallback(cb)
    {
        sTimer.store(this, boost::memory_order_release);
        mSlave.ask(kStart);
    }

    ~TimerMock()
    {
        OTS_ASSERT(mClose);
        sTimer.store(NULL, boost::memory_order_release);
    }

    void cancel()
    {
        if (!mClose) {
            mSlave.ask(kCancel);
        }
        mClose = true;
    }

    void callback()
    {
        if (!mClose) {
            function<void()> cb = mCallback;
            mCallback = function<void()>();
            mActor.pushBack(cb);
        }
        mClose = true;
    }

private:
    Logger* mLogger;
    Actor& mActor;
    Slave& mSlave;
    bool mClose;
    function<void()> mCallback;
};

const string TimerMock::kStart("TimerMock::Start");
const string TimerMock::kCancel("TimerMock::Cancel");

class AsioMock: public http::Asio
{
public:
    explicit AsioMock(Logger& logger, Slave& slave)
      : mLogger(logger),
        mSlave(slave)
    {}
    ~AsioMock()
    {}

    void asyncBorrowConnection(
        const Tracker& tracker,
        util::MonotonicTime deadline,
        const BorrowConnectionHandler& cb)
    {
        OTS_ASSERT(false)
            (tracker);
    }

    http::Timer& startTimer(
        const Tracker& tracker,
        util::MonotonicTime deadline,
        util::Actor& actor,
        const http::TimeoutCallback& cb)
    {
        OTS_LOG_DEBUG(mLogger)
            ("Tracker", tracker)
            .what("UT: start a timer");
        http::Timer* res = new TimerMock(mLogger, actor, mSlave, cb);
        return *res;
    }

    void close()
    {
        OTS_LOG_DEBUG(mLogger)
            .what("UT: close Asio.");
    }

private:
    Logger& mLogger;
    Slave& mSlave;
};

struct HttpIssueContext
{
    MonotonicTime mDeadline;
    string mPath;
    http::InplaceHeaders mAddHeaders;
    deque<MemPiece> mBody;
    http::ResponseCallback mCallback;
    http::Headers mFixedHeaders;
};

class HttpClientMock: public http::Client
{
public:
    static const string kIssue;
    static const string kClose;

    explicit HttpClientMock(
        Logger& logger,
        Slave& slave,
        HttpIssueContext& issueCtx)
      : mLogger(logger),
        mSlave(slave),
        mIssueContext(issueCtx)
    {}

    ~HttpClientMock()
    {}

    static HttpClientMock* create(
        Logger& logger,
        Slave& slave,
        HttpIssueContext& issueCtx,
        const http::Headers& headers)
    {
        HttpClientMock* res = new HttpClientMock(logger, slave, issueCtx);
        issueCtx.mFixedHeaders = headers;
        return res;
    }

    virtual void issue(
        const Tracker& tracker,
        const http::ResponseCallback& respCb,
        MonotonicTime deadline,
        const std::string& path,
        http::InplaceHeaders& additionalHeaders,
        deque<MemPiece>& body)
    {
        OTS_LOG_DEBUG(mLogger)
            ("Tracker", tracker)
            .what("HttpMock: issue an request");
        mIssueContext.mDeadline = deadline;
        mIssueContext.mPath = path;
        moveAssign(mIssueContext.mAddHeaders, util::move(additionalHeaders));
        moveAssign(mIssueContext.mBody, util::move(body));
        mIssueContext.mCallback = respCb;
        mSlave.ask(kIssue);
    }

    void close()
    {
        OTS_LOG_DEBUG(mLogger)
            .what("HttpMock: close");
        mSlave.ask(kClose);
    }

private:
    Logger& mLogger;
    Slave& mSlave;
    HttpIssueContext& mIssueContext;
};

const string HttpClientMock::kIssue("HttpClientMock::issue");
const string HttpClientMock::kClose("HttpClientMock::close");

const string kResult = "result";

class ResponseCallback
{
public:
    explicit ResponseCallback(
        http::ResponseCallback& cb,
        const Tracker& tracker,
        OTSError& err,
        http::Headers& headers,
        const string& body)
      : mCb(cb),
        mTracker(tracker),
        mRawBody(body)
    {
        if (err.httpStatus() < 200 || err.httpStatus() >= 300) {
            mError.reset(util::move(err));
        }
        moveAssign(mHeaders, util::move(headers));
        cb = http::ResponseCallback();
    }

    explicit ResponseCallback(
        http::ResponseCallback& cb,
        const Tracker& tracker,
        OTSError& err,
        http::Headers& headers,
        com::aliyun::tablestore::protocol::Error& pbErr)
      : mCb(cb),
        mTracker(tracker),
        mPbError(pbErr)
    {
        mError.reset(util::move(err));
        moveAssign(mHeaders, util::move(headers));
        cb = http::ResponseCallback();
    }

    explicit ResponseCallback(
        http::ResponseCallback& cb,
        const Tracker& tracker,
        http::Headers& headers,
        com::aliyun::tablestore::protocol::ListTableResponse& body)
      : mCb(cb),
        mTracker(tracker),
        mResponse(body)
    {
        moveAssign(mHeaders, util::move(headers));
        cb = http::ResponseCallback();
    }

    void operator()()
    {
        http::InplaceHeaders inplaceHeaders;
        FOREACH_ITER(i, mHeaders) {
            inplaceHeaders.insert(
                make_pair(MemPiece::from(i->first), MemPiece::from(i->second)));
        }

        string out;
        if (mRawBody.present()) {
            out = *mRawBody;
        } else if (mError.present()) {
            mPbError.SerializeToString(&out);
        } else {
            mResponse.SerializeToString(&out);
        }
        deque<MemPiece> body;
        body.push_back(MemPiece::from(out));
        mCb(mTracker, mError, inplaceHeaders, body);
    }

private:
    http::ResponseCallback mCb;
    Tracker mTracker;
    Optional<OTSError> mError;
    com::aliyun::tablestore::protocol::Error mPbError;
    http::Headers mHeaders;
    com::aliyun::tablestore::protocol::ListTableResponse mResponse;
    Optional<string> mRawBody;
};

class ResultCallback
{
public:
    explicit ResultCallback(
        Logger& logger,
        Slave& slave,
        Optional<OTSError>& err,
        ListTableResponse& resp)
      : mLogger(logger),
        mSlave(slave),
        mError(err),
        mResponse(resp)
    {}

    void operator()(Optional<OTSError>& err, ListTableResponse& resp)
    {
        if (err.present()) {
            OTS_LOG_DEBUG(mLogger)
                ("Error", *err);
            moveAssign(mError, util::move(err));
        } else {
            OTS_LOG_DEBUG(mLogger)
                ("Response", resp);
            moveAssign(mResponse, util::move(resp));
        }
        mSlave.ask(kResult);
    }

private:
    Logger& mLogger;
    Slave& mSlave;
    Optional<OTSError>& mError;
    ListTableResponse& mResponse;
};

const string kDeleteClient("DeleteClient");

class TestBench
{
public:
    explicit TestBench(RetryStrategy* rs = NULL);

    void resetClient();

    Logger& logger()
    {
        return *mLogger;
    }

    Master& master()
    {
        return mMaster;
    }

    Slave& slave()
    {
        return mSlave;
    }

    impl::AsyncClientBase& client()
    {
        return *mClient;
    }

    const Tracker& tracker() const
    {
        return mTracker;
    }

    const string& requestId() const
    {
        return mRequestId;
    }

    HttpIssueContext& issueContext()
    {
        return mIssueContext;
    }

    void asyncRun(const function<void()>& fn)
    {
        mActor->pushBack(fn);
    }

    void deleteClient()
    {
        mActor->pushBack(bind(&TestBench::_deleteClient, this));
        {
            string token = master().listen();
            TESTA_ASSERT(token == HttpClientMock::kClose)
                (token).issue();
            master().answer(token);
        }
        {
            string token = master().listen();
            TESTA_ASSERT(token == kDeleteClient)
                (token).issue();
        }
    }

    Credential& mutableCredential()
    {
        return mCredential;
    }

private:
    void _deleteClient()
    {
        OTS_LOG_DEBUG(*mLogger)
            .what("UT: delete client");
        mClient.reset();
        slave().say(kDeleteClient);
    }

public:
    ClientOptions mOpts;
    Logger* mLogger;
    Actor* mActor;
    auto_ptr<Random> mRng;

    MasterSlave mMasterSlave;
    Master& mMaster;
    Slave& mSlave;

    Credential mCredential;
    Tracker mTracker;
    string mRequestId;

    auto_ptr<impl::AsyncClientBase> mClient;

    HttpIssueContext mIssueContext;
};

TestBench::TestBench(RetryStrategy* rs)
  : mLogger(NULL),
    mActor(NULL),
    mRng(random::newDefault()),
    mMasterSlave(),
    mMaster(mMasterSlave.master()),
    mSlave(mMasterSlave.slave()),
    mTracker(Tracker::create(*mRng)),
    mRequestId(Tracker::create(*mRng).traceId())
{
    if (rs != NULL) {
        mOpts.resetRetryStrategy(rs);
    }
    mOpts.resetLogger(createLogger("/", Logger::kDebug));
    mLogger = &mOpts.mutableLogger();
    mOpts.mutableActors().clear();
    mOpts.mutableActors().push_back(shared_ptr<Actor>(new Actor()));
    mActor = mOpts.actors().front().get();

    mCredential.mutableAccessKeyId() = "akId";
    mCredential.mutableAccessKeySecret() = "akSecret";
}

void TestBench::resetClient()
{
    mClient.reset(
        new impl::AsyncClientBase(
            new AsioMock(*mLogger, mSlave),
            bind(HttpClientMock::create,
                boost::ref(*mLogger),
                boost::ref(mSlave),
                boost::ref(mIssueContext),
                _1),
            mCredential,
            mOpts));
}

void listTable(impl::AsyncClientBase::Context<kApi_ListTable>* ctx)
{
    ctx->issue();
}

string concat(const deque<MemPiece>& pieces)
{
    string res;
    FOREACH_ITER(i, pieces) {
        res.append((char*) i->data(), i->length());
    }
    return res;
}

} // namespace

namespace {
void AsyncClient_Oneshot(const string&)
{
    TestBench tb(new NoRetry());
    tb.resetClient();
    Optional<OTSError> resultErr;
    ListTableRequest req;
    ListTableResponse resp;
    ResultCallback resultCb(tb.logger(), tb.slave(), resultErr, resp);
    impl::AsyncClientBase& client = tb.client();
    impl::AsyncClientBase::Context<kApi_ListTable>* ctx =
        new impl::AsyncClientBase::Context<kApi_ListTable>(client, tb.mTracker);
    ctx->build(req, resultCb);
    tb.asyncRun(bind(listTable, ctx));
    {
        string token = tb.master().listen();
        TESTA_ASSERT(token == HttpClientMock::kIssue)
            (token).issue();
        TESTA_ASSERT(tb.issueContext().mPath == "/ListTable")
            (tb.issueContext().mPath).issue();
        string body = concat(tb.issueContext().mBody);
        TESTA_ASSERT(pp::prettyPrint(MemPiece::from(body)) == "b\"\"")
            (MemPiece::from(body)).issue();
        http::Headers headers;
        headers[impl::kOTSRequestId] = tb.requestId();
        com::aliyun::tablestore::protocol::ListTableResponse resp;
        resp.add_table_names("pet");
        ResponseCallback respCb(
            tb.issueContext().mCallback,
            tb.tracker(),
            headers,
            resp);
        tb.asyncRun(respCb);
        tb.master().answer(token);
    }
    {
        string token = tb.master().listen();
        TESTA_ASSERT(token == kResult)
            (token).issue();
        TESTA_ASSERT(!resultErr.present())
            (*resultErr).issue();
        TESTA_ASSERT(pp::prettyPrint(resp.tables()) == "[\"pet\"]")
            (resp).issue();
        TESTA_ASSERT(resp.traceId() == tb.tracker().traceId())
            (resp).issue();
        TESTA_ASSERT(resp.requestId() == tb.requestId())
            (resp).issue();
        tb.master().answer(token);
    }
    tb.deleteClient();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(AsyncClient_Oneshot);

namespace {
void AsyncClient_Oneshot_BackendError(const string&)
{
    TestBench tb;
    tb.resetClient();
    Optional<OTSError> resultErr;
    ListTableRequest req;
    ListTableResponse resp;
    ResultCallback resultCb(tb.logger(), tb.slave(), resultErr, resp);
    impl::AsyncClientBase& client = tb.client();
    impl::AsyncClientBase::Context<kApi_ListTable>* ctx =
        new impl::AsyncClientBase::Context<kApi_ListTable>(client, tb.mTracker);
    ctx->build(req, resultCb);
    tb.asyncRun(bind(listTable, ctx));
    {
        string token = tb.master().listen();
        TESTA_ASSERT(token == HttpClientMock::kIssue)
            (token).issue();
        TESTA_ASSERT(tb.issueContext().mPath == "/ListTable")
            (tb.issueContext().mPath).issue();
        string body = concat(tb.issueContext().mBody);
        TESTA_ASSERT(pp::prettyPrint(MemPiece::from(body)) == "b\"\"")
            (MemPiece::from(body)).issue();
        OTSError err;
        err.mutableHttpStatus() = 403;
        http::Headers headers;
        headers[impl::kOTSRequestId] = tb.requestId();
        com::aliyun::tablestore::protocol::Error resp;
        *resp.mutable_code() = OTSError::kErrorCode_OTSAuthFailed;
        *resp.mutable_message() = "test";
        ResponseCallback respCb(
            tb.issueContext().mCallback,
            tb.tracker(),
            err,
            headers,
            resp);
        tb.asyncRun(respCb);
        tb.master().answer(token);
    }
    {
        string token = tb.master().listen();
        TESTA_ASSERT(token == kResult)
            (token).issue();
        TESTA_ASSERT(resultErr.present()).issue();
        TESTA_ASSERT(resultErr->traceId() == tb.tracker().traceId())
            (*resultErr).issue();
        TESTA_ASSERT(resultErr->requestId() == tb.requestId())
            (*resultErr).issue();
        TESTA_ASSERT(resultErr->httpStatus() == 403)
            (*resultErr).issue();
        TESTA_ASSERT(resultErr->errorCode() == OTSError::kErrorCode_OTSAuthFailed)
            (*resultErr).issue();
        TESTA_ASSERT(resultErr->message() == "test")
            (*resultErr).issue();
        tb.master().answer(token);
    }
    tb.deleteClient();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(AsyncClient_Oneshot_BackendError);

namespace {
void AsyncClient_Oneshot_NetworkError(const string&)
{
    TestBench tb(new NoRetry());
    tb.resetClient();
    Optional<OTSError> resultErr;
    ListTableRequest req;
    ListTableResponse resp;
    ResultCallback resultCb(tb.logger(), tb.slave(), resultErr, resp);
    impl::AsyncClientBase& client = tb.client();
    impl::AsyncClientBase::Context<kApi_ListTable>* ctx =
        new impl::AsyncClientBase::Context<kApi_ListTable>(client, tb.mTracker);
    ctx->build(req, resultCb);
    tb.asyncRun(bind(listTable, ctx));
    {
        string token = tb.master().listen();
        TESTA_ASSERT(token == HttpClientMock::kIssue)
            (token).issue();
        TESTA_ASSERT(tb.issueContext().mPath == "/ListTable")
            (tb.issueContext().mPath).issue();
        string body = concat(tb.issueContext().mBody);
        TESTA_ASSERT(pp::prettyPrint(MemPiece::from(body)) == "b\"\"")
            (MemPiece::from(body)).issue();
        OTSError err;
        err.mutableHttpStatus() = OTSError::kHttpStatus_WriteRequestFail;
        http::Headers headers;
        ResponseCallback respCb(
            tb.issueContext().mCallback,
            tb.tracker(),
            err,
            headers,
            string("whatever"));
        tb.asyncRun(respCb);
        tb.master().answer(token);
    }
    {
        string token = tb.master().listen();
        TESTA_ASSERT(token == kResult)
            (token).issue();
        TESTA_ASSERT(resultErr.present()).issue();
        TESTA_ASSERT(resultErr->traceId() == tb.tracker().traceId())
            (*resultErr).issue();
        TESTA_ASSERT(resultErr->requestId() == "")
            (*resultErr).issue();
        TESTA_ASSERT(resultErr->httpStatus() == OTSError::kHttpStatus_WriteRequestFail)
            (*resultErr).issue();
        TESTA_ASSERT(resultErr->errorCode() == "")
            (*resultErr).issue();
        TESTA_ASSERT(resultErr->message() == "")
            (*resultErr).issue();
        tb.master().answer(token);
    }
    tb.deleteClient();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(AsyncClient_Oneshot_NetworkError);

namespace {
void AsyncClient_Retry(const string&)
{
    TestBench tb;
    tb.resetClient();
    Optional<OTSError> resultErr;
    ListTableRequest req;
    ListTableResponse resp;
    ResultCallback resultCb(tb.logger(), tb.slave(), resultErr, resp);
    impl::AsyncClientBase& client = tb.client();
    impl::AsyncClientBase::Context<kApi_ListTable>* ctx =
        new impl::AsyncClientBase::Context<kApi_ListTable>(client, tb.mTracker);
    ctx->build(req, resultCb);
    tb.asyncRun(bind(listTable, ctx));
    {
        string token = tb.master().listen();
        TESTA_ASSERT(token == HttpClientMock::kIssue)
            (token).issue();
        TESTA_ASSERT(tb.issueContext().mPath == "/ListTable")
            (tb.issueContext().mPath).issue();
        OTSError err;
        err.mutableHttpStatus() = 500;
        http::Headers headers;
        headers[impl::kOTSRequestId] = tb.requestId();
        com::aliyun::tablestore::protocol::Error resp;
        *resp.mutable_code() = OTSError::kErrorCode_OTSTableNotReady;
        *resp.mutable_message() = "test";
        ResponseCallback respCb(
            tb.issueContext().mCallback,
            tb.tracker(),
            err,
            headers,
            resp);
        tb.asyncRun(respCb);
        tb.master().answer(token);
    }
    {
        string token = tb.master().listen();
        TESTA_ASSERT(token == TimerMock::kStart)
            (token).issue();
        sTimer.load(boost::memory_order_acquire)->callback();
        tb.master().answer(token);
    }
    {
        string token = tb.master().listen();
        TESTA_ASSERT(token == HttpClientMock::kIssue)
            (token).issue();
        TESTA_ASSERT(tb.issueContext().mPath == "/ListTable")
            (tb.issueContext().mPath).issue();
        http::Headers headers;
        headers[impl::kOTSRequestId] = tb.requestId();
        com::aliyun::tablestore::protocol::ListTableResponse resp;
        resp.add_table_names("pet");
        ResponseCallback respCb(
            tb.issueContext().mCallback,
            tb.tracker(),
            headers,
            resp);
        tb.asyncRun(respCb);
        tb.master().answer(token);
    }
    {
        string token = tb.master().listen();
        TESTA_ASSERT(token == kResult)
            (token).issue();
        TESTA_ASSERT(!resultErr.present())
            (*resultErr).issue();
        TESTA_ASSERT(pp::prettyPrint(resp.tables()) == "[\"pet\"]")
            (resp).issue();
        TESTA_ASSERT(resp.traceId() == tb.tracker().traceId())
            (resp).issue();
        TESTA_ASSERT(resp.requestId() == tb.requestId())
            (resp).issue();
        tb.master().answer(token);
    }
    tb.deleteClient();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(AsyncClient_Retry);

namespace {

string md5(const string& in)
{
    Md5 md5;
    md5.update(MemPiece::from(in));
    uint8_t xs[Md5::kLength];
    md5.finalize(MutableMemPiece(xs, Md5::kLength));
    Base64Encoder b64;
    b64.update(MemPiece::from(xs));
    b64.finalize();
    return b64.base64().to<string>();
}

void AsyncClient_ResponseValidation(const string&)
{
    TestBench tb(new NoRetry());
    tb.resetClient();
    Optional<OTSError> resultErr;
    ListTableRequest req;
    ListTableResponse resp;
    ResultCallback resultCb(tb.logger(), tb.slave(), resultErr, resp);
    impl::AsyncClientBase& client = tb.client();
    impl::AsyncClientBase::Context<kApi_ListTable>* ctx =
        new impl::AsyncClientBase::Context<kApi_ListTable>(client, tb.mTracker);
    ctx->build(req, resultCb);
    tb.asyncRun(bind(listTable, ctx));
    {
        string token = tb.master().listen();
        TESTA_ASSERT(token == HttpClientMock::kIssue)
            (token).issue();
        TESTA_ASSERT(tb.issueContext().mPath == "/ListTable")
            (tb.issueContext().mPath).issue();
        string body = concat(tb.issueContext().mBody);
        TESTA_ASSERT(pp::prettyPrint(MemPiece::from(body)) == "b\"\"")
            (MemPiece::from(body)).issue();
        OTSError err;
        http::Headers headers;
        headers[impl::kOTSContentMD5] = md5(string());
        ResponseCallback respCb(
            tb.issueContext().mCallback,
            tb.tracker(),
            err,
            headers,
            string("whatever"));
        tb.asyncRun(respCb);
        tb.master().answer(token);
    }
    {
        string token = tb.master().listen();
        TESTA_ASSERT(token == kResult)
            (token).issue();
        TESTA_ASSERT(resultErr.present()).issue();
        TESTA_ASSERT(resultErr->traceId() == tb.tracker().traceId())
            (*resultErr).issue();
        TESTA_ASSERT(resultErr->requestId() == "")
            (*resultErr).issue();
        TESTA_ASSERT(resultErr->httpStatus() == OTSError::kHttpStatus_CorruptedResponse)
            (*resultErr).issue();
        TESTA_ASSERT(resultErr->errorCode() == OTSError::kErrorCode_CorruptedResponse)
            (*resultErr).issue();
        TESTA_ASSERT(resultErr->message() == "response digest mismatches: "
            "expect=1B2M2Y8AsgTpgAmY7PhCfg==, "
            "real=AIxZJsqGECPB0qNmU/2I4g==")
            (*resultErr).issue();
        tb.master().answer(token);
    }
    tb.deleteClient();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(AsyncClient_ResponseValidation);

namespace {
string searchMap(const http::Headers& headers, const string& key)
{
    http::Headers::const_iterator i = headers.find(key);
    if (i == headers.end()) {
        return string();
    }
    return i->second;
}

void AsyncClient_Sts(const string&)
{
    TestBench tb(new NoRetry());
    tb.mutableCredential().mutableSecurityToken() = "ststoken";
    tb.resetClient();
    Optional<OTSError> resultErr;
    ListTableRequest req;
    ListTableResponse resp;
    ResultCallback resultCb(tb.logger(), tb.slave(), resultErr, resp);
    impl::AsyncClientBase& client = tb.client();
    impl::AsyncClientBase::Context<kApi_ListTable>* ctx =
        new impl::AsyncClientBase::Context<kApi_ListTable>(client, tb.mTracker);
    ctx->build(req, resultCb);
    tb.asyncRun(bind(listTable, ctx));
    {
        string token = tb.master().listen();
        TESTA_ASSERT(token == HttpClientMock::kIssue)
            (token).issue();
        TESTA_ASSERT(searchMap(tb.issueContext().mFixedHeaders, impl::kOTSStsToken) == "ststoken")
            (searchMap(tb.issueContext().mFixedHeaders, impl::kOTSStsToken)).issue();

        http::Headers headers;
        headers[impl::kOTSRequestId] = tb.requestId();
        com::aliyun::tablestore::protocol::ListTableResponse resp;
        resp.add_table_names("pet");
        ResponseCallback respCb(
            tb.issueContext().mCallback,
            tb.tracker(),
            headers,
            resp);
        tb.asyncRun(respCb);
        tb.master().answer(token);
    }
    {
        string token = tb.master().listen();
        TESTA_ASSERT(token == kResult)
            (token).issue();
        TESTA_ASSERT(!resultErr.present())
            (*resultErr).issue();
        TESTA_ASSERT(pp::prettyPrint(resp.tables()) == "[\"pet\"]")
            (resp).issue();
        TESTA_ASSERT(resp.traceId() == tb.tracker().traceId())
            (resp).issue();
        TESTA_ASSERT(resp.requestId() == tb.requestId())
            (resp).issue();
        tb.master().answer(token);
    }
    tb.deleteClient();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(AsyncClient_Sts);

} // namespace core
} // namespace tablestore
} // namespace aliyun

namespace pp {
namespace impl {

template<>
struct PrettyPrinter<com::aliyun::tablestore::protocol::ListTableResponse, void>
{
    void operator()(
        string& out,
        com::aliyun::tablestore::protocol::ListTableResponse resp) const
    {
        out.push_back('[');
        for(int64_t i = 0, sz = resp.table_names_size(); i < sz; ++i) {
            pp::prettyPrint(out, resp.table_names(i));
        }
        out.push_back(']');
    }
};

} // namespace impl
} // namespace pp
