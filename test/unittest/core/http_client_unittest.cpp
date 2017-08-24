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
#include "common.hpp"
#include "src/tablestore/core/http/client.hpp"
#include "src/tablestore/core/http/asio.hpp"
#include "src/tablestore/core/http/types.hpp"
#include "tablestore/core/types.hpp"
#include "tablestore/util/threading.hpp"
#include "tablestore/util/mempool.hpp"
#include "tablestore/util/logging.hpp"
#include "tablestore/util/foreach.hpp"
#include "tablestore/util/move.hpp"
#include "tablestore/util/prettyprint.hpp"
#include "tablestore/util/assert.hpp"
#include "testa/testa.hpp"
#include <boost/ref.hpp>
#include <tr1/functional>
#include <tr1/tuple>
#include <deque>
#include <string>
#include <memory>

using namespace std;
using namespace std::tr1;
using namespace std::tr1::placeholders;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {

namespace {

class ConnectionMock: public http::Connection
{
public:
    static const string kIssue;
    static const string kGiveBack;
    static const string kClose;

    explicit ConnectionMock(
        Logger& logger,
        Slave& slave)
      : mLogger(logger),
        mSlave(slave)
    {}

    void asyncIssue(
        const Tracker& tracker,
        deque<MemPiece>& req,
        Actor&,
        const RequestCompletionHandler& reqComplete,
        const ResponseHandler& resp)
    {
        mTracker = tracker;
        moveAssign(mReq, util::move(req));
        mReqComplete = reqComplete;
        mRespHandler = resp;
        mSlave.ask(kIssue);
    }

    void done()
    {
        OTS_LOG_DEBUG(mLogger)
            .what("UT: give back connection.");
        mSlave.ask(kGiveBack);
    }

    void destroy()
    {
        OTS_LOG_DEBUG(mLogger)
            .what("UT: close connection.");
        mSlave.ask(kClose);
    }

    void reset()
    {
        mTracker = Tracker();
        mReq.clear();
        mReqComplete = RequestCompletionHandler();
        mRespHandler = ResponseHandler();
    }

public:
    Logger& mLogger;
    Slave& mSlave;

    Tracker mTracker;
    deque<MemPiece> mReq;
    RequestCompletionHandler mReqComplete;
    ResponseHandler mRespHandler;
};

const string ConnectionMock::kIssue("ConnectionMock::kIssue");
const string ConnectionMock::kGiveBack("ConnectionMock::kGiveBack");
const string ConnectionMock::kClose("ConnectionMock::kClose");

class AsioMock;

class TimerMock: public http::Timer
{
public:
    explicit TimerMock(AsioMock& asio)
      : mAsioMock(asio)
    {}

    void cancel();

public:
    AsioMock& mAsioMock;
};

class AsioMock: public http::Asio
{
public:
    static const string kBorrowConnection;
    static const string kStartTimer;
    static const string kCancelTimer;

    explicit AsioMock(Logger&, Slave&);
    ~AsioMock()
    {
        for(; !mTimers.empty(); mTimers.pop_back()) {
            delete mTimers.back();
        }
    }

    void asyncBorrowConnection(
        const Tracker& tracker,
        util::MonotonicTime deadline,
        const BorrowConnectionHandler& cb)
    {
        mTracker = tracker;
        mDeadline = deadline;
        mConnCb = cb;
        mSlave.ask(kBorrowConnection);
    }

    http::Timer& startTimer(
        const Tracker& tracker,
        util::MonotonicTime deadline,
        util::Actor&,
        const http::TimeoutCallback& cb)
    {
        OTS_LOG_DEBUG(mLogger)
            ("Deadline", deadline)
            .what("UT: start a timer.");
        mTracker = tracker;
        mDeadline = deadline;
        mToCb = cb;
        mSlave.ask(kStartTimer);
        http::Timer* res = new TimerMock(*this);
        mTimers.push_back(res);
        return *res;
    }

    void close()
    {
        OTS_LOG_DEBUG(mLogger)
            .what("UT: close Asio.");
    }

    void reset()
    {
        mTracker = Tracker();
        mDeadline = MonotonicTime();
        mConnCb = BorrowConnectionHandler();
        mToCb = http::TimeoutCallback();
    }

public:
    Logger& mLogger;
    Slave& mSlave;
    deque<http::Timer*> mTimers;

    Tracker mTracker;
    MonotonicTime mDeadline;
    BorrowConnectionHandler mConnCb;
    http::TimeoutCallback mToCb;
};

const string AsioMock::kBorrowConnection("AsioMock::kBorrowConnection");
const string AsioMock::kStartTimer("AsioMock::kStartTimer");
const string AsioMock::kCancelTimer("AsioMock::kCancelTimer");

AsioMock::AsioMock(Logger& logger, Slave& slave)
  : mLogger(logger),
    mSlave(slave)
{}

void TimerMock::cancel()
{
    OTS_LOG_DEBUG(mAsioMock.mLogger)
        .what("UT: cancel a timer");
    mAsioMock.mToCb = http::TimeoutCallback();
    mAsioMock.mSlave.ask(AsioMock::kCancelTimer);
}

MutableMemPiece write(
    MutableMemPiece out,
    const MemPiece& in)
{
    OTS_ASSERT(out.length() >= in.length())
        (in.length())
        (out.length());
    uint8_t* b = out.begin();
    uint8_t* e = out.end();
    for(int64_t i = 0; b < e && i < in.length(); ++i, ++b) {
        *b = in.get(i);
    }
    return out.subpiece(b);
}

string onepiece(const deque<MemPiece>& pieces)
{
    string res;
    FOREACH_ITER(i, pieces) {
        res.append((char*) i->data(), i->length());
    }
    return res;
}

struct IssueContext
{
    http::Client* mClient;
    const Tracker* mTracker;
    http::ResponseCallback mRespCb;
    MonotonicTime mDeadline;
    string mPath;
    http::InplaceHeaders* mAddHeaders;
    deque<MemPiece>* mBody;
};

void clientIssuer(IssueContext& ctx)
{
    http::Client& client = *ctx.mClient;
    const Tracker& tracker = *ctx.mTracker;
    const http::ResponseCallback& respCb = ctx.mRespCb;
    MonotonicTime deadline = ctx.mDeadline;
    const string& path = ctx.mPath;
    http::InplaceHeaders& addHeaders = *ctx.mAddHeaders;
    deque<MemPiece>& body = *ctx.mBody;

    client.issue(
        tracker,
        respCb,
        deadline,
        path,
        addHeaders,
        body);
}

template<class T>
void returnWrapper(
    T& out,
    Slave& slave,
    const string& token,
    const function<T()>& fn)
{
    out = fn();
    slave.ask(token);
}

const string kReqCompl("reqCompl");
const string kRespHdl("respHdl");
const string kRespCb("respCb");

void respCb(
    Logger& logger,
    Slave& slave,
    const Tracker& tracker,
    const Optional<OTSError>& err,
    const http::InplaceHeaders& headers,
    deque<MemPiece>& body)
{
    if (err.present()) {
        OTS_LOG_INFO(logger)
            ("Tracker", tracker)
            ("Error", *err)
            .what("UT: error occurs.");
    } else {
        OTS_LOG_INFO(logger)
            ("Tracker", tracker)
            .what("UT: response received.");
    }
    slave.ask(kRespCb);
}

struct TestBench
{
    MasterSlave mMasterSlave;
    http::Endpoint mEndpoint;
    auto_ptr<Logger> mLogger;
    auto_ptr<MemPool> mPool;
    deque<shared_ptr<Actor> > mActors;
    Actor* mInvoker;
    http::Headers mFixedHeaders;
    deque<MemPiece> mBody;
    http::InplaceHeaders mAddHeaders;
    Tracker mTracker;
    AsioMock mAsio;
    auto_ptr<ConnectionMock> mConn;
    MonotonicTime mDeadline;
    auto_ptr<http::Client> mClient;

    explicit TestBench();

    void issue();
    string feedConnection();
    MutableMemPiece completeRequest(const Optional<OTSError>&);
    void asyncFeedResponsePiece(
        bool& goOn,
        MutableMemPiece& newPiece,
        MutableMemPiece,
        const MemPiece&,
        const Optional<OTSError>&);
    void waitForTimerCanceled();
    void waitForResponseHandled();
    void waitForConnectionReturned();
    void waitForConnectionClose();
    void waitForResponseCallback();
    void asyncTimeout();
};

TestBench::TestBench()
  : mLogger(createLogger("/", Logger::kDebug)),
    mTracker("track"),
    mAsio(*mLogger, mMasterSlave.slave())
{
    mEndpoint.mProtocol = http::Endpoint::HTTP;
    mEndpoint.mHost = "anyhost";
    mEndpoint.mPort = "80";

    mPool.reset(new IncrementalMemPool());

    mActors.push_back(shared_ptr<Actor>(new Actor()));
    mInvoker = mActors.front().get();

    mFixedHeaders["FixedHeader"] = "Haha";
    mAddHeaders[MemPiece::from("AddHeader")] = MemPiece::from("Hoho");

    mConn.reset(new ConnectionMock(*mLogger, mMasterSlave.slave()));

    mDeadline = MonotonicTime::now() + Duration::fromMin(1);

    mClient.reset(http::Client::create(
            *mLogger,
            *mPool,
            mEndpoint,
            mFixedHeaders,
            mAsio,
            mActors));
}

void TestBench::issue()
{
    IssueContext ctx;
    ctx.mClient = mClient.get();
    ctx.mTracker = &mTracker;
    ctx.mRespCb = bind(respCb, boost::ref(*mLogger), ref(mMasterSlave.slave()),
        _1, _2, _3, _4);
    ctx.mDeadline = mDeadline;
    ctx.mPath = "/ListTable";
    ctx.mAddHeaders = &mAddHeaders;
    ctx.mBody = &mBody;
    mInvoker->pushBack(bind(clientIssuer, ctx));
    {
        string token = mMasterSlave.master().listen();
        TESTA_ASSERT(token == AsioMock::kStartTimer)
            (token).issue();
        TESTA_ASSERT(
            pp::prettyPrint(mAsio.mTracker) == pp::prettyPrint(mTracker))
            (mAsio.mTracker).issue();
        TESTA_ASSERT(mAsio.mDeadline == mDeadline)
            (mAsio.mDeadline)
            (mDeadline).issue();
        mMasterSlave.master().answer(token);
    }
    {
        string token = mMasterSlave.master().listen();
        TESTA_ASSERT(token == AsioMock::kBorrowConnection)
            (token).issue();
        TESTA_ASSERT(
            pp::prettyPrint(mAsio.mTracker) == pp::prettyPrint(mTracker))
            (mAsio.mTracker).issue();
        TESTA_ASSERT(mAsio.mDeadline == mDeadline)
            (mAsio.mDeadline)
            (mDeadline).issue();
        mMasterSlave.master().answer(token);
    }
}

string TestBench::feedConnection()
{
    mInvoker->pushBack(bind(mAsio.mConnCb, boost::ref(*mConn), Optional<OTSError>()));
    mAsio.mConnCb = http::Asio::BorrowConnectionHandler();

    string token = mMasterSlave.master().listen();
    TESTA_ASSERT(token == ConnectionMock::kIssue)
        (token).issue();
    TESTA_ASSERT(pp::prettyPrint(mConn->mTracker) == pp::prettyPrint(mTracker))
        (mConn->mTracker).issue();
    string req = onepiece(mConn->mReq);
    mMasterSlave.master().answer(token);
    return req;
}

MutableMemPiece TestBench::completeRequest(const Optional<OTSError>& err)
{
    MutableMemPiece mmp;
    function<MutableMemPiece()> fn = bind(mConn->mReqComplete, err);
    mInvoker->pushBack(
        bind(&returnWrapper<MutableMemPiece>,
            ref(mmp),
            ref(mMasterSlave.slave()),
            cref(kReqCompl),
            fn));
    string token = mMasterSlave.master().listen();
    TESTA_ASSERT(token == kReqCompl)
        (token).issue();
    mMasterSlave.master().answer(token);
    return mmp;
}

void TestBench::asyncFeedResponsePiece(
    bool& goOn,
    MutableMemPiece& newPiece,
    MutableMemPiece buf,
    const MemPiece& content,
    const Optional<OTSError>& err)
{
    MutableMemPiece mmp1 = write(buf, content);
    function<bool()> fn = bind(mConn->mRespHandler, 
        buf.length() - mmp1.length(), err, &newPiece);
    mInvoker->pushBack(
        bind(&returnWrapper<bool>,
            ref(goOn),
            ref(mMasterSlave.slave()),
            cref(kRespHdl),
            fn));
}

void TestBench::waitForTimerCanceled()
{
    mAsio.mConnCb = http::Asio::BorrowConnectionHandler();
    string token = mMasterSlave.master().listen();
    TESTA_ASSERT(token == AsioMock::kCancelTimer)
        (token).issue();
    mMasterSlave.master().answer(token);
}

void TestBench::waitForResponseHandled()
{
    string token = mMasterSlave.master().listen();
    TESTA_ASSERT(token == kRespHdl)
        (token).issue();
    mMasterSlave.master().answer(token);
}

void TestBench::waitForConnectionReturned()
{
    string token = mMasterSlave.master().listen();
    TESTA_ASSERT(token == ConnectionMock::kGiveBack)
        (token).issue();
    mConn.reset();
    mMasterSlave.master().answer(token);
}

void TestBench::waitForConnectionClose()
{
    string token = mMasterSlave.master().listen();
    TESTA_ASSERT(token == ConnectionMock::kClose)
        (token).issue();
    mConn.reset();
    mMasterSlave.master().answer(token);
}

void TestBench::waitForResponseCallback()
{
    string token = mMasterSlave.master().listen();
    TESTA_ASSERT(token == kRespCb)
        (token).issue();
    mMasterSlave.master().answer(token);
}

void TestBench::asyncTimeout()
{
    mInvoker->pushBack(mAsio.mToCb);
    mAsio.mToCb = http::TimeoutCallback();
}


void HttpClient_normal(const string&)
{
    TestBench tb;
    tb.issue();
    {
        string req = tb.feedConnection();
        TESTA_ASSERT(req ==
            "POST /ListTable HTTP/1.1\r\n"
            "AddHeader: Hoho\r\n"
            "Host: anyhost:80\r\n"
            "FixedHeader: Haha\r\n"
            "\r\n")
            (req).issue();
    }
    MutableMemPiece mmp = tb.completeRequest(Optional<OTSError>());

    MutableMemPiece mmp1;
    bool ret = true;
    tb.asyncFeedResponsePiece(
        ret, mmp1, mmp,
        MemPiece::from(
            "HTTP/1.1 200 OK\r\n"
            "Content-Length: 0\r\n"
            "\r\n"),
        Optional<OTSError>());
    tb.waitForResponseHandled();
    tb.waitForTimerCanceled();
    tb.waitForConnectionReturned();
    tb.waitForResponseCallback();

    TESTA_ASSERT(!ret).issue();
    TESTA_ASSERT(mmp1.length() == 0)(mmp1).issue();
    tb.mClient->close();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(HttpClient_normal);

namespace {
void HttpClient_request_fail(const string&)
{
    TestBench tb;
    tb.issue();
    tb.feedConnection();

    {
        OTSError err;
        err.mutableHttpStatus() = OTSError::kHttpStatus_WriteRequestFail;
        err.mutableErrorCode() = OTSError::kErrorCode_WriteRequestFail;
        MutableMemPiece x = tb.completeRequest(Optional<OTSError>(err));
        TESTA_ASSERT(x.length() == 0)
            (x.length()).issue();
    }
    tb.waitForTimerCanceled();
    tb.waitForConnectionClose();
    tb.waitForResponseCallback();
    tb.mClient->close();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(HttpClient_request_fail);

namespace {
void HttpClient_response_fail(const string&)
{
    TestBench tb;
    tb.issue();
    tb.feedConnection();
    MutableMemPiece mmp = tb.completeRequest(Optional<OTSError>());
    (void) mmp;

    bool ret = true;
    {
        MutableMemPiece mmp1;
        OTSError err;
        err.mutableHttpStatus() = OTSError::kHttpStatus_CorruptedResponse;
        err.mutableErrorCode() = OTSError::kErrorCode_CorruptedResponse;
        tb.asyncFeedResponsePiece(
            ret, mmp1, mmp,
            MemPiece(),
            Optional<OTSError>(err));
    }
    tb.waitForResponseHandled();
    tb.waitForTimerCanceled();
    tb.waitForConnectionClose();
    tb.waitForResponseCallback();
    TESTA_ASSERT(!ret).issue();
    tb.mClient->close();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(HttpClient_response_fail);

namespace {
void HttpClient_BorrowConnectionTimeout(const string&)
{
    TestBench tb;
    tb.issue();
    tb.asyncTimeout();
    tb.waitForTimerCanceled();
    tb.waitForResponseCallback();
    tb.mClient->close();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(HttpClient_BorrowConnectionTimeout);

namespace {
void HttpClient_request_timeout(const string&)
{
    TestBench tb;
    tb.issue();
    tb.feedConnection();
    tb.asyncTimeout();
    tb.waitForTimerCanceled();
    tb.waitForConnectionClose();
    tb.waitForResponseCallback();
    tb.mClient->close();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(HttpClient_request_timeout);

} // namespace core
} // namespace tablestore
} // namespace aliyun

