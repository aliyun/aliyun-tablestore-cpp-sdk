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
#include "client.hpp"
#include "response_reader.hpp"
#include "types.hpp"
#include "asio.hpp"
#include "../impl/ots_helper.hpp"
#include "tablestore/core/types.hpp"
#include "tablestore/util/threading.hpp"
#include "tablestore/util/mempool.hpp"
#include "tablestore/util/logger.hpp"
#include "tablestore/util/logging.hpp"
#include "tablestore/util/foreach.hpp"
#include <tr1/memory>
#include <tr1/functional>
#include <deque>

using namespace std;
using namespace std::tr1;
using namespace std::tr1::placeholders;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {
namespace http {

namespace {

class ClientImpl: public Client
{
public:
    explicit ClientImpl(
        Logger&,
        MemPool&,
        const http::Endpoint&,
        const Headers&,
        Asio&,
        const deque<shared_ptr<Actor> >&);
    ~ClientImpl();

    void issue(
        const Tracker&,
        const ResponseCallback& respCb,
        util::MonotonicTime deadline,
        const std::string& path,
        InplaceHeaders& additionalHeaders,
        std::deque<util::MemPiece>& body);
    void close();

private:
    struct Context
    {
        Logger& mLogger;
        Actor& mActor;
        Tracker mTracker;
        boost::atomic<int64_t> mAlreadyCallbacked;
        boost::atomic<int64_t>& mOngoingRequestCount;
        deque<MemPool::Block*> mMemBlocks;
        Timer* mTimer;
        string mPath;
        InplaceHeaders mAdditionalHeaders;
        deque<MemPiece> mBody;

        MutableMemPiece mLastRespBuffer;
        ResponseCallback mRespCb;
        auto_ptr<ResponseReader> mResponseReader;
        Connection* mConnection;

        explicit Context(Logger&, Actor&, boost::atomic<int64_t>& ongoing);
        ~Context();
    };

private:
    void timeout(
        const shared_ptr<Context>&);
    void connectionBorrowed(
        const shared_ptr<Context>&,
        Connection&,
        const Optional<OTSError>&);
    util::MutableMemPiece requestSent(
        const shared_ptr<Context>&,
        const Optional<OTSError>&);
    bool responseReceived(
        const shared_ptr<Context>&,
        int64_t readBytes,
        const util::Optional<OTSError>&,
        util::MutableMemPiece*);
    Actor& assignActor(const Tracker&);
    void invokeCallback(
        const shared_ptr<Context>&,
        Optional<OTSError>&);
    void giveBackConnection(const shared_ptr<Context>&);
    void closeConnection(const shared_ptr<Context>&);

private:
    Logger& mLogger;
    deque<shared_ptr<Actor> > mActors;
    MemPool& mMemPool;
    Endpoint mEndpoint;
    Asio& mAsio;
    deque<MemPool::Block*> mFixedMemBlocks;
    deque<MemPiece> mFixedRequestBuffer;
    boost::atomic<bool> mClosed;
    boost::atomic<int64_t> mOngoingRequestCount;
};

uint8_t* copy(uint8_t* b, uint8_t* e, const MemPiece& mp)
{
    OTS_ASSERT(b + mp.length() <= e);
    const uint8_t* mps = mp.data();
    const uint8_t* mpe = mps + mp.length();
    for(; mps < mpe; ++mps) {
        *b = *mps;
        ++b;
    }
    return b;
}

ClientImpl::ClientImpl(
    Logger& logger,
    MemPool& mpool,
    const http::Endpoint& ep,
    const Headers& fixedHeaders,
    Asio& asio,
    const deque<shared_ptr<Actor> >& actors)
  : mLogger(logger),
    mActors(actors),
    mMemPool(mpool),
    mEndpoint(ep),
    mAsio(asio),
    mClosed(false),
    mOngoingRequestCount(0)
{
    OTS_ASSERT(!actors.empty());

    mFixedMemBlocks.push_back(mMemPool.borrow());
    MutableMemPiece mmp = mFixedMemBlocks.back()->mutablePiece();
    uint8_t* b = mmp.begin();
    uint8_t* c = b;
    uint8_t* e = mmp.end();
    c = copy(c, e, MemPiece::from("Host: "));
    c = copy(c, e, MemPiece::from(mEndpoint.mHost));
    c = copy(c, e, MemPiece::from(":"));
    c = copy(c, e, MemPiece::from(mEndpoint.mPort));
    c = copy(c, e, MemPiece::from("\r\n"));
    FOREACH_ITER(i, fixedHeaders) {
        c = copy(c, e, MemPiece::from(i->first));
        c = copy(c, e, MemPiece::from(": "));
        c = copy(c, e, MemPiece::from(i->second));
        c = copy(c, e, MemPiece::from("\r\n"));
    }
    c = copy(c, e, MemPiece::from("\r\n"));
    mFixedRequestBuffer.push_back(MemPiece(b, c - b));
}

ClientImpl::~ClientImpl()
{
    OTS_ASSERT(mClosed.load(boost::memory_order_acquire))
        .what("HTTP: client must be closed before destroying.");
}

void ClientImpl::close()
{
    if (mClosed.exchange(true, boost::memory_order_acq_rel)) {
        return;
    }

    core::impl::countDown(mLogger, mOngoingRequestCount,
        "HTTP: wait for finishing all requests.",
        "HTTP: all requests were finished.");

    for(; !mFixedMemBlocks.empty(); ) {
        MemPool::BlockHolder hold(mFixedMemBlocks.back());
        mFixedMemBlocks.pop_back();
        hold.giveBack();
    }
}

ClientImpl::Context::Context(
    Logger& logger,
    Actor& actor,
    boost::atomic<int64_t>& ongoing)
  : mLogger(logger),
    mActor(actor),
    mAlreadyCallbacked(false),
    mOngoingRequestCount(ongoing),
    mTimer(NULL),
    mConnection(NULL)
{
    int64_t x = mOngoingRequestCount.fetch_add(1, boost::memory_order_acq_rel);
    OTS_ASSERT(x >= 0)(x);
}

ClientImpl::Context::~Context()
{
    OTS_LOG_DEBUG(mLogger)
        ("Tracker", mTracker)
        ("OngoindRequests", mOngoingRequestCount.load())
        .what("HTTP: destroy ClientImpl::Context.");

    OTS_ASSERT(mConnection == NULL);

    for(; !mMemBlocks.empty(); ) {
        MemPool::BlockHolder hold(mMemBlocks.back());
        mMemBlocks.pop_back();
        hold.giveBack();
    }

    int64_t load = mOngoingRequestCount.fetch_sub(1, boost::memory_order_acq_rel);
    OTS_ASSERT(load >= 1)(load);
    OTS_LOG_DEBUG(mLogger)
        ("Tracker", mTracker)
        ("OngoingRequests", load)
        .what("HTTP: end destroying ClientImpl::Context.");
}

Actor& ClientImpl::assignActor(const Tracker& tracker)
{
    return *mActors[tracker.traceHash() % mActors.size()];
}

void ClientImpl::issue(
    const Tracker& tracker,
    const ResponseCallback& respCb,
    util::MonotonicTime deadline,
    const std::string& path,
    InplaceHeaders& additionalHeaders,
    std::deque<util::MemPiece>& body)
{
    OTS_ASSERT(!mClosed.load(boost::memory_order_acquire))
        (tracker);

    shared_ptr<Context> ctx(
        new Context(mLogger, assignActor(tracker), mOngoingRequestCount));
    ctx->mTracker = tracker;
    ctx->mTimer = &mAsio.startTimer(
        ctx->mTracker, deadline, ctx->mActor,
        bind(&ClientImpl::timeout, this, ctx));
    OTS_LOG_DEBUG(mLogger)
        ("Tracker", tracker)
        .what("HTTP: Start request timer.");
    ctx->mPath = path;
    moveAssign(ctx->mBody, util::move(body));
    moveAssign(ctx->mAdditionalHeaders, util::move(additionalHeaders));
    ctx->mRespCb = respCb;
    ctx->mResponseReader.reset(new ResponseReader(mLogger, ctx->mTracker));
    mAsio.asyncBorrowConnection(
        ctx->mTracker,
        deadline,
        bind(&ClientImpl::connectionBorrowed, this, ctx, _1, _2));
    OTS_LOG_DEBUG(mLogger)
        ("Tracker", tracker)
        .what("HTTP: Start connection borrower.");
}

void ClientImpl::timeout(
    const shared_ptr<Context>& ctx)
{
    OTS_LOG_DEBUG(mLogger)
        ("Tracker", ctx->mTracker)
        .what("HTTP: timeout");
    OTSError e(OTSError::kPredefined_OperationTimeout);
    e.mutableMessage() = "Request timeout.";
    ctx->mActor.pushBack(
        bind(&ClientImpl::invokeCallback,
            this, ctx, Optional<OTSError>(e)));
}

void ClientImpl::connectionBorrowed(
    const shared_ptr<Context>& ctx,
    Connection& conn,
    const Optional<OTSError>& err)
{
    if (err.present()) {
        OTS_LOG_DEBUG(mLogger)
            ("Tracker", ctx->mTracker)
            ("Error", *err)
            .what("HTTP: no connection available.");
        ctx->mActor.pushBack(
            bind(&ClientImpl::invokeCallback,
                this, ctx, err));
        return;
    }

    OTS_LOG_DEBUG(mLogger)
        ("Tracker", ctx->mTracker)
        .what("HTTP: connection borrowed");
    ctx->mConnection = &conn;

    // request line
    static const char sHttpPost[] = "POST ";
    static const char sReqLineSuffix[] = " HTTP/1.1\r\n";
    deque<MemPiece> reqbuf;
    reqbuf.push_back(MemPiece::from(sHttpPost));
    reqbuf.push_back(MemPiece::from(ctx->mPath));
    reqbuf.push_back(MemPiece::from(sReqLineSuffix));

    // additional headers
    static const char sHeaderSep[] = ": ";
    static const char sCrlf[] = "\r\n";
    FOREACH_ITER(i, ctx->mAdditionalHeaders) {
        reqbuf.push_back(i->first);
        reqbuf.push_back(MemPiece::from(sHeaderSep));
        reqbuf.push_back(i->second);
        reqbuf.push_back(MemPiece::from(sCrlf));
    }

    // fixed headers
    FOREACH_ITER(i, mFixedRequestBuffer) {
        reqbuf.push_back(*i);
    }

    // body
    FOREACH_ITER(i, ctx->mBody) {
        reqbuf.push_back(*i);
    }

    ctx->mConnection->asyncIssue(
        ctx->mTracker,
        reqbuf,
        assignActor(ctx->mTracker),
        bind(&ClientImpl::requestSent, this, ctx, _1),
        bind(&ClientImpl::responseReceived, this, ctx, _1, _2, _3));
}

util::MutableMemPiece ClientImpl::requestSent(
    const shared_ptr<Context>& ctx,
    const Optional<OTSError>& err)
{
    if (err.present()) {
        OTS_LOG_DEBUG(mLogger)
            ("Tracker", ctx->mTracker)
            ("Error", *err)
            .what("HTTP: fail to send request.");
        ctx->mActor.pushBack(
            bind(&ClientImpl::invokeCallback, this, ctx, err));
        return MutableMemPiece();
    }

    OTS_LOG_DEBUG(mLogger)
        ("Tracker", ctx->mTracker)
        .what("request was sent");
    ctx->mMemBlocks.push_back(mMemPool.borrow());
    ctx->mLastRespBuffer = ctx->mMemBlocks.back()->mutablePiece();
    return ctx->mLastRespBuffer;
}

bool ClientImpl::responseReceived(
    const shared_ptr<Context>& ctx,
    int64_t readBytes,
    const util::Optional<OTSError>& err,
    MutableMemPiece* newPiece)
{
    if (err.present()) {
        OTS_LOG_DEBUG(mLogger)
            ("Tracker", ctx->mTracker)
            ("Error", *err)
            .what("HTTP: fail to receive response.");
        ctx->mActor.pushBack(
            bind(&ClientImpl::invokeCallback,
                this, ctx, err));
        return false;
    }

    OTS_LOG_DEBUG(mLogger)
        ("Tracker", ctx->mTracker)
        ("ReadBytes", readBytes)
        .what("HTTP: a new piece of response came in.");
    OTS_ASSERT(readBytes <= ctx->mLastRespBuffer.length())
        (ctx->mTracker)
        (readBytes)
        (reinterpret_cast<uintptr_t>(ctx->mLastRespBuffer.begin()))
        (ctx->mLastRespBuffer.length());
    MemPiece mp(ctx->mLastRespBuffer.begin(), readBytes);
    ResponseReader::RequireMore more = ResponseReader::STOP;
    Optional<OTSError> e = ctx->mResponseReader->feed(more, mp);
    OTS_ASSERT(!e.present())
        (ctx->mTracker)
        (*e);
    if (more == ResponseReader::STOP) {
        OTS_LOG_DEBUG(mLogger)
            ("Tracker", ctx->mTracker)
            .what("HTTP: response finishes properly.");
        ctx->mActor.pushBack(
            bind(&ClientImpl::invokeCallback,
                this, ctx, Optional<OTSError>()));
        return false;
    } else {
        OTS_ASSERT(readBytes <= ctx->mLastRespBuffer.length())
            (readBytes)
            (ctx->mLastRespBuffer.length());
        if (readBytes == ctx->mLastRespBuffer.length()) {
            ctx->mMemBlocks.push_back(mMemPool.borrow());
            *newPiece = ctx->mMemBlocks.back()->mutablePiece();
            ctx->mLastRespBuffer = *newPiece;
        } else {
            uint8_t* b = ctx->mLastRespBuffer.begin();
            ctx->mLastRespBuffer = ctx->mLastRespBuffer.subpiece(b + readBytes);
        }
        return true;
    }
}

void ClientImpl::giveBackConnection(const shared_ptr<Context>& ctx)
{
    OTS_LOG_DEBUG(mLogger)
        ("Tracker", ctx->mTracker)
        .what("HTTP: give back a connection.");
    OTS_ASSERT(ctx->mConnection != NULL)
        (ctx->mTracker);
    ctx->mConnection->done();
    ctx->mConnection = NULL;
}

void ClientImpl::closeConnection(const shared_ptr<Context>& ctx)
{
    OTS_LOG_DEBUG(mLogger)
        ("Tracker", ctx->mTracker)
        .what("HTTP: close a connection.");
    if (ctx->mConnection != NULL) {
        // if an error occurs when borrowing a connection,
        // ctx->mConnection will be null.
        ctx->mConnection->destroy();
        ctx->mConnection = NULL;
    }
}

void ClientImpl::invokeCallback(
    const shared_ptr<Context>& ctx,
    Optional<OTSError>& err)
{
    if (ctx->mAlreadyCallbacked.exchange(true, boost::memory_order_acq_rel)) {
        return;
    }

    if (ctx->mTimer != NULL) {
        ctx->mTimer->cancel();
    }

    if (err.present()) {
        OTS_LOG_DEBUG(mLogger)
            ("Tracker", ctx->mTracker)
            ("Error", *err)
            .what("HTTP: invoke callback");
        closeConnection(ctx);
        InplaceHeaders headers;
        deque<MemPiece> body;
        err->mutableTraceId() = ctx->mTracker.traceId();
        ctx->mRespCb(ctx->mTracker, err, headers, body);
    } else {
        giveBackConnection(ctx);
        InplaceHeaders headers;
        moveAssign(headers, util::move(ctx->mResponseReader->mutableHeaders()));
        deque<MemPiece> body;
        moveAssign(body, util::move(ctx->mResponseReader->mutableBody()));
        int64_t httpStatus = ctx->mResponseReader->httpStatus();
        OTS_LOG_DEBUG(mLogger)
            ("Tracker", ctx->mTracker)
            ("HttpStatus", httpStatus)
            ("BytesOfHeaders", headers.size())
            ("Body", body)
            .what("HTTP: invoke callback");
        Optional<OTSError> ex;
        if (httpStatus < 200 || httpStatus >= 300) {
            OTSError x;
            x.mutableHttpStatus() = httpStatus;
            ex.reset(util::move(x));
        }
        ctx->mRespCb(ctx->mTracker, ex, headers, body);
    }
}

} // namespace

Client* Client::create(
    Logger& logger,
    MemPool& mpool,
    const http::Endpoint& ep,
    const Headers& headers,
    Asio& asio,
    const deque<shared_ptr<Actor> >& actors)
{
    return new ClientImpl(logger, mpool, ep, headers, asio, actors);
}

} // namespace http
} // namespace core
} // namespace tablestore
} // namespace aliyun
