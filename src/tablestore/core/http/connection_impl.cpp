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
#include "connection_impl.hpp"
#include "asio_impl.hpp"
#include "../impl/ots_helper.hpp"
#include "tablestore/util/logging.hpp"
#include <boost/ref.hpp>

using namespace std;
using namespace std::tr1;
using namespace std::tr1::placeholders;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {
namespace http {

ConnectionBase::ConnectionBase(
    Connector& cntor,
    util::Logger& logger)
  : mConnector(cntor),
    mLogger(logger)
{}

ConnectionBase::~ConnectionBase()
{}

void ConnectionBase::done()
{
    mConnector.giveBack(this);
}

void ConnectionBase::destroy()
{
    mConnector.destroy(this);
}

void ConnectionBase::handleResponse(
    const boost::system::error_code& err,
    size_t size)
{}

ConnectionBase::RequestContext::~RequestContext()
{
    OTS_LOG_DEBUG(mLogger)
        ("Tracker", mTracker)
        .what("CONN: destroying ConnectionBase::RequestContext.");
}

namespace {

template<Endpoint::Protocol kProto>
class ConnectionTraits {};

template<>
class ConnectionTraits<Endpoint::HTTP>
{
public:
    typedef boost::asio::ip::tcp::socket Backbone;
    typedef boost::asio::ip::tcp::socket FrontStream;

    explicit ConnectionTraits(boost::asio::io_service&);
    Backbone& backbone();
    FrontStream& frontStream();
    void asyncHandshake(
        const Endpoint&,
        const std::tr1::function<void(const boost::system::error_code&)>&);
    
private:
    boost::asio::ip::tcp::socket mSocket;
};

template<>
class ConnectionTraits<Endpoint::HTTPS>
{
public:
    typedef boost::asio::basic_socket<boost::asio::ip::tcp, boost::asio::stream_socket_service<boost::asio::ip::tcp> > Backbone;
    typedef boost::asio::ssl::stream<boost::asio::ip::tcp::socket> FrontStream;

    explicit ConnectionTraits(boost::asio::io_service&);
    Backbone& backbone();
    FrontStream& frontStream();
    void asyncHandshake(
        const Endpoint&,
        const std::tr1::function<void(const boost::system::error_code&)>&);
    
private:
    std::auto_ptr<FrontStream> mSslStream;
};

template<Endpoint::Protocol kProto>
class ConnectionImpl: public ConnectionBase
{
public:
    typedef typename ConnectionTraits<kProto>::Backbone Backbone;
    typedef typename ConnectionTraits<kProto>::FrontStream FrontStream;

    explicit ConnectionImpl(
        Connector&,
        util::Logger& logger,
        boost::asio::io_service&);
    virtual ~ConnectionImpl();

    void asyncIssue(
        const Tracker& tracker,
        std::deque<util::MemPiece>& req,
        util::Actor& actor,
        const RequestCompletionHandler&,
        const ResponseHandler&);
    void asyncConnect(
        boost::asio::ip::tcp::resolver::iterator,
        const std::tr1::function<
            void(const boost::system::error_code&,
              boost::asio::ip::tcp::resolver::iterator)>&);
    void gentlyClose();
    
    void asyncHandshake(
        const Endpoint& ep,
        const std::tr1::function<void(const boost::system::error_code&)>& cb)
    {
        mTraits.asyncHandshake(ep, cb);
    }

    Backbone& backbone()
    {
        return mTraits.backbone();
    }

    FrontStream& frontStream()
    {
        return mTraits.frontStream();
    }

private:
    void handleWrite(
        const std::tr1::shared_ptr<RequestContext>&,
        const boost::system::error_code&,
        size_t);
    void handleRequestComplete(
        const std::tr1::shared_ptr<RequestContext>&,
        const boost::system::error_code&,
        size_t);
    void requestComplete(
        const std::tr1::shared_ptr<RequestContext>&,
        const util::Optional<OTSError>&,
        int64_t);
    size_t handleResponseCompletionCondition(
        const std::tr1::shared_ptr<RequestContext>&,
        const boost::system::error_code&,
        size_t);
    void responseCompletionCondition(
        const std::tr1::shared_ptr<RequestContext>&,
        const boost::system::error_code&,
        size_t);

private:
    ConnectionTraits<kProto> mTraits;
};

ConnectionTraits<Endpoint::HTTP>::ConnectionTraits(
    boost::asio::io_service& ios)
  : mSocket(ios)
{}

typename ConnectionTraits<Endpoint::HTTP>::Backbone&
ConnectionTraits<Endpoint::HTTP>::backbone()
{
    return mSocket;
}

typename ConnectionTraits<Endpoint::HTTP>::FrontStream&
ConnectionTraits<Endpoint::HTTP>::frontStream()
{
    return mSocket;
}

void ConnectionTraits<Endpoint::HTTP>::asyncHandshake(
    const Endpoint& ep,
    const function<void(const boost::system::error_code&)>& cb)
{
    boost::system::error_code ec;
    cb(ec);
}

ConnectionTraits<Endpoint::HTTPS>::ConnectionTraits(
    boost::asio::io_service& ios)
{
    boost::asio::ssl::context ctx(boost::asio::ssl::context::sslv23);
    ctx.set_default_verify_paths();
    mSslStream.reset(new FrontStream(ios, ctx));
}

typename ConnectionTraits<Endpoint::HTTPS>::Backbone&
ConnectionTraits<Endpoint::HTTPS>::backbone()
{
    return mSslStream->lowest_layer();
}

typename ConnectionTraits<Endpoint::HTTPS>::FrontStream&
ConnectionTraits<Endpoint::HTTPS>::frontStream()
{
    return *mSslStream;
}

void ConnectionTraits<Endpoint::HTTPS>::asyncHandshake(
    const Endpoint& ep,
    const function<void(const boost::system::error_code&)>& cb)
{
    frontStream().set_verify_mode(boost::asio::ssl::verify_peer);
    frontStream().set_verify_callback(boost::asio::ssl::rfc2818_verification(ep.mHost));
    frontStream().async_handshake(FrontStream::client, cb);
}

template<Endpoint::Protocol kProto>
ConnectionImpl<kProto>::ConnectionImpl(
    Connector& cntor,
    Logger& logger,
    boost::asio::io_service& ios)
  : ConnectionBase(cntor, logger),
    mTraits(ios)
{}

template<Endpoint::Protocol kProto>
ConnectionImpl<kProto>::~ConnectionImpl()
{
    OTS_LOG_DEBUG(mLogger)
        ("Connection", tracker())
        .what("CONN: destroying ConnectionImpl.");
}

template<Endpoint::Protocol kProto>
void ConnectionImpl<kProto>::asyncIssue(
    const Tracker& reqTracker,
    deque<MemPiece>& req,
    Actor& actor,
    const RequestCompletionHandler& reqCompletion,
    const ResponseHandler& respCompletion)
{
    OTS_LOG_DEBUG(mLogger)
        ("Request", reqTracker)
        ("Connection", tracker())
        ("RequestSize", totalLength(req))
        .what("CONN: issue a request.");
    
    shared_ptr<RequestContext> ctx(new RequestContext(
            mLogger, reqTracker, actor, reqCompletion, respCompletion));
    vector<boost::asio::const_buffer> buf;
    FOREACH_ITER(i, req) {
        buf.push_back(boost::asio::buffer(i->data(), i->length()));
    }
    boost::asio::async_write(frontStream(), buf,
        bind(&ConnectionImpl::handleWrite, this, ctx, _1, _2));
}

template<Endpoint::Protocol kProto>
void ConnectionImpl<kProto>::asyncConnect(
    boost::asio::ip::tcp::resolver::iterator epIter,
    const function<
        void(const boost::system::error_code&,
            boost::asio::ip::tcp::resolver::iterator)>& cb)
{
    boost::asio::async_connect(backbone(), epIter, cb);
}

template<Endpoint::Protocol kProto>
void ConnectionImpl<kProto>::gentlyClose()
{
    OTS_LOG_DEBUG(mLogger)
        ("Connection", tracker())
        .what("CONN: close a connection gently.");
    try {
        backbone().shutdown(boost::asio::ip::tcp::socket::shutdown_both);
    } catch(const boost::system::system_error& ex) {
        OTS_LOG_INFO(mLogger)
            ("Connection", tracker())
            ("Exception", ex.what())
            .what("CONN: error in shutdowning socket.");
    }
    try {
        backbone().close();
    } catch(const boost::system::system_error& ex) {
        OTS_LOG_INFO(mLogger)
            ("Connection", tracker())
            ("Exception", ex.what())
            .what("CONN: error in closing socket.");
    }
}


template<Endpoint::Protocol kProto>
void ConnectionImpl<kProto>::handleWrite(
    const shared_ptr<RequestContext>& ctx,
    const boost::system::error_code& err,
    size_t bytes)
{
    if (err == boost::asio::error::operation_aborted) {
        OTS_LOG_DEBUG(mLogger)
            ("Request", ctx->mTracker)
            ("Connection", tracker())
            .what("CONN: a sending connection is canceled.");
        delete this;
        return;
    }
    ctx->mActor.pushBack(
        bind(&ConnectionImpl::handleRequestComplete,
            this, ctx, err, bytes));
}

template<Endpoint::Protocol kProto>
void ConnectionImpl<kProto>::handleRequestComplete(
    const shared_ptr<RequestContext>& ctx,
    const boost::system::error_code& err,
    size_t bytes)
{
    if (err) {
        OTSError e(OTSError::kPredefined_WriteRequestFail);
        e.mutableMessage() = err.message();
        e.mutableTraceId() = ctx->mTracker.traceId();
        ctx->mActor.pushBack(
            bind(&ConnectionImpl::requestComplete,
                this,
                ctx,
                Optional<OTSError>(e),
                bytes));
        return;
    }
    ctx->mActor.pushBack(
        bind(&ConnectionImpl::requestComplete,
            this,
            ctx,
            Optional<OTSError>(),
            bytes));
}

template<Endpoint::Protocol kProto>
void ConnectionImpl<kProto>::requestComplete(
    const shared_ptr<RequestContext>& ctx,
    const Optional<OTSError>& err,
    int64_t bytes)
{
    if (!err.present()) {
        OTS_LOG_DEBUG(mLogger)
            ("Request", ctx->mTracker)
            ("Connection", tracker())
            ("SentBytes", bytes)
            .what("CONN: written");
        ctx->mLastResponseBuffer = ctx->mRequestCompletion(err);
        boost::asio::async_read(frontStream(),
            boost::asio::buffer(
                ctx->mLastResponseBuffer.begin(),
                ctx->mLastResponseBuffer.length()),
            bind(&ConnectionImpl::handleResponseCompletionCondition,
                this, ctx, _1, _2),
            bind(&ConnectionImpl::handleResponse,
                _1, _2));
    } else {
        OTS_LOG_ERROR(mLogger)
            ("Request", ctx->mTracker)
            ("Connection", tracker())
            ("Error", *err)
            .what("CONN: error in sending request");
        MutableMemPiece p = ctx->mRequestCompletion(err);
        (void) p;
    }
}

template<Endpoint::Protocol kProto>
size_t ConnectionImpl<kProto>::handleResponseCompletionCondition(
    const shared_ptr<RequestContext>& ctx,
    const boost::system::error_code& err,
    size_t size)
{
    if (err == boost::asio::error::operation_aborted) {
        OTS_LOG_DEBUG(mLogger)
            ("Request", ctx->mTracker)
            ("Connection", tracker())
            .what("CONN: a receiving connection is canceled.");
        delete this;
        return 0;
    }
    if (size == 0) {
        return ctx->mLastResponseBuffer.length();
    } else {
        ctx->mActor.pushBack(
            bind(&ConnectionImpl::responseCompletionCondition,
                this, ctx, err, size));
        return 0;
    }
}

template<Endpoint::Protocol kProto>
void ConnectionImpl<kProto>::responseCompletionCondition(
    const shared_ptr<RequestContext>& ctx,
    const boost::system::error_code& err,
    size_t size)
{
    if (!err) {
        OTS_LOG_DEBUG(mLogger)
            ("Request", ctx->mTracker)
            ("Connection", tracker())
            ("ReadBytes", size)
            .what("CONN: a piece of response comes in.");
        MutableMemPiece mp;
        bool ret = ctx->mResponseCompletion(
            size, Optional<OTSError>(), &mp);
        if (!ret) {
            return;
        } else if (mp.length() == 0) {
            ctx->mLastResponseBuffer = ctx->mLastResponseBuffer.subpiece(
                ctx->mLastResponseBuffer.begin() + size);
        } else {
            ctx->mLastResponseBuffer = mp;
        }
        boost::asio::async_read(frontStream(),
            boost::asio::buffer(
                ctx->mLastResponseBuffer.begin(),
                ctx->mLastResponseBuffer.length()),
            bind(&ConnectionImpl::handleResponseCompletionCondition,
                this, ctx, _1, _2),
            bind(&ConnectionImpl::handleResponse,
                _1, _2));
    } else {
        OTS_LOG_ERROR(mLogger)
            ("Request", ctx->mTracker)
            ("Connection", tracker())
            ("Error", err.message())
            .what("CONN: error in receiving response.");
        OTSError e(OTSError::kPredefined_CorruptedResponse);
        e.mutableMessage() = err.message();
        e.mutableTraceId() = ctx->mTracker.traceId();
        MutableMemPiece mp;
        bool ret = ctx->mResponseCompletion(
            size, Optional<OTSError>(e), &mp);
        (void) ret;
    }
}

} // namespace

Scheduler::WaitForConnection::~WaitForConnection()
{
    OTS_LOG_DEBUG(mLogger)
        ("Request", mTracker)
        .what("CONN: destroying AsioImpl::WaitForConnection.");
}

void Scheduler::WaitForConnection::close()
{
    OTSError e(OTSError::kPredefined_NoConnectionAvailable);
    e.mutableMessage() =
        "Fails requests on the waiting list while closing.";
    mActor.pushBack(
        bind(mCallback,
            boost::ref(*static_cast<Connection*>(NULL)),
            Optional<OTSError>(e)));
}

Connector::Connector(
    AsioImpl& asio,
    const Endpoint& ep,
    int64_t maxConnections)
  : mLogger(asio.mutableLogger()),
    mAsio(asio),
    mEndpoint(ep),
    mMaxConnections(maxConnections),
    mClosed(asio.mutableClosed()),
    mScheduler(mLogger, mClosed),
    mResolver(asio.mutableIoService()),
    mResolveTracker("resolver"),
    mRetryResolveTimer(NULL),
    mLastResolveErrorTime(0),
    mConnectingCount(0)
{
}

Connector::~Connector()
{
    OTS_LOG_DEBUG(mLogger)
        .what("CONN: destroying Connector.");
}

Connector::Context::Context(
    util::Logger& logger,
    util::Actor& actor,
    const Tracker& tracker,
    const Endpoint& ep)
  : mLogger(logger),
    mActor(actor),
    mTracker(tracker),
    mEndpoint(ep)
{}

Connector::Context::~Context()
{
    OTS_LOG_DEBUG(mLogger)
        .what("CONN: destroying Connector::Context.");
}

void Connector::start()
{
    mScheduler.start();
    supplyConnections();
}

void Connector::close()
{
    // wait for steadiness of all connections
    mRetryResolveTimer->cancel();
    mResolver.cancel();
    impl::countDown(mLogger, mConnectingCount,
        "CONN: wait for all being connected.",
        "CONN: all connections connected.");
    mScheduler.close();
}

void Connector::supplyConnections()
{
    int64_t idle = mScheduler.countIdle();
    int64_t busy = mScheduler.countBusy();
    int64_t connecting =
        mConnectingCount.load(boost::memory_order_acquire);
    int64_t require = mMaxConnections - (idle + busy + connecting);
    OTS_LOG_INFO(mLogger)
        ("MaxConnections", mMaxConnections)
        ("Idle", idle)
        ("Busy", busy)
        ("Connecting", connecting)
        ("RequireMore", require)
        .what("CONN: make connections.");
    int64_t x = mConnectingCount.fetch_add(require, boost::memory_order_acq_rel);
    OTS_ASSERT(x >= 0)(x);
    for(int64_t i = 0; i < require; ++i) {
        connect();
    }

    if (!mClosed.load(boost::memory_order_acquire)) {
        mRetryResolveTimer = &mAsio.startTimer(
            mResolveTracker,
            MonotonicTime::now() + Duration::fromSec(15),
            boost::ref(mAsio.arbitraryActor()),
            bind(&Connector::handleSupplyConnections, this));
    }
}

void Connector::handleSupplyConnections()
{
    supplyConnections();
}

void Connector::connect()
{
    Tracker tracker = Tracker::create();
    OTS_LOG_DEBUG(mLogger)
        ("Connection", tracker)
        .what("CONN: start connecting");
    auto_ptr<Context> ctx(
        new Context(mLogger, mAsio.selectedActor(tracker), tracker, mEndpoint));
    ctx->mCallback = bind(&Connector::connectComplete, this, ctx.get(), _1, _2);
    boost::asio::ip::tcp::resolver::query query(
        mEndpoint.mHost, mEndpoint.mPort);
    mResolver.async_resolve(query,
        bind(&Connector::handleResolve, this, ctx.release(), _1, _2));
}

ConnectionBase* Connector::newConnection()
{
    switch(mEndpoint.mProtocol) {
    case Endpoint::HTTP:
        return new ConnectionImpl<Endpoint::HTTP>(
            *this, mLogger, mAsio.mutableIoService());
    case Endpoint::HTTPS:
        return new ConnectionImpl<Endpoint::HTTPS>(
            *this, mLogger, mAsio.mutableIoService());
    }
    OTS_ASSERT(false);
    return NULL;
}

void Connector::handleResolve(
    Context* ctx,
    const boost::system::error_code& err,
    boost::asio::ip::tcp::resolver::iterator epIter)
{
    if (!err) {
        OTS_LOG_DEBUG(mLogger)
            ("Connection", ctx->mTracker)
            .what("CONN: address resolved");
        ConnectionBase* conn = newConnection();
        conn->mutableTracker() = ctx->mTracker;
        conn->asyncConnect(epIter,
            bind(&Connector::handleConnect, this, ctx, conn, _1, _2));
    } else {
        MonotonicTime now = MonotonicTime::now();
        Duration silence = Duration::fromSec(15);
        if (mLastResolveErrorTime + silence < now) {
            mLastResolveErrorTime = now;
            OTS_LOG_ERROR(mLogger)
                ("Connection", ctx->mTracker)
                ("Endpoint", ctx->mEndpoint)
                ("ErrorMessage", err.message())
                .what("CONN: fail to resolve endpoint.");
        } else {
            OTS_LOG_DEBUG(mLogger)
                ("Connection", ctx->mTracker)
                ("Endpoint", ctx->mEndpoint)
                ("ErrorMessage", err.message())
                .what("CONN: fail to resolve endpoint.");
        }
        OTSError e(OTSError::kPredefined_CouldntResoveHost);
        e.mutableMessage() = err.message();
        ctx->mActor.pushBack(
            bind(ctx->mCallback, Optional<OTSError>(e), (ConnectionBase*) NULL));
    }
}

void Connector::handleConnect(
    Context* ctx,
    ConnectionBase* conn,
    const boost::system::error_code& err,
    boost::asio::ip::tcp::resolver::iterator)
{
    if (!err) {
        OTS_LOG_DEBUG(mLogger)
            ("Connection", ctx->mTracker)
            .what("CONN: connected");
        conn->asyncHandshake(
            mEndpoint,
            bind(&Connector::handleHandshake, this, ctx, conn, _1));
    } else {
        MonotonicTime now = MonotonicTime::now();
        Duration silence = Duration::fromSec(15);
        if (mLastResolveErrorTime + silence < now) {
            mLastResolveErrorTime = now;
            OTS_LOG_ERROR(mLogger)
                ("Connection", ctx->mTracker)
                ("Endpoint", ctx->mEndpoint)
                ("ErrorMessage", err.message())
                .what("CONN: fail to connect.");
        } else {
            OTS_LOG_DEBUG(mLogger)
                ("Connection", ctx->mTracker)
                ("Endpoint", ctx->mEndpoint)
                ("ErrorMessage", err.message())
                .what("CONN: fail to connect.");
        }
        OTSError e(OTSError::kPredefined_CouldntConnect);
        e.mutableMessage() = err.message();
        ctx->mActor.pushBack(
            bind(ctx->mCallback, Optional<OTSError>(e), (ConnectionBase*) NULL));
        delete conn;
    }
}

void Connector::handleHandshake(
    Context* ctx,
    ConnectionBase* conn,
    const boost::system::error_code& err)
{
    if (!err) {
        OTS_LOG_DEBUG(mLogger)
            ("Connection", ctx->mTracker)
            .what("CONN: handshake pass");
        ctx->mActor.pushBack(bind(ctx->mCallback, Optional<OTSError>(), conn));
    } else {
        MonotonicTime now = MonotonicTime::now();
        Duration silence = Duration::fromSec(15);
        if (mLastResolveErrorTime + silence < now) {
            mLastResolveErrorTime = now;
            OTS_LOG_ERROR(mLogger)
                ("Connection", ctx->mTracker)
                ("Endpoint", ctx->mEndpoint)
                ("ErrorMessage", err.message())
                .what("CONN: fail to handshake.");
        } else {
            OTS_LOG_DEBUG(mLogger)
                ("Connection", ctx->mTracker)
                ("Endpoint", ctx->mEndpoint)
                ("ErrorMessage", err.message())
                .what("CONN: fail to handshake.");
        }
        OTSError e(OTSError::kPredefined_SslHandshakeFail);
        e.mutableMessage() = err.message();
        ctx->mActor.pushBack(
            bind(ctx->mCallback, Optional<OTSError>(e), (ConnectionBase*) NULL));
        delete conn;
    }
}

void Connector::asyncBorrowConnection(
    const Tracker& tracker,
    util::MonotonicTime deadline,
    const BorrowConnectionHandler& cb)
{
    mScheduler.asyncBorrowConnection(
        tracker,
        mAsio.selectedActor(tracker),
        deadline,
        cb);
}

void Connector::giveBack(ConnectionBase* conn)
{
    mScheduler.giveBack(conn);
}

void Connector::destroy(ConnectionBase* conn)
{
    mScheduler.destroy(conn);
}

void Connector::connectComplete(
    Context* ctx,
    const Optional<OTSError>& err,
    ConnectionBase* conn)
{
    auto_ptr<Context> ctxHolder(ctx);
    auto_ptr<ConnectionBase> connHolder(conn);
    if (err.present()) {
        OTS_LOG_DEBUG(mLogger)
            ("Connection", ctx->mTracker)
            ("Error", *err)
            .what("CONN: error on connecting.");
    } else {
        OTS_LOG_DEBUG(mLogger)
            ("Connection", ctx->mTracker)
            .what("CONN: a connection ready.");
        mScheduler.giveIdle(connHolder.release());
    }
    int64_t x = mConnectingCount.fetch_sub(1, boost::memory_order_release);
    OTS_ASSERT(x >= 1)(x);
}

Scheduler::Scheduler(
    Logger& logger,
    boost::atomic<bool>& closed)
  : mLogger(logger),
    mClosed(closed),
    mBusyCount(0),
    mSem(0)
{
}

Scheduler::~Scheduler()
{}

void Scheduler::start()
{
    Thread t(bind(&Scheduler::loop, this));
    moveAssign(mThread, util::move(t));
}

void Scheduler::close()
{
    mSem.post();
    mThread.join();

    impl::countDown(mLogger, mBusyCount,
        "CONN: wait for return of all connections.",
        "CONN: all connections returned.");
    for(;;) {
        ConnectionBase* conn = NULL;
        if (!mIdleConnections.pop(conn)) {
            break;
        }
        conn->gentlyClose();
    }

    deque<WaitForConnection*> waits;
    {
        ScopedLock lock(mWaitingListMutex);
        std::swap(waits, mWaitingList);
    }
    for(; !waits.empty(); waits.pop_front()) {
        auto_ptr<WaitForConnection> w(waits.front());
        w->close();
    }
}

void Scheduler::giveIdle(ConnectionBase* conn)
{
    bool ret = mIdleConnections.push(conn);
    OTS_ASSERT(ret);
    mSem.post();
}

void Scheduler::giveBack(ConnectionBase* conn)
{
    OTS_LOG_DEBUG(mLogger)
        ("Connection", conn->tracker())
        .what("CONN: give back a connection");
    giveIdle(conn);
    int64_t x = mBusyCount.fetch_sub(1, boost::memory_order_acq_rel);
    OTS_ASSERT(x >= 1)(x);
}

void Scheduler::destroy(ConnectionBase* conn)
{
    Tracker tracker = conn->tracker();
    OTS_LOG_DEBUG(mLogger)
        ("Connection", tracker)
        .what("CONN: destroy a connection");
    conn->gentlyClose();
    int64_t x = mBusyCount.fetch_sub(1, boost::memory_order_acq_rel);
    OTS_ASSERT(x >= 1)(x);
    OTS_LOG_DEBUG(mLogger)
        ("Connection", tracker)
        .what("CONN: a connection is destroyed");
}

void Scheduler::asyncBorrowConnection(
    const Tracker& tracker,
    util::Actor& actor,
    util::MonotonicTime deadline,
    const BorrowConnectionHandler& cb)
{
    ConnectionBase* conn = NULL;
    bool ret = mIdleConnections.pop(conn);
    if (ret) {
        OTS_LOG_DEBUG(mLogger)
            ("Request", tracker)
            ("Connection", conn->tracker())
            .what("CONN: Borrows an idle connection.");
        int64_t x = mBusyCount.fetch_add(1, boost::memory_order_acq_rel);
        OTS_ASSERT(x >= 0)(x);
        actor.pushBack(bind(cb, boost::ref(*conn), Optional<OTSError>()));
    } else {
        OTS_LOG_DEBUG(mLogger)
            ("Request", tracker)
            .what("CONN: No idle connection. Waits for connecting.");
        WaitForConnection* wait = new WaitForConnection(
            mLogger, tracker, actor, cb, deadline);
        {
            ScopedLock lock(mWaitingListMutex);
            mWaitingList.push_back(wait);
        }
        mSem.post();
    }
}

void Scheduler::loop()
{
    OTS_LOG_DEBUG(mLogger)
        .what("CONN: Scheduler::loop() starts.");
    Duration timeout(Duration::fromMsec(10));
    for(;;) {
        mSem.waitFor(timeout);
        if (mClosed.load(boost::memory_order_acquire)) {
            break;
        }
        scanWaitingList();
    }
    OTS_LOG_DEBUG(mLogger)
        .what("CONN: Scheduler::loop() is quiting.");
}

void Scheduler::scanWaitingList()
{
    deque<WaitForConnection*> waits;
    {
        ScopedLock lock(mWaitingListMutex);
        std::swap(waits, mWaitingList);
    }
    if (waits.empty()) {
        return;
    }
    OTS_LOG_DEBUG(mLogger)
        ("WaitingList", waits.size())
        .what("CONN: some requests are waiting for connections.");
    MonotonicTime now = MonotonicTime::now();
    for(; !waits.empty();) {
        auto_ptr<WaitForConnection> req(waits.front());
        waits.pop_front();

        if (req->mDeadline <= now) {
            OTS_LOG_DEBUG(mLogger)
                ("Request", req->mTracker)
                .what("CONN: Timeout while waiting for idle connections.");
            OTSError e(OTSError::kPredefined_OperationTimeout);
            e.mutableMessage() = "Timeout while waiting for idle connections.";
            req->mActor.pushBack(
                bind(req->mCallback,
                    boost::ref(*static_cast<ConnectionBase*>(NULL)),
                    Optional<OTSError>(util::move(e))));
            continue;
        }

        ConnectionBase* conn = NULL;
        if (mIdleConnections.pop(conn)) {
            OTS_LOG_DEBUG(mLogger)
                ("Request", req->mTracker)
                ("Connection", conn->tracker())
                .what("CONN: Borrows an idle connection in slow-path.");
            int64_t x = mBusyCount.fetch_add(1, boost::memory_order_acq_rel);
            OTS_ASSERT(x >= 0)(x);
            req->mActor.pushBack(
                bind(req->mCallback,
                    boost::ref(*conn),
                    Optional<OTSError>()));
        } else {
            OTS_LOG_DEBUG(mLogger)
                .what("CONN: No idle connections anymore.");
            waits.push_front(req.release());
            break;
        }
    }

    if (!waits.empty()) {
        ScopedLock lock(mWaitingListMutex);
        for(; !waits.empty(); waits.pop_back()) {
            mWaitingList.push_front(waits.back());
        }
    }
}

int64_t Scheduler::countIdle()
{
    return mIdleConnections.size();
}

int64_t Scheduler::countBusy()
{
    return mBusyCount.load(boost::memory_order_acquire);
}

} // namespace http
} // namespace core
} // namespace tablestore
} // namespace aliyun
