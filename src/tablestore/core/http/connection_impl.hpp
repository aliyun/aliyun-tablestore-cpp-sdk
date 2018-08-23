#pragma once
#ifndef TABLESTORE_CORE_HTTP_CONNECTION_IMPL_HPP
#define TABLESTORE_CORE_HTTP_CONNECTION_IMPL_HPP
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
#include "asio.hpp"
#include "tablestore/core/types.hpp"
#include "tablestore/util/threading.hpp"
#include "tablestore/util/mempiece.hpp"
#include "tablestore/util/optional.hpp"
#include <boost/lockfree/queue.hpp>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/atomic.hpp>
#include <tr1/memory>
#include <deque>

namespace aliyun {
namespace tablestore {
namespace core {
namespace http {
class AsioImpl;
class Connector;

class ConnectionBase: public Connection
{
public:
    explicit ConnectionBase(
        Connector&,
        util::Logger& logger);
    ~ConnectionBase();

    void done();
    void destroy();

    const Tracker& tracker() const
    {
        return mTracker;
    }

    Tracker& mutableTracker()
    {
        return mTracker;
    }

    virtual void asyncConnect(
        boost::asio::ip::tcp::resolver::iterator,
        const std::tr1::function<
            void(const boost::system::error_code&,
              boost::asio::ip::tcp::resolver::iterator)>&) =0;
    virtual void gentlyClose() =0;
    virtual void asyncHandshake(
        const Endpoint&,
        const std::tr1::function<void(const boost::system::error_code&)>&) =0;

protected:
    struct RequestContext
    {
        util::Logger& mLogger;
        Tracker mTracker;
        util::Actor& mActor;
        RequestCompletionHandler mRequestCompletion;
        ResponseHandler mResponseCompletion;
        util::MutableMemPiece mLastResponseBuffer;

        explicit RequestContext(
            util::Logger& logger,
            const Tracker& tracker,
            util::Actor& actor,
            const RequestCompletionHandler& reqComp,
            const ResponseHandler& respComp)
          : mLogger(logger),
            mTracker(tracker),
            mActor(actor),
            mRequestCompletion(reqComp),
            mResponseCompletion(respComp)
        {}
        ~RequestContext();
    };

protected:
    static void handleResponse(
        const boost::system::error_code&,
        size_t);

protected:
    Connector& mConnector;
    util::Logger& mLogger;
    Tracker mTracker;
};

template<class T>
class LockFreeQueue
{
public:
    explicit LockFreeQueue()
      : mQueue(0),
        mSize(0)
    {}

    bool push(T* x)
    {
        bool ret = mQueue.push(x);
        if (ret) {
            int64_t x = mSize.fetch_add(1, boost::memory_order_acq_rel);
            OTS_ASSERT(x >= 0)(x);
        }
        return ret;
    }

    bool pop(T*& out)
    {
        bool ret = mQueue.pop(out);
        if (ret) {
            int64_t x = mSize.fetch_sub(1, boost::memory_order_acq_rel);
            OTS_ASSERT(x >= 1)(x);
        }
        return ret;
    }

    int64_t size()
    {
        return mSize.load(boost::memory_order_acquire);
    }

private:
    boost::lockfree::queue<T*, boost::lockfree::fixed_sized<false> > mQueue;
    boost::atomic<int64_t> mSize;
};

class Scheduler
{
public:
    typedef Asio::BorrowConnectionHandler BorrowConnectionHandler;

    explicit Scheduler(
        util::Logger&,
        boost::atomic<bool>& closed);
    ~Scheduler();

    void start();
    void close();
    void giveIdle(ConnectionBase*);
    void giveBack(ConnectionBase*);
    void destroy(ConnectionBase*);
    void asyncBorrowConnection(
        const Tracker&,
        util::Actor&,
        util::MonotonicTime deadline,
        const BorrowConnectionHandler&);
    int64_t countIdle();
    int64_t countBusy();

private:
    struct WaitForConnection
    {
        util::Logger& mLogger;
        Tracker mTracker;
        util::Actor& mActor;
        BorrowConnectionHandler mCallback;
        util::MonotonicTime mDeadline;

        explicit WaitForConnection(
            util::Logger& logger,
            const Tracker& tracker,
            util::Actor& actor,
            const BorrowConnectionHandler& cb,
            util::MonotonicTime deadline)
          : mLogger(logger),
            mTracker(tracker),
            mActor(actor),
            mCallback(cb),
            mDeadline(deadline)
        {}
        ~WaitForConnection();

        void close();
    };

private:
    void loop();
    void scanWaitingList();

private:
    util::Logger& mLogger;
    boost::atomic<bool>& mClosed;
    LockFreeQueue<ConnectionBase> mIdleConnections;
    boost::atomic<int64_t> mBusyCount;
    util::Thread mThread;
    util::Semaphore mSem;
    util::Mutex mWaitingListMutex;
    std::deque<WaitForConnection*> mWaitingList;
};

class Connector
{
public:
    typedef Asio::BorrowConnectionHandler BorrowConnectionHandler;

    explicit Connector(
        AsioImpl&,
        util::Random&,
        const Endpoint&,
        int64_t maxConnections);
    ~Connector();

    void start();
    void close();
    void asyncBorrowConnection(
        const Tracker&,
        util::MonotonicTime deadline,
        const BorrowConnectionHandler&);
    void giveBack(ConnectionBase*);
    void destroy(ConnectionBase*);

private:
    struct Context
    {
        util::Logger& mLogger;
        util::Actor& mActor;
        const Tracker mTracker;
        std::tr1::function<void(const util::Optional<OTSError>&, ConnectionBase*)> mCallback;
        Endpoint mEndpoint;

        explicit Context(
            util::Logger&,
            util::Actor&,
            const Tracker&,
            const Endpoint&);
        ~Context();
    };

private:
    ConnectionBase* newConnection();
    void connect();
    void handleResolve(
        Context*,
        const boost::system::error_code&,
        boost::asio::ip::tcp::resolver::iterator);
    void handleConnect(
        Context*,
        ConnectionBase*,
        const boost::system::error_code&,
        boost::asio::ip::tcp::resolver::iterator);
    void handleHandshake(
        Context*,
        ConnectionBase*,
        const boost::system::error_code&);
    void connectComplete(
        Context*,
        const util::Optional<OTSError>&,
        ConnectionBase*);
    void supplyConnections();
    void handleSupplyConnections();

private:
    util::Logger& mLogger;
    AsioImpl& mAsio;
    util::Random& mRng;
    const Endpoint mEndpoint;
    const int64_t mMaxConnections;
    boost::atomic<bool>& mClosed;

    Scheduler mScheduler;
    boost::asio::ip::tcp::resolver mResolver;
    Tracker mResolveTracker;
    Timer* mRetryResolveTimer;
    util::MonotonicTime mLastResolveErrorTime;
    boost::atomic<int64_t> mConnectingCount;
};

} // namespace http
} // namespace core
} // namespace tablestore
} // namespace aliyun

#endif
