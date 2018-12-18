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
#include "asio_impl.hpp"
#include "tablestore/util/logging.hpp"

using namespace std;
using namespace std::tr1;
using namespace std::tr1::placeholders;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {
namespace http {

AsioImpl::AsioImpl(
    Logger& logger,
    Random& rng,
    int64_t maxConnections,
    Duration connectTimeout,
    const Endpoint& ep,
    const deque<shared_ptr<Actor> >& actors)
  : mLogger(logger),
    mActors(actors),
    mClosed(false),
    mConnector(*this, rng, ep, maxConnections),
    mTimerCenter(mIoService, mLogger, mActors, mSeqGen)
{
    Thread t(bind(&AsioImpl::loop, this));
    moveAssign(mLoopThread, util::move(t));

    mConnector.start();
}

AsioImpl::~AsioImpl()
{
    OTS_LOG_DEBUG(mLogger)
        .what("ASIO: destroying AsioImpl.");
    OTS_ASSERT(mClosed.load(boost::memory_order_acquire))
        .what("ASIO: destroying an unclosed Asio.");
}

void AsioImpl::close()
{
    if (mClosed.exchange(true, boost::memory_order_acq_rel)) {
        OTS_LOG_DEBUG(mLogger)
            .what("ASIO: closes a closed Asio.");
        return;
    }

    OTS_LOG_INFO(mLogger)
        .what("ASIO: close Asio.");
    mConnector.close();
    mTimerCenter.close();

    // stop the event loop
    mIoService.stop();
    mLoopThread.join();
}

void AsioImpl::loop()
{
    boost::asio::io_service::work work(mIoService);
    size_t served = mIoService.run();
    OTS_LOG_INFO(mLogger)
        ("TotalServed", served)
        .what("event loop is quitting.");
}

void AsioImpl::asyncBorrowConnection(
    const Tracker& tracker,
    util::MonotonicTime deadline,
    const BorrowConnectionHandler& cb)
{
    OTS_ASSERT(!mClosed.load(boost::memory_order_acquire))
        (tracker)
        .what("A closing Asio do never accept any requests.");

    mConnector.asyncBorrowConnection(tracker, deadline, cb);
}

Timer& AsioImpl::startTimer(
    const Tracker& tracker,
    util::MonotonicTime deadline,
    Actor& actor,
    const TimeoutCallback& toCb)
{
    OTS_ASSERT(!mClosed.load(boost::memory_order_acquire))
        (tracker)
        .what("A closing Asio do never accept any requests.");

    return mTimerCenter.startTimer(tracker, deadline, actor, toCb);
}

Actor& AsioImpl::arbitraryActor()
{
    uint64_t idx = mSeqGen.next();
    return *mActors[idx % mActors.size()];
}

Actor& AsioImpl::selectedActor(const Tracker& tracker)
{
    uint64_t hash = tracker.traceHash();
    return *mActors[hash % mActors.size()];
}

} // namespace http
} // namespace core
} // namespace tablestore
} // namespace aliyun
