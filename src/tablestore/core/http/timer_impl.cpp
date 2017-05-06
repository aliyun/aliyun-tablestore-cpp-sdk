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
#include "timer_impl.hpp"
#include "tablestore/util/logging.hpp"
#include "tablestore/util/threading.hpp"
#include "tablestore/util/seq_gen.hpp"

using namespace std;
using namespace std::tr1;
using namespace std::tr1::placeholders;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {
namespace http {

namespace {

inline boost::posix_time::time_duration toBoostDuration(Duration d)
{
    return boost::posix_time::microseconds(d.toUsec());
}

inline boost::posix_time::time_duration toBoostPtime(MonotonicTime deadline)
{
    Duration dur = deadline - MonotonicTime::now();
    return toBoostDuration(dur);
}

} // namespace

TimerImpl::TimerImpl(
    Logger& logger,
    const Tracker& tracker,
    util::MonotonicTime deadline,
    Actor& actor,
    const TimeoutCallback& toCb,
    boost::asio::io_service& ios)
  : mLogger(logger),
    mActor(actor),
    mTracker(tracker),
    mTimer(ios, toBoostPtime(deadline)),
    mTimeoutCallback(toCb),
    mClosed(false)
{
    mTimer.async_wait(bind(&TimerImpl::handleTimeout, this, _1));
}

TimerImpl::~TimerImpl()
{
    OTS_LOG_DEBUG(mLogger)
        ("Tracker", mTracker)
        .what("ASIO: destroy TimerImpl.");
}

void TimerImpl::handleTimeout(
    const boost::system::error_code& ec)
{
    if (ec != boost::asio::error::operation_aborted) {
        if (mTimeoutCallback) {
            mActor.pushBack(mTimeoutCallback);
        }
    }
    mTimeoutCallback = TimeoutCallback();
    mClosed.store(true, boost::memory_order_release);
}

void TimerImpl::cancel()
{
    size_t canceled = mTimer.cancel();
    OTS_LOG_DEBUG(mLogger)
        ("Tracker", mTracker)
        ("Canceled", canceled)
        .what("Asio: cancel a timer.");
}

bool TimerImpl::closed()
{
    return mClosed.load(boost::memory_order_acquire);
}


TimerCenter::TimerCenter(
    boost::asio::io_service& ios,
    Logger& logger,
    const deque<shared_ptr<Actor> >& actors,
    SequenceGenerator& seqgen)
  : mIoService(ios),
    mLogger(logger),
    mActors(actors),
    mSeqGen(seqgen),
    mCleaner(mIoService),
    mClose(false)
{
    ticktock();
}

TimerCenter::~TimerCenter()
{
    OTS_ASSERT(mClose.load(boost::memory_order_acquire))
        .what("ASIO: destroy a TimerCenter which is unclosed.");
}

void TimerCenter::close()
{
    if (mClose.exchange(true, boost::memory_order_acq_rel)) {
        return;
    }

    size_t x = mCleaner.cancel();
    (void) x;
    cancelAll();
    for(;;) {
        int64_t remains = deleteClosed();
        if (remains == 0) {
            break;
        }
        OTS_LOG_DEBUG(mLogger)
            ("TicktockingTimer", remains)
            .what("ASIO: still timers ongoing.");
        sleepFor(Duration::fromMsec(1));
    }
    OTS_LOG_DEBUG(mLogger)
        .what("ASIO: all timers released.");
}

void TimerCenter::cancelAll()
{
    ScopedLock lock(mMutex);
    FOREACH_ITER(i, mTimers) {
        i->second->cancel();
    }
}

int64_t TimerCenter::deleteClosed()
{
    deque<Id> closed;
    ScopedLock lock(mMutex);
    FOREACH_ITER(i, mTimers) {
        TimerImpl* tm = i->second;
        if (tm->closed()) {
            closed.push_back(i->first);
            delete tm;
        }
    }
    FOREACH_ITER(i, closed) {
        mTimers.erase(*i);
    }
    return mTimers.size();
}

void TimerCenter::ticktock()
{
    const Duration kTicktock(Duration::fromSec(1));
    mCleaner.expires_from_now(toBoostDuration(kTicktock));
    mCleaner.async_wait(bind(&TimerCenter::cleaner, this, _1));
}

void TimerCenter::cleaner(const boost::system::error_code& ec)
{
    if (ec == boost::asio::error::operation_aborted) {
        OTS_LOG_DEBUG(mLogger)
            .what("Asio: cleaner of TimerCenter is canceled.");
        return;
    }
    uint64_t seq = mSeqGen.next();
    mActors[seq % mActors.size()]->pushBack(bind(&TimerCenter::realCleaner, this));
}

void TimerCenter::realCleaner()
{
    OTS_LOG_DEBUG(mLogger)
        .what("ASIO: enter TimerCenter::realCleaner().");
    deque<Id> toClean;
    MonotonicTime now = MonotonicTime::now();
    
    ScopedLock lock(mMutex);
    if (mClose.load(boost::memory_order_acquire)) {
        return;
    }
    OTS_LOG_DEBUG(mLogger)
        .what("ASIO: start TimerCenter::realCleaner().");

    FOREACH_ITER(i, mTimers) {
        const Id& id = i->first;
        if (id.mDeadline >= now) {
            break;
        }
        TimerImpl* tm = i->second;
        if (tm->closed()) {
            toClean.push_back(id);
            delete tm;
        }
    }
    FOREACH_ITER(i, toClean) {
        mTimers.erase(*i);
    }
    OTS_LOG_DEBUG(mLogger)
        ("CleanedTimers", toClean.size())
        .what("ASIO: leave TimerCenter::realCleaner().");
}

Timer& TimerCenter::startTimer(
    const Tracker& tracker,
    util::MonotonicTime deadline,
    Actor& actor,
    const TimeoutCallback& toCb)
{
    OTS_ASSERT(!mClose.load(boost::memory_order_acquire))
        (tracker)
        .what("ASIO: A closed TimerCenter do not accept requests anymore.");
    Id id(deadline, mSeqGen.next());
    TimerImpl* tm = new TimerImpl(
        mLogger, tracker, deadline, actor, toCb, mIoService);
    {
        ScopedLock lock(mMutex);
        bool r = mTimers.insert(make_pair(id, tm)).second;
        OTS_ASSERT(r)
            (tracker);
    }
    return *tm;
}

} // namespace http
} // namespace core
} // namespace tablestore
} // namespace aliyun

