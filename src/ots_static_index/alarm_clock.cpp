#include "alarm_clock.h"
#include "random.h"
#include "logging_assert.h"
#include "ots_static_index/thread_pool.h"
#include "ots_static_index/logger.h"

using namespace ::std;
using namespace ::std::tr1;

namespace static_index {

AlarmClock::AlarmClock(
    Logger* logger,
    IRandom* rnd,
    ThreadPool* tp)
  : mLogger(logger),
    mRandom(rnd),
    mThreadPool(tp),
    mStop(false),
    mBgLoopSem(logger, 0),
    mMutex(logger)
{
    mBgLoopThread.reset(
        Thread::New(mLogger, bind(&AlarmClock::BgLoop, this)));
}

AlarmClock::~AlarmClock()
{
    __atomic_store_1(&mStop, true, __ATOMIC_RELEASE);
    mBgLoopSem.Post();
    mBgLoopThread->Join();
}

AlarmClock::Handler AlarmClock::AddRelatively(
    const Interval& intv,
    const Closure& fn)
{
    return AddAbsolutely(MonotonicTime::Now(mLogger) + intv, fn);
}

AlarmClock::Handler AlarmClock::AddAbsolutely(
    const MonotonicTime& tm,
    const Closure& fn)
{
    Handler hdl(tm);
    Scoped<Mutex> g(&mMutex);
    _AddAbsWithoutLock(hdl, fn);
    mBgLoopSem.Post();
    return hdl;
}

void AlarmClock::BgLoop()
{
    Interval dur = Interval::FromSec(mLogger, 1);
    for(;;) {
        mBgLoopSem.Wait(dur);
        if (__atomic_load_1(&mStop, __ATOMIC_ACQUIRE)) {
            return;
        }

        dur = Interval::FromSec(mLogger, 1);
        Scoped<Mutex> g(&mMutex);
        MonotonicTime now = MonotonicTime::Now(mLogger);
        int64_t ongoing = 0;
        int64_t delayed = 0;
        for(; !mWaitings.empty(); ) {
            map<Handler, Closure>::iterator it = mWaitings.begin();
            const Handler& hdl = it->first;
            if (hdl.mDeadline > now) {
                dur = hdl.mDeadline - now;
                break;
            } else {
                if (mThreadPool->TryEnqueue(it->second)) {
                    ++ongoing;
                } else {
                    Interval nxt = Interval::FromUsec(mLogger, NextInt(mRandom, 100, 1000));
                    Handler newHdl(now + nxt);
                    _AddAbsWithoutLock(newHdl, it->second);
                    ++delayed;
                }
                mWaitings.erase(it);
            }
        }
        if (ongoing > 0) {
            OTS_LOG_DEBUG(mLogger)(ongoing).What("Alarms triggered.");
        }
        if (delayed > 0) {
            OTS_LOG_ERROR(mLogger)
                (delayed)
                (mThreadPool->CountItems())
                .What("The thread pool is busy. Is it too small?");
        }
    }
    OTS_LOG_INFO(mLogger).What("AlarmClock intends to quit.");
}

void AlarmClock::_AddAbsWithoutLock(
    Handler& hdl,
    const Closure& fn)
{
    for(;;) {
        hdl.mRandomSalt = NextInt(mRandom, mRandom->Max());
        bool ok = mWaitings.insert(make_pair(hdl, fn)).second;
        if (ok) {
            break;
        }
    }
}

bool AlarmClock::Erase(const Handler& hdl)
{
    Scoped<Mutex> g(&mMutex);
    return mWaitings.erase(hdl) > 0;
}

} // namespace static_index
