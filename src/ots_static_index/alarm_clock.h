#ifndef OTS_STATIC_INDEX_ALARM_CLOCK_H
#define OTS_STATIC_INDEX_ALARM_CLOCK_H

#include "timestamp.h"
#include "threading.h"
#include "noncopyable.h"
#include <tr1/functional>
#include <tr1/memory>
#include <memory>
#include <map>
#include <stdint.h>

namespace static_index {
class Logger;
class IRandom;
class ThreadPool;

class AlarmClock: private Noncopyable
{
public:
    typedef ::std::tr1::function<void()> Closure;
    
    struct Handler
    {
    private:
        MonotonicTime mDeadline;
        uint64_t mRandomSalt;

    public:
        explicit Handler(const MonotonicTime& tm)
          : mDeadline(tm),
            mRandomSalt(0)
        {}

        bool operator==(const Handler& ano) const
        {
            return mDeadline == ano.mDeadline
                && mRandomSalt == ano.mRandomSalt;
        }

        bool operator!=(const Handler& ano) const
        {
            return !(*this == ano);
        }

        bool operator<(const Handler& ano) const
        {
            if (mDeadline < ano.mDeadline) {
                return true;
            } else if (mDeadline > ano.mDeadline) {
                return false;
            } else {
                return mRandomSalt < ano.mRandomSalt;
            }
        }

        friend class AlarmClock;
    };

private:
    Logger* mLogger;
    IRandom* mRandom;
    ThreadPool* mThreadPool;
    bool mStop;
    Semaphore mBgLoopSem;
    ::std::auto_ptr<Thread> mBgLoopThread;
    Mutex mMutex;
    ::std::map<Handler, Closure> mWaitings;

public:
    explicit AlarmClock(Logger*, IRandom*, ThreadPool*);
    ~AlarmClock();

    Handler AddAbsolutely(const MonotonicTime&, const Closure&);
    Handler AddRelatively(const Interval&, const Closure&);
    bool Erase(const Handler&);

private:
    void BgLoop();
    void _AddAbsWithoutLock(Handler&, const Closure&);
};

} // namespace static_index

#endif /* OTS_STATIC_INDEX_ALARM_CLOCK_H */
