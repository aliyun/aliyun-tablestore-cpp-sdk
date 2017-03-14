#include "threading.h"
#include "timestamp.h"
#include "random.h"
#include "logging_assert.h"
#include "foreach.h"
#include "ots_static_index/static_index.h"
#include <memory>
#include <cassert>
#include <cerrno>
#include <cstring>

using namespace ::std;
using namespace ::std::tr1;

namespace static_index {

namespace {

struct ThreadWrapper
{
    Thread::Function mFunc;
};

} // namespace

Thread::Thread(Logger* logger, const Function& func)
  : mLogger(logger),
    mTid(0)
{
    auto_ptr<ThreadWrapper> p(new ThreadWrapper);
    p->mFunc = func;
    int ret = pthread_create(&mCThread, NULL, &Thread::Runner, p.release());
    OTS_ASSERT(mLogger, ret == 0)(ret)(errno)(strerror(errno));
    mTid = mCThread;
}

Thread* Thread::New(Logger* logger, const Function& func)
{
    auto_ptr<Thread> t(new Thread(logger, func));
    return t.release();
}

void Thread::Join()
{
    int ret = pthread_join(mCThread, NULL);
    OTS_ASSERT(mLogger, ret == 0)(errno)(strerror(errno));
}

uint64_t Thread::GetId() const
{
    return mTid;
}

void* Thread::Runner(void* x)
{
    auto_ptr<ThreadWrapper> t(static_cast<ThreadWrapper*>(x));
    t->mFunc();
    return NULL;
}

Semaphore::Semaphore(Logger* logger, int64_t init)
  : mLogger(logger)
{
    OTS_ASSERT(mLogger, init >= 0)(init);
    unsigned int x = init;
    OTS_ASSERT(mLogger, static_cast<int64_t>(x) == init)(init)(x);
    int ret = sem_init(&mSem, 0, x);
    OTS_ASSERT(mLogger, ret == 0)(ret)(strerror(errno));
}

Semaphore::~Semaphore()
{
    int ret = sem_destroy(&mSem);
    OTS_ASSERT(mLogger, ret == 0)(ret)(strerror(errno));
}

void Semaphore::Post()
{
    int ret = sem_post(&mSem);
    OTS_ASSERT(mLogger, ret == 0)(ret)(strerror(errno));
}

void Semaphore::Wait()
{
    int ret = -1;
    do {
        ret = sem_wait(&mSem);
    } while (ret != 0 && errno == EINTR);
    OTS_ASSERT(mLogger, ret == 0)(ret)(strerror(errno));
}

bool Semaphore::TryWait()
{
    int32_t ret = -1;
    do {
        ret = sem_trywait(&mSem);
    } while (ret != 0 && errno == EINTR);
    if (ret == 0) {
        return true;
    } else {
        OTS_ASSERT(mLogger, errno == EAGAIN)(ret)(strerror(errno));
        return false;
    }
}

namespace {

void Convert(timespec* to, const UtcTime& from)
{
    int64_t ns = from.ToIntInUsec() * kNsecPerUsec;
    to->tv_sec = ns / kNsecPerSec;
    to->tv_nsec = ns % kNsecPerSec;
}

} // namespace

bool Semaphore::Wait(const Interval& interval)
{
    UtcTime abstime = UtcTime::Now(mLogger) + interval;
    timespec ts;
    Convert(&ts, abstime);

    int ret = -1;
    do {
        ret = sem_timedwait(&mSem, &ts);
    } while (ret != 0 && errno == EINTR);

    if (ret == 0) {
        return true;
    } else {
        OTS_ASSERT(mLogger, errno == ETIMEDOUT)(ret)(strerror(errno));
        return false;
    }
}

int64_t Semaphore::InspectValue()
{
    int value;
    int ret = sem_getvalue(&mSem, &value);
    OTS_ASSERT(mLogger, ret == 0)(ret)(strerror(errno));
    return value;

}

Mutex::Mutex(Logger* logger)
  : mLogger(logger)
{
    assert(logger != NULL);
    pthread_mutexattr_t attr;
    int ret = pthread_mutexattr_init(&attr);
    OTS_ASSERT(mLogger, ret == 0)(errno)(strerror(errno));
    ret = pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK);
    OTS_ASSERT(mLogger, ret == 0)(errno)(strerror(errno));
    ret = pthread_mutex_init(&mMutex, &attr);
    OTS_ASSERT(mLogger, ret == 0)(errno)(strerror(errno));
    ret = pthread_mutexattr_destroy(&attr);
    OTS_ASSERT(mLogger, ret == 0)(errno)(strerror(errno));
}

Mutex::~Mutex()
{
    int ret = pthread_mutex_destroy(&mMutex);
    OTS_ASSERT(mLogger, ret == 0)(errno)(strerror(errno));
}

void Mutex::Lock()
{
    int ret = pthread_mutex_lock(&mMutex);
    OTS_ASSERT(mLogger, ret == 0)(errno)(strerror(errno));
}

bool Mutex::TryLock()
{
    int ret = pthread_mutex_trylock(&mMutex);
    OTS_ASSERT(mLogger, ret == 0 || ret == EBUSY)(errno)(strerror(errno));
    return ret == 0;
}

void Mutex::Unlock()
{
    int ret = pthread_mutex_unlock(&mMutex);
    OTS_ASSERT(mLogger, ret == 0)(errno)(strerror(errno));
}

void Enqueue(
    Logger* logger,
    ThreadPool* tpool,
    IRandom* rnd,
    const function<void()>& func)
{
    for(;;) {
        if (tpool->TryEnqueue(func)) {
            return;
        }
        OTS_LOG_ERROR(logger)
            (tpool->CountItems())
            .What("The thread pool is busy. Is it too small?");
        Interval dur = Interval::FromUsec(logger, NextInt(rnd, 100, 1000));
        SleepFor(dur);
    }
}

void RemoveDone(deque<Future*>* waiting, deque<Future*>* done)
{
    deque<Future*> undone;
    FOREACH_ITER(i, *waiting) {
        Future* f = *i;
        bool r = f->TryWait();
        if (r) {
            done->push_back(f);
        } else {
            undone.push_back(f);
        }
    }
    std::swap(undone, *waiting);
}

} // namespace static_index

