#include "ots_static_index/thread_pool.h"
#include "ots_static_index/logger.h"
#include "threading.h"
#include "ots_memory.h"
#include "logging_assert.h"
#include "noncopyable.h"
#include <tr1/memory>
#include <deque>

using namespace ::std;
using namespace ::std::tr1;

namespace static_index {

namespace {

template<typename T>
class RingBuffer
{
    int64_t mSize;
    ScopedArray<T> mItems; // own the array, but not own any of the items
    int64_t mHead;
    int64_t mTail;

public:
    explicit RingBuffer(int64_t size)
      : mSize(size + 1),
        mItems(new T[size + 1]),
        mHead(0),
        mTail(0)
    {
    }

    bool TryPush(T w)
    {
        int64_t nxtTail = (mTail + 1) % mSize;
        if (nxtTail == mHead) {
            return false;
        }
        mItems[mTail] = w;
        mTail = nxtTail;
        return true;
    }

    bool TryPop(T* out)
    {
        if (mHead == mTail) {
            return false;
        }
        *out = mItems[mHead];
        mHead = (mHead + 1) % mSize;
        return true;
    }

    int64_t Count() const
    {
        return (mTail - mHead + mSize) % mSize;
    }
};

struct Worker
{
    UniquePtr<Thread> mThread;
    Semaphore mNotifier;
    ThreadPool::Closure mTask;

    explicit Worker(Logger* logger)
      : mNotifier(logger, 0)
    {}
};

class ThreadPoolImpl : public ThreadPool, private Noncopyable
{
    Logger* mLogger;

    deque<shared_ptr<Worker> > mWorkers;
    UniquePtr<Thread> mDispatcher;
    Semaphore mDispatcherNotifier;

    mutable Mutex mMutex;
    bool mStop;
    RingBuffer<Worker*> mWaitingList;
    RingBuffer<Closure> mToDoList;
    
public:
    ThreadPoolImpl(Logger* logger, int64_t threadSize, int64_t queueSize)
      : mLogger(logger),
        mDispatcherNotifier(logger, 0),
        mMutex(logger),
        mStop(false),
        mWaitingList(threadSize),
        mToDoList(queueSize)
    {
        for(int64_t i = 0; i < threadSize; ++i) {
            mWorkers.push_back(shared_ptr<Worker>(new Worker(logger)));
            Worker* w = mWorkers.back().get();
            w->mThread.Reset(Thread::New(logger, bind(&ThreadPoolImpl::WorkerFunc, this, w)));
            bool ret = mWaitingList.TryPush(w);
            OTS_ASSERT(mLogger, ret)(i)(mWaitingList.Count())(threadSize);
        }
        mDispatcher.Reset(Thread::New(logger, bind(&ThreadPoolImpl::Dispatch, this)));
    }
    ~ThreadPoolImpl()
    {
        __atomic_store_1(&mStop, true, __ATOMIC_RELEASE);
        mDispatcherNotifier.Post();
        mDispatcher->Join();

        for(; !mWorkers.empty(); mWorkers.pop_back()) {
            Worker* w = mWorkers.back().get();
            w->mNotifier.Post();
            w->mThread->Join();
        }
    }

    bool TryEnqueue(const Closure& c)
    {
        bool res = false;
        {
            Scoped<Mutex> g(&mMutex);
            res = mToDoList.TryPush(c);
        }
        if (res) {
            mDispatcherNotifier.Post();
        }
        return res;
    }

    int64_t CountItems() const
    {
        Scoped<Mutex> g(&mMutex);
        return mToDoList.Count();
    }

private:
    void Dispatch()
    {
        for(;;) {
            mDispatcherNotifier.Wait();
            Scoped<Mutex> g(&mMutex);
            if (mStop) {
                return;
            }
            if (mToDoList.Count() > 0 && mWaitingList.Count() > 0) {
                Worker* w = NULL;
                bool r0 = mWaitingList.TryPop(&w);
                OTS_ASSERT(mLogger, r0);
                OTS_ASSERT(mLogger, w != NULL);
                bool r1 = mToDoList.TryPop(&w->mTask);
                OTS_ASSERT(mLogger, r1);
                w->mNotifier.Post();
            }
        }
    }

    void WorkerFunc(Worker* w)
    {
        for(;;) {
            w->mNotifier.Wait();
            if (__atomic_load_1(&mStop, __ATOMIC_ACQUIRE)) {
                return;
            }
            if (w->mTask) {
                w->mTask();
            }
            {
                Scoped<Mutex> g(&mMutex);
                mWaitingList.TryPush(w);
            }
            mDispatcherNotifier.Post();
        }
    }
};

} // namespace

ThreadPool* ThreadPool::New(Logger* logger, int64_t threadSize, int64_t queueSize)
{
    return new ThreadPoolImpl(logger, threadSize, queueSize);
}

} // namespace static_index

