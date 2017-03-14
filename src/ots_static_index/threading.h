#ifndef OTS_STATIC_INDEX_THREADING_H
#define OTS_STATIC_INDEX_THREADING_H

#include "noncopyable.h"
#include <tr1/functional>
#include <tr1/memory>
#include <deque>
#include <stdint.h>
extern "C" {
#include <semaphore.h>
#include <pthread.h>
}

namespace static_index {

class Logger;
class Interval;

class Future
{
public:
    virtual ~Future() {}

    virtual void Wait() =0;
    virtual bool TryWait() =0;
    virtual bool Wait(const Interval&) =0;
};

void RemoveDone(::std::deque<Future*>* waiting, ::std::deque<Future*>* done);

class Semaphore : public Future, private Noncopyable
{
    Logger* mLogger;
    sem_t mSem;

public:
    explicit Semaphore(Logger*, int64_t init);
    ~Semaphore();

    void Post();
    void Wait();
    bool TryWait();
    bool Wait(const Interval&);
    int64_t InspectValue();
};

class Mutex : private Noncopyable
{
    Logger* mLogger;
    pthread_mutex_t mMutex;

public:
    explicit Mutex(Logger*);
    ~Mutex();

    void Lock();
    bool TryLock();
    void Unlock();
};

template<typename Mutex>
class Scoped : private Noncopyable
{
    Mutex* mMutex;

public:
    explicit Scoped(Mutex* mutex)
      : mMutex(mutex)
    {
        mutex->Lock();
    }
    ~Scoped()
    {
        mMutex->Unlock();
    }
};

/**
 * Delegate to a physical thread.
 *
 * Users must keep these objects alive during lifetime of their delegated threads.
 */
class Thread : private Noncopyable
{
public:
    typedef ::std::tr1::function<void()> Function;

private:
    Logger* mLogger;
    pthread_t mCThread;
    uint64_t mTid;

public:
    /**
     * Start a new physical thread to run @p func.
     *
     * Caveats:
     * - If it fails to start a new physical thread, the program will abort;
     */
    static Thread* New(Logger*, const Function& func);
    
    uint64_t GetId() const;
    void Join();

private:
    explicit Thread(Logger*, const Function& func);

    static void* Runner(void*);
};

class ThreadPool;
class IRandom;
void Enqueue(Logger*, ThreadPool*, IRandom*, const ::std::tr1::function<void()>&);

} // namespace static_index

#endif /* OTS_STATIC_INDEX_THREADING_H */
