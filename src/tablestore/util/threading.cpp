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
#include "threading.hpp"
#include "tablestore/util/assert.hpp"
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/lockfree/queue.hpp>

using namespace std;
using namespace std::tr1;

namespace pp {
namespace impl {

typedef aliyun::tablestore::util::Semaphore Semaphore;
void PrettyPrinter<Semaphore::Status, void>::operator()(
    string& out, Semaphore::Status st) const
{
    switch(st) {
    case Semaphore::kInTime:
        out.append("Semaphore::kInTime");
        break;
    case Semaphore::kTimeout:
        out.append("Semaphore::kTimeout");
        break;
    }
}

} // namespace impl
} // namespace pp

namespace aliyun {
namespace tablestore {
namespace util {

namespace impl {

class Thread
{
public:
    explicit Thread(function<void()> fn)
      : mImpl(fn)
    {}

    ~Thread()
    {}

    void join()
    {
        mImpl.join();
    }

private:
    boost::thread mImpl;
};

} // namespace impl

Thread::Thread(const function<void()>& fn)
  : mImpl(new impl::Thread(fn))
{}

Thread::Thread()
  : mImpl(NULL)
{}

Thread::Thread(const MoveHolder<Thread>& a)
{
    *this = a;
}

Thread& Thread::operator=(const MoveHolder<Thread>& a)
{
    if (this != &*a) {
        OTS_ASSERT(mImpl == NULL)
            .what("Being move target of a thread delegate"
                " requires no thread it delegated to.");
        mImpl = a->mImpl;
        a->mImpl = NULL;
    }
    return *this;
}

Thread::~Thread()
{
    delete mImpl;
}

void Thread::join()
{
    if (mImpl != NULL) {
        mImpl->join();
        delete mImpl;
        mImpl = NULL;
    }
}

namespace impl {

class Mutex: private boost::noncopyable
{
public:
    void lock()
    {
        mMutex.lock();
    }

    void unlock()
    {
        mMutex.unlock();
    }

private:
    boost::mutex mMutex;
};

} // namespace impl

Mutex::Mutex()
  : mMutex(new impl::Mutex())
{}

Mutex::~Mutex()
{
    delete mMutex;
}

void Mutex::lock()
{
    mMutex->lock();
}

void Mutex::unlock()
{
    mMutex->unlock();
}

ScopedLock::ScopedLock(Mutex& mutex)
  : mMutex(mutex)
{
    mMutex.lock();
}

ScopedLock::~ScopedLock()
{
    mMutex.unlock();
}

namespace impl {

class Semaphore: private boost::noncopyable
{
public:
    explicit Semaphore(int64_t init)
      : mAvailable(init)
    {}

    void post()
    {
        {
            boost::unique_lock<boost::mutex> lck(mMutex);
            ++mAvailable;
        }
        mCondVar.notify_one();
    }

    void wait()
    {
        boost::unique_lock<boost::mutex> lck(mMutex);
        while(mAvailable == 0) {
            mCondVar.wait(lck);
        }
        --mAvailable;
    }

    util::Semaphore::Status waitFor(Duration d)
    {
        boost::unique_lock<boost::mutex> lck(mMutex);
        while(mAvailable == 0) {
            boost::cv_status res = mCondVar.wait_for(lck,
                boost::chrono::microseconds(d.toUsec()));
            if (res == boost::cv_status::timeout) {
                return util::Semaphore::kTimeout;
            }
        }
        --mAvailable;
        return util::Semaphore::kInTime;
    }

private:
    boost::mutex mMutex;
    boost::condition_variable mCondVar;
    int64_t mAvailable;
};

} // namespace impl

Semaphore::Semaphore(int64_t init)
  : mImpl(new impl::Semaphore(init))
{}

Semaphore::~Semaphore()
{
    delete mImpl;
}

void Semaphore::post()
{
    mImpl->post();
}

void Semaphore::wait()
{
    mImpl->wait();
}

Semaphore::Status Semaphore::waitFor(Duration d)
{
    return mImpl->waitFor(d);
}


namespace impl {

class ActionQueue
{
public:
    explicit ActionQueue()
      : mActions(0)
    {}
    ~ActionQueue();

    bool push(const Actor::Action&);
    bool pop(Actor::Action*);

private:
    struct Item
    {
        Actor::Action mAction;
    };

private:
    boost::lockfree::queue<
        Item*,
        boost::lockfree::fixed_sized<false> > mActions;
};

ActionQueue::~ActionQueue()
{
    for(;;) {
        Item* item = NULL;
        bool ret = mActions.pop(item);
        if (ret) {
            delete item;
        } else {
            break;
        }
    }
}

bool ActionQueue::push(const Actor::Action& act)
{
    Item* item = new Item();
    item->mAction = act;
    bool ret = mActions.push(item);
    if (!ret) {
        delete item;
    }
    return ret;
}

bool ActionQueue::pop(Actor::Action* act)
{
    Item* item = NULL;
    bool ret = mActions.pop(item);
    if (ret) {
        *act = item->mAction;
        delete item;
    }
    return ret;
}

} // namespace impl

Actor::Actor()
  : mSem(0),
    mStopper(false),
    mScript(new impl::ActionQueue())
{
    Thread t(bind(&Actor::acting, this));
    moveAssign(mThread, util::move(t));
}

Actor::~Actor()
{
    mStopper.store(true, boost::memory_order_release);
    mSem.post();
    mThread.join();
    delete mScript;
}

void Actor::pushBack(const Action& act)
{
    OTS_ASSERT(!mStopper.load(boost::memory_order_acquire))
        .what("Should not make a destroying actor do anything.");
    bool ret = mScript->push(act);
    OTS_ASSERT(ret);
    mSem.post();
}

void Actor::acting()
{
    deque<Action> batch;
    for(;;) {
        mSem.wait();
        if (mStopper.load(boost::memory_order_acquire)) {
            break;
        }

        batch.clear();
        for(int64_t i = 0; i < kMaxBatchSize; ++i) {
            Action act;
            bool ret = mScript->pop(&act);
            if (!ret) {
                break;
            } else {
                batch.push_back(act);
            }
        }
        for(; !batch.empty(); batch.pop_front()) {
            const Action& act = batch.front();
            act();
        }
    }
}

} // namespace util
} // namespace tablestore
} // namespace aliyun
