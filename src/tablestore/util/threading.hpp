#pragma once
#ifndef TABLESTORE_UTIL_THREADING_HPP
#define TABLESTORE_UTIL_THREADING_HPP
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
#include "tablestore/util/move.hpp"
#include "tablestore/util/timestamp.hpp"
#include "tablestore/util/prettyprint.hpp"
#include <boost/noncopyable.hpp>
#include <boost/atomic.hpp>
#include <tr1/functional>
#include <tr1/memory>
#include <memory>
#include <string>
#include <stdint.h>

namespace aliyun {
namespace tablestore {
namespace util {

namespace impl {
class Thread;
} // namespace impl

class Thread: private boost::noncopyable
{
public:
    explicit Thread(const std::tr1::function<void()>&);
    explicit Thread();
    explicit Thread(const MoveHolder<Thread>&);
    Thread& operator=(const MoveHolder<Thread>&);

    ~Thread();
    void join();

private:
    std::auto_ptr<impl::Thread> mImpl;
};

namespace impl {
class Mutex;
} // namespace impl

class Mutex: private boost::noncopyable
{
public:
    explicit Mutex();
    void lock();
    void unlock();

private:
    std::auto_ptr<impl::Mutex> mMutex;
};

class ScopedLock: private boost::noncopyable
{
public:
    explicit ScopedLock(Mutex& mutex);
    ~ScopedLock();

private:
    Mutex& mMutex;
};

namespace impl {
class Semaphore;
} // namespace impl

class Semaphore: private boost::noncopyable
{
public:
    enum Status
    {
        kInTime,
        kTimeout,
    };

    explicit Semaphore(int64_t init);

    void post();
    void wait();
    Status waitFor(Duration);

private:
    std::auto_ptr<impl::Semaphore> mImpl;
};

namespace impl {
class ActionQueue;
} // namespace impl

class Actor
{
    static const int64_t kMaxBatchSize = 1000;

public:
    typedef std::tr1::function<void()> Action;

    explicit Actor();
    ~Actor();

    void pushBack(const Action&);

private:
    void acting();

private:
    Thread mThread;
    Semaphore mSem;
    boost::atomic<bool> mStopper;
    std::auto_ptr<impl::ActionQueue> mScript;
};

} // namespace util
} // namespace tablestore
} // namespace aliyun

namespace pp {
namespace impl {

template<>
struct PrettyPrinter<aliyun::tablestore::util::Semaphore::Status, void>
{
    void operator()(std::string&, aliyun::tablestore::util::Semaphore::Status) const;
};

} // namespace impl
} // namespace pp
#endif
