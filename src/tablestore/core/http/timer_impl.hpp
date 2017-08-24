#pragma once
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
#include "tablestore/util/timestamp.hpp"
#include "tablestore/util/threading.hpp"
#include <boost/asio.hpp>
#include <boost/atomic.hpp>
#include <tr1/memory>
#include <deque>
#include <map>
#include <stdint.h>

namespace aliyun {
namespace tablestore {
namespace util {
class Logger;
class Actor;
class SequenceGenerator;
} // namespace util

namespace core {
namespace http {

class TimerImpl: public Timer
{
public:
    explicit TimerImpl(
        util::Logger&,
        const Tracker& tracker,
        util::MonotonicTime deadline,
        util::Actor&,
        const TimeoutCallback& toCb,
        boost::asio::io_service&);
    ~TimerImpl();

    void cancel();
    bool closed();

private:
    void handleTimeout(const boost::system::error_code&);
    
private:
    util::Logger& mLogger;
    util::Actor& mActor;
    Tracker mTracker;
    boost::asio::deadline_timer mTimer;
    TimeoutCallback mTimeoutCallback;
    boost::atomic<bool> mClosed;
};

class TimerCenter
{
public:
    explicit TimerCenter(
        boost::asio::io_service&,
        util::Logger&,
        const std::deque<std::tr1::shared_ptr<util::Actor> >&,
        util::SequenceGenerator&);
    ~TimerCenter();

    Timer& startTimer(
        const Tracker& tracker,
        util::MonotonicTime deadline,
        util::Actor&,
        const TimeoutCallback& toCb);
    void close();

private:
    struct Id
    {
        const util::MonotonicTime mDeadline;
        const int64_t mSeq;

        explicit Id(util::MonotonicTime deadline, int64_t seq)
          : mDeadline(deadline),
            mSeq(seq)
        {}

        bool operator<(const Id& a) const
        {
            if (mDeadline < a.mDeadline) {
                return true;
            } else if (mDeadline > a.mDeadline) {
                return false;
            } else {
                return mSeq < a.mSeq;
            }
        }
    };
    
private:
    void ticktock();
    void cleaner(const boost::system::error_code&);
    void realCleaner();
    void cancelAll();
    int64_t deleteClosed();

private:
    boost::asio::io_service& mIoService;
    util::Logger& mLogger;
    const std::deque<std::tr1::shared_ptr<util::Actor> >& mActors;
    util::SequenceGenerator& mSeqGen;

    boost::asio::deadline_timer mCleaner;
    util::Mutex mMutex;
    std::map<Id, TimerImpl*> mTimers;
    boost::atomic<bool> mClose;
};

} // namespace http
} // namespace core
} // namespace tablestore
} // namespace aliyun

