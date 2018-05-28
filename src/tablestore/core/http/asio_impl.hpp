#pragma once
#ifndef TABLESTORE_CORE_HTTP_ASIO_IMPL_HPP
#define TABLESTORE_CORE_HTTP_ASIO_IMPL_HPP
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
#include "connection_impl.hpp"
#include "timer_impl.hpp"
#include "types.hpp"
#include "tablestore/core/types.hpp"
#include "tablestore/util/optional.hpp"
#include "tablestore/util/threading.hpp"
#include "tablestore/util/seq_gen.hpp"
#include <boost/atomic.hpp>
#include <boost/asio.hpp>
#include <tr1/memory>
#include <deque>
#include <stdint.h>

namespace aliyun {
namespace tablestore {
namespace core {
namespace http {

class AsioImpl: public Asio
{
public:
    explicit AsioImpl(
        util::Logger& logger,
        int64_t maxConnections,
        util::Duration connectTimeout,
        const Endpoint& ep,
        const std::deque<std::tr1::shared_ptr<util::Actor> >&);
    ~AsioImpl();

    void asyncBorrowConnection(
        const Tracker& tracker,
        util::MonotonicTime deadline,
        const BorrowConnectionHandler&);

    Timer& startTimer(
        const Tracker& tracker,
        util::MonotonicTime deadline,
        util::Actor&,
        const TimeoutCallback& toCb);
    void close();

public:
    void cancelTimer(TimerImpl&);

    boost::asio::io_service& mutableIoService()
    {
        return mIoService;
    }

    util::Logger& mutableLogger()
    {
        return mLogger;
    }

    boost::atomic<bool>& mutableClosed()
    {
        return mClosed;
    }

    util::Actor& arbitraryActor();
    util::Actor& selectedActor(const Tracker&);

private:
    void loop();

private:
    util::Logger& mLogger;
    std::deque<std::tr1::shared_ptr<util::Actor> > mActors;
    util::SequenceGenerator mSeqGen;
    boost::atomic<bool> mClosed;

    boost::asio::io_service mIoService;
    util::Thread mLoopThread;

    Connector mConnector;
    TimerCenter mTimerCenter;
};

} // namespace http
} // namespace core
} // namespace tablestore
} // namespace aliyun

#endif
