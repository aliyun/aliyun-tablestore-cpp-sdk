#pragma once
#ifndef TABLESTORE_CORE_HTTP_ASIO_HPP
#define TABLESTORE_CORE_HTTP_ASIO_HPP
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
#include "types.hpp"
#include "tablestore/core/error.hpp"
#include "tablestore/util/mempiece.hpp"
#include "tablestore/util/timestamp.hpp"
#include "tablestore/util/optional.hpp"
#include <tr1/functional>
#include <tr1/memory>
#include <deque>
#include <stdint.h>

namespace aliyun {
namespace tablestore {

namespace util {
class Logger;
class Actor;
} // namespace util

namespace core {
class Tracker;

namespace http {

class Endpoint;

class Connection
{
public:
    /**
     * Triggered when a request is completely written into the underlying
     * socket or an error occurs during writing.
     */
    typedef std::tr1::function<
        util::MutableMemPiece(
            const util::Optional<OTSError>&)
        > RequestCompletionHandler;
    /**
     * Triggered when a piece of response comes in or an error occurs.
     */
    typedef std::tr1::function<
        bool( // true means continue
            int64_t readBytes,
            const util::Optional<OTSError>&,
            // nonempty means "continue with a new piece of memory",
            // empty means "go ahead with the remaining part of the last piece of memory".
            util::MutableMemPiece*)
        > ResponseHandler;

    virtual ~Connection() {}

    /**
     * Sends and receives over the connection.
     * All callbacks will be executed by the given actor.
     */
    virtual void asyncIssue(
        const Tracker& tracker,
        std::deque<util::MemPiece>& req,
        util::Actor&,
        const RequestCompletionHandler&,
        const ResponseHandler&) =0;
    /**
     * Reports a request-response is completed.
     */
    virtual void done() =0;

    /**
     * Make the Asio destroy the connection,
     * usually because there is an error during sending/receiving
     * request/response.
     */
    virtual void destroy() =0;
};

class Timer
{
public:
    virtual ~Timer() {}

    /**
     * Cancels a timer and invokes the callback immediately.
     */
    virtual void cancel() =0;
};

class Asio
{
public:
    typedef std::tr1::function<
        void(Connection&,
            const util::Optional<OTSError>&)
        > BorrowConnectionHandler;

    static Asio* create(
        util::Logger&,
        int64_t maxConnections,
        util::Duration mConnectTimeout,
        const Endpoint&,
        const std::deque<std::tr1::shared_ptr<util::Actor> >&);

    /**
     * It is usually dangerous to destroy an unclosed Asio.
     */
    virtual ~Asio() {}

    /**
     * Borrows asynchronously a stand-by connection.
     * The callback will be executed by the given actor.
     * When it is executed, the ownership of the connection is transfered.
     */
    virtual void asyncBorrowConnection(
        const Tracker& tracker,
        util::MonotonicTime deadline,
        const BorrowConnectionHandler&) =0;
    /**
     * Starts a timer which will be triggered on @p deadline reached.
     * The callback will be executed by the given actor.
     */
    virtual Timer& startTimer(
        const Tracker& tracker,
        util::MonotonicTime deadline,
        util::Actor&,
        const TimeoutCallback& toCb) =0;

    /**
     * Closes the Asio gracefully and synchronously.
     * It is acceptable to close a closed Asio.
     */
    virtual void close() =0;
};

} // namespace http
} // namespace core
} // namespace tablestore
} // namespace aliyun

#endif
