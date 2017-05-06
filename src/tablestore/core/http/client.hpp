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
#include "types.hpp"
#include "tablestore/util/mempiece.hpp"
#include "tablestore/util/timestamp.hpp"
#include "tablestore/util/optional.hpp"
#include <tr1/functional>
#include <tr1/memory>
#include <deque>
#include <map>
#include <string>
#include <stdint.h>

namespace aliyun {
namespace tablestore {

namespace util {
class MemPool;
class Logger;
class Actor;
} // namespace util

namespace core {
class Tracker;

namespace http {
class Asio;

class Client
{
public:
    static Client* create(
        util::Logger&,
        util::MemPool&,
        const http::Endpoint&,
        const Headers& fixedHeaders,
        Asio&,
        const std::deque<std::tr1::shared_ptr<util::Actor> >&);

    virtual ~Client() {}

    /**
     * Issues a HTTP request asynchronously.
     *
     * @p respCb will be triggered as soon as
     * 1. Response is completely and correctly received.
     *    In this case, HTTP status except 2xx will be filled in the error
     *    field of the callback.
     * 2. Error occurs in sending request or receiving response.
     *    Then, NoConnectionAvailable, CouldntResolveHost, CouldntConnect, 
     *    WriteRequestFail, or CorruptedResponse will be filled in the error
     *    field of the callback.
     * 3. Timeout. In this case, OperationTimeout/OTSRequestTimeout will be
     *    filled in the error field of the callback.
     * In the later two cases, the underlying connection will be shutdown.
     */
    virtual void issue(
        const Tracker&,
        const ResponseCallback& respCb,
        util::MonotonicTime deadline,
        const std::string& path,
        InplaceHeaders& additionalHeaders,
        std::deque<util::MemPiece>& body) =0;

    /**
     * Closes the client gracefully and synchronously.
     * It is acceptable to close a closed Asio.
     */
    virtual void close() =0;
};

} // namespace http
} // namespace core
} // namespace tablestore
} // namespace aliyun

