#pragma once
#ifndef TABLESTORE_CORE_HTTP_TYPES_HPP
#define TABLESTORE_CORE_HTTP_TYPES_HPP
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
#include "tablestore/core/error.hpp"
#include "tablestore/core/types.hpp"
#include "tablestore/util/mempiece.hpp"
#include "tablestore/util/timestamp.hpp"
#include "tablestore/util/optional.hpp"
#include "tablestore/util/move.hpp"
#include "tablestore/util/mempool.hpp"
#include "tablestore/util/logger.hpp"
#include "tablestore/util/threading.hpp"
#include <tr1/functional>
#include <tr1/memory>
#include <map>
#include <string>
#include <stdint.h>

namespace aliyun {
namespace tablestore {
namespace core {
namespace http {

typedef std::map<
    std::string, std::string,
    util::QuasiLexicographicLess<std::string> > Headers;
typedef std::map<
    util::MemPiece, util::MemPiece,
    util::QuasiLexicographicLess<util::MemPiece> > InplaceHeaders;
typedef std::tr1::function<
    void(const Tracker&,
        util::Optional<OTSError>& err,
        InplaceHeaders& respHeaders,
        std::deque<util::MemPiece>& respBody)
    > ResponseCallback;
typedef std::tr1::function<void()> TimeoutCallback;


struct Endpoint
{
    enum Protocol
    {
        HTTP,
        HTTPS,
    };

    explicit Endpoint();
    explicit Endpoint(const util::MoveHolder<Endpoint>&);
    Endpoint& operator=(const util::MoveHolder<Endpoint>&);

    static util::Optional<std::string> parse(Endpoint&, const std::string&);

    void prettyPrint(std::string&) const;

    Protocol mProtocol;
    std::string mHost;
    std::string mPort;
};

} // namespace http
} // namespace core
} // namespace tablestore
} // namespace aliyun

#endif
