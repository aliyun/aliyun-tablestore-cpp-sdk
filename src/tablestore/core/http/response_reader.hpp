#pragma once
#ifndef TABLESTORE_CORE_HTTP_RESPONSE_READER_HPP
#define TABLESTORE_CORE_HTTP_RESPONSE_READER_HPP
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
#include "bookmark_input_stream.hpp"
#include "tablestore/core/error.hpp"
#include "tablestore/util/mempiece.hpp"
#include "tablestore/util/optional.hpp"
#include <memory>
#include <deque>
#include <stdint.h>

namespace aliyun {
namespace tablestore {
namespace util {
class Logger;
} // namespace util

namespace core {
class Tracker;

namespace http {

namespace impl {
class ResponseParserState;
} // namespace impl

class ResponseReader
{
public:
    enum RequireMore
    {
        REQUIRE_MORE,
        STOP,
    };

    explicit ResponseReader(util::Logger&, const Tracker&);
    ~ResponseReader();

    /**
     * Feeds and parses another piece of memory.
     * If returns true, more pieces are required to complete the response;
     * if returns false, the last piece of the response is fed;
     * if any error occurs, returns it.
     */
    util::Optional<OTSError> feed(RequireMore&, const util::MemPiece&);

    std::deque<util::MemPiece>& mutableBody()
    {
        return mBody;
    }

    InplaceHeaders& mutableHeaders()
    {
        return mHeaders;
    }

    int64_t httpStatus() const
    {
        return mHttpStatus;
    }

    int64_t& mutableHttpStatus()
    {
        return mHttpStatus;
    }

private:
    util::Optional<OTSError> parse(RequireMore&);

private:
    util::Logger& mLogger;
    const Tracker& mTracker;
    BookmarkInputStream mInputStream;
    std::deque<impl::ResponseParserState*> mStates; // own
    impl::ResponseParserState* mState; // not own

    int64_t mHttpStatus;
    InplaceHeaders mHeaders;
    std::deque<util::MemPiece> mBody;
};

} // namespace http
} // namespace core
} // namespace tablestore
} // namespace aliyun
#endif
