#pragma once
#ifndef TABLESTORE_CORE_HTTP_BOOKMARK_INPUT_STREAM_HPP
#define TABLESTORE_CORE_HTTP_BOOKMARK_INPUT_STREAM_HPP
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
#include "tablestore/core/types.hpp"
#include "tablestore/util/mempiece.hpp"
#include "tablestore/util/optional.hpp"
#include <deque>
#include <stdint.h>

namespace aliyun {
namespace tablestore {
namespace core {
namespace http {

class BookmarkInputStream
{
public:
    explicit BookmarkInputStream(const Tracker&);

    /**
     * Feeds a new piece of memory.
     * Thread-unsafe.
     */
    void feed(const util::MemPiece&);

    /**
     * Returns the current byte, which is initialized at the 1st position.
     * If it is pointed to the end, nothing will be returned.
     * Thread-unsafe.
     */
    util::Optional<uint8_t> peek();
    /**
     * Moves to the next position and returns true if there is one;
     * otherwise, returns false.
     * Thread-unsafe.
     */
    bool moveNext();

    /**
     * Pushs a bookmark to the current position.
     * Thread-unsafe.
     */
    void pushBookmark();
    /**
     * Pops the top bookmark with buffers between it and the current position.
     * Thread-unsafe.
     */
    void popBookmark(std::deque<util::MemPiece>&);

private:
    struct Location
    {
        std::size_t mIndex;
        int64_t mOffset;

        explicit Location()
          : mIndex(0),
            mOffset(0)
        {}
    };

    void inc(Location&);

private:
    const Tracker& mTracker;
    std::deque<util::MemPiece> mBuffers;
    std::deque<Location> mBookmarks;
    Location mCurrent;
};

} // namespace http
} // namespace core
} // namespace tablestore
} // namespace aliyun
#endif
