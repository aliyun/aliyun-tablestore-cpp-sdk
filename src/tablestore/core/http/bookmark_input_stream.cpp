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
#include "bookmark_input_stream.hpp"
#include "tablestore/core/types.hpp"
#include "tablestore/util/logging.hpp"
#include "tablestore/util/assert.hpp"

using namespace std;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {
namespace http {

BookmarkInputStream::BookmarkInputStream(const Tracker& tracker)
  : mTracker(tracker)
{}

void BookmarkInputStream::feed(const MemPiece& mem)
{
    mBuffers.push_back(mem);
}

Optional<uint8_t> BookmarkInputStream::peek()
{
    if (mCurrent.mIndex >= mBuffers.size()) {
        return Optional<uint8_t>();
    }
    OTS_ASSERT(mCurrent.mOffset < mBuffers[mCurrent.mIndex].length())
        (mTracker)
        (mCurrent.mOffset)
        (mCurrent.mIndex)
        (mBuffers[mCurrent.mIndex].length());
    return Optional<uint8_t>(mBuffers[mCurrent.mIndex].get(mCurrent.mOffset));
}

bool BookmarkInputStream::moveNext()
{
    if (mCurrent.mIndex >= mBuffers.size()) {
        return false;
    }
    OTS_ASSERT(mCurrent.mOffset < mBuffers[mCurrent.mIndex].length())
        (mTracker)
        (mCurrent.mOffset)
        (mCurrent.mIndex)
        (mBuffers[mCurrent.mIndex].length());
    inc(mCurrent);
    return mCurrent.mIndex < mBuffers.size();
}

void BookmarkInputStream::pushBookmark()
{
    mBookmarks.push_back(mCurrent);
}

void BookmarkInputStream::popBookmark(deque<MemPiece>& out)
{
    OTS_ASSERT(!mBookmarks.empty())
        (mTracker);
    Location start = mBookmarks.back();
    mBookmarks.pop_back();
    Location end = mCurrent;
    OTS_ASSERT(start.mIndex <= end.mIndex)
        (mTracker)
        (start.mIndex)
        (end.mIndex)
        (mBuffers.size());
    OTS_ASSERT(end.mIndex < mBuffers.size())
        (mTracker)
        (start.mIndex)
        (end.mIndex)
        (mBuffers.size());

    deque<MemPiece> res;
    if (start.mIndex == end.mIndex) {
        OTS_ASSERT(start.mIndex < mBuffers.size())
            (start.mIndex)
            (mBuffers.size());
        OTS_ASSERT(start.mOffset <= end.mOffset)
            (mTracker)
            (start.mOffset)
            (end.mOffset);
        OTS_ASSERT(end.mOffset < mBuffers[end.mIndex].length())
            (mTracker)
            (end.mIndex)
            (end.mOffset)
            (mBuffers[end.mIndex].length());
        res.push_back(
            mBuffers[start.mIndex].subpiece(
                start.mOffset, end.mOffset - start.mOffset + 1));
    } else {
        res.push_back(mBuffers[start.mIndex].subpiece(start.mOffset));
        for(size_t i = start.mIndex + 1; i < end.mIndex; ++i) {
            res.push_back(mBuffers[i]);
        }
        res.push_back(mBuffers[end.mIndex].subpiece(0, end.mOffset + 1));
    }

    if (res.size() <= 1) {
        out.push_back(res.front());
    } else {
        MemPiece last = res.front();
        res.pop_front();
        out.push_back(last);
        for(; !res.empty(); res.pop_front()) {
            const MemPiece& p = res.front();
            if (p.data() == last.data() + last.length()) {
                last = MemPiece(last.data(), last.length() + p.length());
                out.pop_back();
                out.push_back(last);
            } else {
                last = p;
                out.push_back(last);
            }
        }
    }
}

void BookmarkInputStream::inc(Location& loc)
{
    ++loc.mOffset;
    if (loc.mOffset == mBuffers[loc.mIndex].length()) {
        loc.mOffset = 0;
        ++loc.mIndex;
    }
}

} // namespace http
} // namespace core
} // namespace tablestore
} // namespace aliyun
