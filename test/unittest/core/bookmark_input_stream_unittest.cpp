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
#include "src/tablestore/core/http/bookmark_input_stream.hpp"
#include "tablestore/core/types.hpp"
#include "tablestore/util/logger.hpp"
#include "tablestore/util/foreach.hpp"
#include "tablestore/util/prettyprint.hpp"
#include "testa/testa.hpp"
#include <string>
#include <memory>

using namespace std;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {

void BookmarkInputStream_empty(const string&)
{
    Tracker tracker("track");
    http::BookmarkInputStream is(tracker);
    TESTA_ASSERT(!is.peek().present()).issue();
    TESTA_ASSERT(!is.moveNext()).issue();
}
TESTA_DEF_JUNIT_LIKE1(BookmarkInputStream_empty);

void BookmarkInputStream_traverse(const string&)
{
    Tracker tracker("track");
    http::BookmarkInputStream is(tracker);

    TESTA_ASSERT(!is.peek().present()).issue();
    TESTA_ASSERT(!is.moveNext()).issue();

    string buf0("a");
    is.feed(MemPiece::from(buf0));
    string buf1("b");
    is.feed(MemPiece::from(buf1));
    {
        Optional<uint8_t> c = is.peek();
        TESTA_ASSERT(c.present()).issue();
        TESTA_ASSERT(*c == 'a')(*c).issue();
        bool ret = is.moveNext();
        TESTA_ASSERT(ret).issue();
    }
    {
        Optional<uint8_t> c = is.peek();
        TESTA_ASSERT(c.present()).issue();
        TESTA_ASSERT(*c == 'b')(*c).issue();
        bool ret = is.moveNext();
        TESTA_ASSERT(!ret).issue();
    }
    string buf2("c");
    is.feed(MemPiece::from(buf2));
    {
        Optional<uint8_t> c = is.peek();
        TESTA_ASSERT(c.present()).issue();
        TESTA_ASSERT(*c == 'c')(*c).issue();
        bool ret = is.moveNext();
        TESTA_ASSERT(!ret).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(BookmarkInputStream_traverse);

void BookmarkInputStream_BookmarkInOnePiece(const string&)
{
    string buf("ab");
    {
        Tracker tracker("track0");
        http::BookmarkInputStream is(tracker);
        is.pushBookmark();
        is.feed(MemPiece::from(buf));
        TESTA_ASSERT(is.moveNext()).issue();
        is.pushBookmark();

        deque<MemPiece> pieces;
        is.popBookmark(pieces);
        TESTA_ASSERT(pp::prettyPrint(pieces) == "[b\"b\"]")
            (pieces).issue();
        pieces.clear();
        is.popBookmark(pieces);
        TESTA_ASSERT(pp::prettyPrint(pieces) == "[b\"ab\"]")
            (pieces).issue();
    }
    {
        Tracker tracker("track1");
        http::BookmarkInputStream is(tracker);
        is.pushBookmark();
        is.feed(MemPiece::from(buf));

        deque<MemPiece> pieces;
        is.popBookmark(pieces);
        TESTA_ASSERT(pp::prettyPrint(pieces) == "[b\"a\"]")
            (pieces).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(BookmarkInputStream_BookmarkInOnePiece);

void BookmarkInputStream_BookmarkCrossPieces(const string&)
{
    string buf0("ab");
    string buf1("cd");
    string buf2("ef");
    string oracle = buf0 + buf1 + buf2;
    for(size_t s = 0; s < oracle.size(); ++s) {
        for(size_t e = s; e < oracle.size(); ++e) {
            string t("track_");
            t.append(pp::prettyPrint(s));
            t.push_back('_');
            t.append(pp::prettyPrint(e));
            Tracker tracker(t);
            http::BookmarkInputStream is(tracker);
            is.feed(MemPiece::from(buf0));
            is.feed(MemPiece::from(buf1));
            is.feed(MemPiece::from(buf2));

            for(size_t i = 0; i < s; ++i) {
                is.moveNext();
            }
            is.pushBookmark();
            for(size_t i = s; i < e; ++i) {
                is.moveNext();
            }
            deque<MemPiece> pieces;
            is.popBookmark(pieces);

            string trial;
            FOREACH_ITER(i, pieces) {
                const MemPiece& p = *i;
                trial.append((char*) p.data(), p.length());
            }
            TESTA_ASSERT(oracle.substr(s, e - s + 1) == trial)
                (oracle)
                (s)
                (e)
                (trial)
                (pieces).issue();
        }
    }
}
TESTA_DEF_JUNIT_LIKE1(BookmarkInputStream_BookmarkCrossPieces);

void BookmarkInputStream_MergePieces(const string&)
{
    string buf("abc");
    MemPiece oracle = MemPiece::from(buf);

    Tracker tracker("track");
    http::BookmarkInputStream is(tracker);
    is.feed(oracle.subpiece(0, 1));
    is.feed(oracle.subpiece(1, 1));
    is.feed(oracle.subpiece(2, 1));
    is.pushBookmark();
    is.moveNext();
    is.moveNext();
    deque<MemPiece> trial;
    is.popBookmark(trial);
    TESTA_ASSERT(pp::prettyPrint(trial) == "[b\"abc\"]")
        (trial)
        (oracle).issue();
}
TESTA_DEF_JUNIT_LIKE1(BookmarkInputStream_MergePieces);

} // namespace core
} // namespace tablestore
} // namespace aliyun

