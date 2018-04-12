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
#include "mempiece.hpp"
#include "tablestore/util/assert.hpp"
#include "tablestore/util/prettyprint.hpp"

using namespace std;

namespace aliyun {
namespace tablestore {
namespace util {

void MemPiece::prettyPrint(string& out) const
{
    if (data() == NULL) {
        out.append("b\"\"");
        return;
    }
    out.append("b\"");
    const uint8_t* b = data();
    const uint8_t* e = b + length();
    for(; b < e; ++b) {
        pp::impl::Char::p(out, static_cast<char>(*b));
    }
    out.append("\"");
}

namespace {

inline bool digit(uint8_t c)
{
    return c >= '0' && c <= '9';
}

} // namespace

namespace impl {

Optional<string> FromMemPiece<int64_t, void>::operator()(
    int64_t& out,
    const MemPiece& mp) const
{
    if (mp.length() == 0) {
        return Optional<string>(string("Empty piece of memory."));
    }
    const uint8_t* b = mp.data();
    const uint8_t* e = b + mp.length();
    bool positive = true;
    if (*b == '-') {
        positive = false;
        ++b;
    }
    int64_t x = 0;
    if (positive) {
        for(; b < e; ++b) {
            if (!digit(*b)) {
                return Optional<string>(string("Nondigital."));
            }
            x = x * 10 + (*b - '0');
            if (x < 0) {
                return Optional<string>(string("Overflow."));
            }
        }
        out = x;
    } else {
        if (b == e) {
            return Optional<string>(string("A single '-'."));
        }
        for(; b < e; ++b) {
            if (!digit(*b)) {
                return Optional<string>(string("Nondigital."));
            }
            x = x * 10 - (*b - '0');
            if (x >= 0) {
                return Optional<string>(string("Underflow."));
            }
        }
        out = x;
    }
    return Optional<string>();
}

Optional<string> FromMemPiece<string, void>::operator()(
    string& out,
    const MemPiece& mp) const
{
    if (out.size() + mp.length() + 1 > out.capacity()) {
        out.reserve(out.size() + mp.length() + 1);
    }
    out.append(
        static_cast<const char*>(static_cast<const void*>(mp.data())),
        mp.length());
    return Optional<string>();
}

} // namespace impl

} // namespace util
} // namespace tablestore
} // namespace aliyun
