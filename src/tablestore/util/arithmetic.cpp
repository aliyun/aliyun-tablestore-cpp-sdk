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
#include "arithmetic.hpp"
#include "tablestore/util/mempiece.hpp"
#include "tablestore/util/assert.hpp"
#include <algorithm>
#include <deque>

using namespace std;

namespace aliyun {
namespace tablestore {
namespace util {

Optional<string> toUint64(uint64_t& out, const MemPiece& in, int64_t radix)
{
    OTS_ASSERT(1 < radix && radix <= 36)(radix);
    if (in.length() == 0) {
        return Optional<string>(string(
                "Cannot convert a string of 0 length to a number."));
    }
    uint64_t res = 0;
    const uint8_t* b = in.data();
    const uint8_t* e = b + in.length();
    for(; b < e; ++b) {
        uint64_t x = res * radix;
        if ('0' <= *b && *b <= min('9', static_cast<char>('0' + radix))) {
            x += (*b - '0');
        } else if ('A' <= *b && *b <= 'A' + radix - 10) {
            x += (*b - 'A') + 10;
        } else if ('a' <= *b && *b <= 'a' + radix - 10) {
            x += (*b - 'a') + 10;
        } else {
            return Optional<string>(string(
                    "Unrecognized character in converting a string to a number."));
        }
        if (x < res) {
            return Optional<string>(string(
                    "Overflow in converting a string to a number."));
        }
        res = x;
    }
    out = res;
    return Optional<string>();
}

void format(string& out, uint64_t num, int64_t radix)
{
    static const char kAlphabet[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    OTS_ASSERT(1 < radix && radix <= 36)(radix);
    if (num == 0) {
        out.push_back('0');
    } else {
        deque<char> xs;
        for(; num > 0; num /= radix) {
            xs.push_back(kAlphabet[num % radix]);
        }
        for(; !xs.empty(); xs.pop_back()) {
            out.push_back(xs.back());
        }
    }
}

void hex(string& out, const MemPiece& in)
{
    if (in.length() == 0) {
        return;
    }
    static const char kAlphabet[] = "0123456789ABCDEF";
    const uint8_t* b = in.data();
    const uint8_t* e = b + in.length();
    for(; b < e; ++b) {
        uint8_t low = *b & 0xF;
        uint8_t high = *b >> 4;
        out.push_back(kAlphabet[high]);
        out.push_back(kAlphabet[low]);
    }
}

namespace impl {
static const char kBase57Alphabet[] =
    "0123456789abcdefghijkmnopqrstvwxyzABCDEFGHJKLMNPQRSTVWXYZ";

uint8_t base57decode(char in)
{
    const char* p = find(kBase57Alphabet, kBase57Alphabet + 57, in);
    OTS_ASSERT(p < kBase57Alphabet + 57)
        (in)
        (p - kBase57Alphabet);
    return p - kBase57Alphabet;
}

} // namespace impl

void base57encode(string& out, uint64_t in)
{
    if (in == 0) {
        out.push_back('0');
        return;
    }
    for(; in > 0; in /= 57) {
        out.push_back(impl::kBase57Alphabet[in % 57]);
    }
    for(int64_t left = 0, right = out.size() - 1; left < right; ++left, --right) {
        swap(out[left], out[right]);
    }
}

uint64_t base57decode(const MemPiece& in)
{
    uint64_t out = 0;
    const uint8_t* s = in.data();
    const uint8_t* e = s + in.length();
    for(; s < e; ++s) {
        out *= 57;
        out += impl::base57decode(static_cast<char>(*s));
    }
    return out;
}

} // namespace util
} // namespace tablestore
} // namespace aliyun

