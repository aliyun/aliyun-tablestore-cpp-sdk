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
#include "tablestore/util/arithmetic.hpp"
#include "tablestore/util/mempiece.hpp"
#include "tablestore/util/foreach.hpp"
#include "testa/testa.hpp"
#include <tr1/tuple>
#include <string>
#include <limits>
#include <stdint.h>
#include <cstdio>

using namespace std;
using namespace std::tr1;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {

namespace {
uint64_t to_uint64_uppercase(const tuple<uint64_t, int>& in)
{
    string s;
    format(s, get<0>(in), get<1>(in));
    uint64_t res;
    Optional<string> err = toUint64(res, MemPiece::from(s), get<1>(in));
    TESTA_ASSERT(!err.present())
        (*err).issue();
    return res;
}

uint64_t to_uint64_lowercase(const tuple<uint64_t, int>& in)
{
    static const char kLowerCases[] = "abcdefghijklmnopqrstuvwxyz";
    string s;
    format(s, get<0>(in), get<1>(in));
    for(size_t i = 0; i < s.size(); ++i) {
        if ('A' <= s[i] && s[i] <= 'Z') {
            s[i] = kLowerCases[s[i] - 'A'];
        }
    }
    fprintf(stderr, "str=%s\n", s.c_str());
    uint64_t res;
    Optional<string> err = toUint64(res, MemPiece::from(s), get<1>(in));
    TESTA_ASSERT(!err.present())
        (*err).issue();
    return res;
}

void to_uint64_tb(const string& name, function<void(const tuple<uint64_t, int>&)> cs)
{
    deque<uint64_t> oracles;
    oracles.push_back(0);
    oracles.push_back(1);
    oracles.push_back(12);
    oracles.push_back(numeric_limits<uint64_t>::max());
    for(int radix = 2; radix <= 36; ++radix) {
        FOREACH_ITER(i, oracles) {
            cs(make_tuple(*i, radix));
        }
    }
}

void to_uint64_verifier(const uint64_t& res, const tuple<uint64_t, int>& in)
{
    TESTA_ASSERT(res == get<0>(in))
        (res)
        (in)
        .issue();
}
}
TESTA_DEF_VERIFY_WITH_TB(ToUnt64_UpperCase, to_uint64_tb, to_uint64_verifier, to_uint64_uppercase);
TESTA_DEF_VERIFY_WITH_TB(ToUnt64_LowerCase, to_uint64_tb, to_uint64_verifier, to_uint64_lowercase);

void Base57_0(const string&)
{
    string out;
    base57encode(out, 0);
    TESTA_ASSERT(out == "0")
        (out).issue();
}
TESTA_DEF_JUNIT_LIKE1(Base57_0);

void Base57_bFCnz(const string&)
{
    string out;
    base57encode(out, 123456789ULL);
    TESTA_ASSERT(out == "bFCnz")
        (out).issue();
}
TESTA_DEF_JUNIT_LIKE1(Base57_bFCnz);

void Base57_range(const string&)
{
    string out;
    for(uint64_t orc = 0; orc < 100000; ++orc) {
        out.clear();
        base57encode(out, orc);
        uint64_t trial = base57decode(MemPiece::from(out));
        TESTA_ASSERT(trial == orc)
            (trial)
            (orc)
            .issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(Base57_range);

} // namespace tablestore
} // namespace aliyun
