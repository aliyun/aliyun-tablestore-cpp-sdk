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
#include "src/tablestore/core/plainbuffer/plain_buffer_crc8.hpp"
#include "tablestore/util/random.hpp"
#include "tablestore/util/security.hpp"
#include "tablestore/util/optional.hpp"
#include "testa/testa.hpp"
#include <deque>
#include <string>
#include <memory>
#include <limits>
#include <cstdio>

using namespace std;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {

namespace {
void Crc8(const string&)
{
    for(int i = 0; i < 256; ++i) {
        uint8_t oracle = PlainBufferCrc8::CrcInt8(0, (uint8_t) i);
        uint8_t trial = 0;
        crc8(trial, (uint8_t) i);
        TESTA_ASSERT(oracle == trial)
            (oracle)
            (trial).issue();
    }
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(Crc8);

namespace {
void Crc8_U32(const string&)
{
    auto_ptr<random::Random> rng(random::newDefault(0));
    for(int i = 0; i < 10000; ++i) {
        uint32_t in = random::nextInt(*rng, numeric_limits<uint32_t>::max());
        uint8_t oracle = PlainBufferCrc8::CrcInt32(0, in);
        uint8_t trial = 0;
        crc8U32(trial, in);
        TESTA_ASSERT(trial == oracle)
            (oracle)
            (trial)
            (in).issue();
    }
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(Crc8_U32);

namespace {
uint64_t randomU64(random::Random& rng)
{
    uint64_t res = 0;
    for(int i = 0; i < 4; ++i) {
        res <<= 16;
        res |= random::nextInt(rng, numeric_limits<uint16_t>::max());
    }
    return res;
}

void Crc8_U64(const string&)
{
    auto_ptr<random::Random> rng(random::newDefault(0));
    for(int i = 0; i < 10000; ++i) {
        uint64_t in = randomU64(*rng);
        uint8_t oracle = PlainBufferCrc8::CrcInt64(0, in);
        uint8_t trial = 0;
        crc8U64(trial, in);
        TESTA_ASSERT(trial == oracle)
            (oracle)
            (trial)
            (in).issue();
    }
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(Crc8_U64);

namespace {
string randomStr(random::Random& rng, const string& alphabet, char terminator)
{
    string res;
    for(;;) {
        int64_t idx = random::nextInt(rng, alphabet.size());
        char c = alphabet[idx];
        if (c == terminator) {
            break;
        }
        res.push_back(c);
    }
    return res;
}

void Crc8_Str(const string&)
{
    const string kAlphabet("abcdefgh.");
    auto_ptr<random::Random> rng(random::newDefault(0));
    printf("%lld\n", (long long) rng->upperBound());
    for(int64_t i = 0; i < 10000; ++i) {
        string in = randomStr(*rng, kAlphabet, '.');
        uint8_t oracle = PlainBufferCrc8::CrcString(0, MemPiece::from(in));
        uint8_t trial = 0;
        crc8MemPiece(trial, MemPiece::from(in));
        TESTA_ASSERT(trial == oracle)
            (in)
            (oracle)
            (trial).issue();
    }
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(Crc8_Str);

} // namespace core
} // namespace tablestore
} // namespace aliyun

