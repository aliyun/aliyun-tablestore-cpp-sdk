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
#include "tablestore/util/security.hpp"
#include "tablestore/util/mempiece.hpp"
#include "tablestore/util/arithmetic.hpp"
#include "testa/testa.hpp"
#include <string>

using namespace std;

namespace aliyun {
namespace tablestore {
namespace util {

namespace {
void TestMd5(const string&)
{
    string in("abcdefghijklmnopqrstuvwxyz0123456789");
    Md5 dig;
    dig.update(MemPiece::from(in));
    uint8_t xs[16];
    MutableMemPiece p(xs, sizeof(xs));
    dig.finalize(p);
    string h;
    hex(h, MemPiece::from(xs));
    TESTA_ASSERT(h == "6D2286301265512F019781CC0CE7A39F")
        (h).issue();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(TestMd5);

namespace {
void TestSha1(const string&)
{
    string in("abcdefghijklmnopqrstuvwxyz0123456789");
    Sha1 dig;
    dig.update(MemPiece::from(in));
    uint8_t xs[Sha1::kLength];
    dig.finalize(MutableMemPiece(xs, sizeof(xs)));
    string h;
    hex(h, MemPiece::from(xs));
    TESTA_ASSERT(h == "D2985049A677BBC4B4E8DEA3B89C4820E5668E3A")
        (h).issue();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(TestSha1);

namespace {
void TestBase64Encoder(const string&)
{
    string in("abcdefghijklmnopqrstuvwxyz0123456789");
    Base64Encoder b64;
    b64.update(MemPiece::from(in));
    b64.finalize();
    MemPiece p = b64.base64();
    TESTA_ASSERT(pp::prettyPrint(p) == "b\"YWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXowMTIzNDU2Nzg5\"")
        (p).issue();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(TestBase64Encoder);

namespace {
void TestHmacSha1_ShortKey(const string&)
{
    string key("key");
    string in("The quick brown fox jumps over the lazy dog");
    HmacSha1 hmac(MemPiece::from(key));
    hmac.update(MemPiece::from(in));
    uint8_t xs[HmacSha1::kLength];
    hmac.finalize(MutableMemPiece(xs, sizeof(xs)));
    string h;
    hex(h, MemPiece::from(xs));
    TESTA_ASSERT(h == "DE7C9B85B8B78AA6BC8A7A36F70A90701C9DB4D9")
        (h).issue();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(TestHmacSha1_ShortKey);

namespace {
void TestHmacSha1_LongKey(const string&)
{
    string key("0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz");
    string in("The quick brown fox jumps over the lazy dog");
    HmacSha1 hmac(MemPiece::from(key));
    hmac.update(MemPiece::from(in));
    uint8_t xs[HmacSha1::kLength];
    hmac.finalize(MutableMemPiece(xs, sizeof(xs)));
    string h;
    hex(h, MemPiece::from(xs));
    TESTA_ASSERT(h == "2D30FB10D128679D283E2C79B0B4CFB33708CA66")
        (h).issue();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(TestHmacSha1_LongKey);

namespace {
inline uint32_t adler32oracle(const MemPiece& in)
{
    static const uint32_t kMod = 65521; // the largest prime number below 2^16
    uint32_t x = 1;
    uint32_t y = 0;
    const uint8_t* b = in.data();
    const uint8_t* e = b + in.length();
    for(; b < e; ++b) {
        x = (x + *b) % kMod;
        y = (y + x) % kMod;
    }
    return (y << 16) | x;
}

inline uint32_t adler32trial(const MemPiece& in)
{
    Adler32 alg;
    const uint8_t* b = in.data();
    const uint8_t* e = b + in.length();
    for(; b < e; ++b) {
        alg.update(*b);
    }
    return alg.get();
}

void TestAdler32_0(const string&)
{
    string plaintext("0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz");
    uint32_t oracle = adler32oracle(MemPiece::from(plaintext));
    uint32_t trial = adler32trial(MemPiece::from(plaintext));
    TESTA_ASSERT(oracle == trial)
        (oracle)
        (trial).issue();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(TestAdler32_0);

namespace {
void TestAdler32_1(const string&)
{
    string plaintext("0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz");
    uint32_t oracle = adler32oracle(MemPiece::from(plaintext));
    Adler32 adl0;
    update(adl0, MemPiece::from(plaintext.substr(0, 10)));
    Adler32 adl1(adl0.get());
    update(adl1, MemPiece::from(plaintext.substr(10)));
    uint32_t trial = adl1.get();
    TESTA_ASSERT(oracle == trial)
        (oracle)
        (trial).issue();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(TestAdler32_1);

} // namespace util
} // namespace tablestore
} // namespace aliyun
