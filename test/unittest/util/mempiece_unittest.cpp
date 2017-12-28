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
#include "tablestore/util/mempiece.hpp"
#include "tablestore/util/foreach.hpp"
#include "testa/testa.hpp"
#include <tr1/functional>
#include <deque>
#include <string>
#include <limits>
#include <stdint.h>

using namespace std;
using namespace std::tr1;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {

void MemPiece_fromStr(const string&)
{
    {
        string s("abc");
        MemPiece piece = MemPiece::from(s);
        TESTA_ASSERT(piece.to<string>() == "abc")
            (piece.to<string>()).issue();
    }
    {
        string s("def");
        MemPiece piece = MemPiece::from(s.c_str());
        TESTA_ASSERT(piece.to<string>() == "def")
            (piece.to<string>()).issue();
    }
    {
        MemPiece piece = MemPiece::from("ghi");
        TESTA_ASSERT(piece.to<string>() == "ghi")
            (piece.to<string>()).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(MemPiece_fromStr);

void MemPiece_pp(const string&)
{
    {
        MemPiece piece = MemPiece::from("abc");
        TESTA_ASSERT(pp::prettyPrint(piece) == "b\"abc\"")
            (piece).issue();
    }
    {
        MemPiece piece = MemPiece::from("abc\0");
        TESTA_ASSERT(pp::prettyPrint(piece) == "b\"abc\\x00\"")
            (piece).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(MemPiece_pp);

void MemPiece_prefix(const string&)
{
    MemPiece whole = MemPiece::from("ababa");
    {
        MemPiece b = whole.subpiece(0, 1);
        MemPiece c = whole.subpiece(0, 2);
        TESTA_ASSERT(!b.startsWith(c))
            (b)(c).issue();
    }
    {
        MemPiece b;
        TESTA_ASSERT(whole.startsWith(b))
            (whole)(b).issue();
    }
    {
        MemPiece b = whole.subpiece(2, 0);
        TESTA_ASSERT(whole.startsWith(b))
            (whole)(b).issue();
    }
    {
        MemPiece b = whole.subpiece(0, 1);
        TESTA_ASSERT(whole.startsWith(b))
            (whole)(b).issue();
    }
    {
        MemPiece b = whole.subpiece(2, 2);
        TESTA_ASSERT(whole.startsWith(b))
            (whole)(b).issue();
    }
    {
        MemPiece b = whole.subpiece(1, 2);
        TESTA_ASSERT(!whole.startsWith(b))
            (whole)(b).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(MemPiece_prefix);

void MemPiece_suffix(const string&)
{
    MemPiece whole = MemPiece::from("ababa");
    {
        MemPiece b = whole.subpiece(4, 1);
        MemPiece c = whole.subpiece(3, 2);
        TESTA_ASSERT(!b.endsWith(c))
            (b)(c).issue();
    }
    {
        MemPiece b;
        TESTA_ASSERT(whole.endsWith(b))
            (whole)(b).issue();
    }
    {
        MemPiece b = whole.subpiece(1, 0);
        TESTA_ASSERT(whole.startsWith(b))
            (whole)(b).issue();
    }
    {
        MemPiece b = whole.subpiece(4, 1);
        TESTA_ASSERT(whole.startsWith(b))
            (whole)(b).issue();
    }
    {
        MemPiece b = whole.subpiece(1, 2);
        TESTA_ASSERT(whole.endsWith(b))
            (whole)(b).issue();
    }
    {
        MemPiece b = whole.subpiece(2, 2);
        TESTA_ASSERT(!whole.endsWith(b))
            (whole)(b).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(MemPiece_suffix);

void MemPiece_lexi_order(const string&)
{
    string aa = "aa";
    string b = "b";
    TESTA_ASSERT(lexicographicOrder(MemPiece::from(aa), MemPiece::from(b)) < 0).issue();
    TESTA_ASSERT(lexicographicOrder(MemPiece::from(b), MemPiece::from(aa)) > 0).issue();
    LexicographicLess<string> less;
    TESTA_ASSERT(less(aa, b)).issue();
}
TESTA_DEF_JUNIT_LIKE1(MemPiece_lexi_order);

void MemPiece_quasilexi_order(const string&)
{
    string aa = "aa";
    string b = "b";
    TESTA_ASSERT(quasilexicographicOrder(MemPiece::from(aa), MemPiece::from(b)) > 0).issue();
    TESTA_ASSERT(quasilexicographicOrder(MemPiece::from(b), MemPiece::from(aa)) < 0).issue();
    QuasiLexicographicLess<string> less;
    TESTA_ASSERT(less(b, aa)).issue();
}
TESTA_DEF_JUNIT_LIKE1(MemPiece_quasilexi_order);

void MutableMemPiece_pp(const string&)
{
    {
        MutableMemPiece mp;
        TESTA_ASSERT(pp::prettyPrint(mp) == "b\"\"")
            (mp).issue();
    }
    char str[] = "abc";
    {
        MutableMemPiece mp(str, sizeof(str) - 1);
        TESTA_ASSERT(pp::prettyPrint(mp) == "b\"abc\"")
            (mp).issue();
    }
    {
        MutableMemPiece mp(str, str + 1);
        TESTA_ASSERT(pp::prettyPrint(mp) == "b\"a\"")
            (mp).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(MutableMemPiece_pp);

void MutableMemPiece_subpiece(const string&)
{
    {
        MutableMemPiece mp;
        TESTA_ASSERT(pp::prettyPrint(mp.subpiece(NULL)) == "b\"\"")
            (mp.subpiece(NULL)).issue();
        TESTA_ASSERT(pp::prettyPrint(mp.subpiece(NULL, NULL)) == "b\"\"")
            (mp.subpiece(NULL, NULL)).issue();
    }
    char str[] = "abc";
    {
        MutableMemPiece mp(str, sizeof(str) - 1);
        TESTA_ASSERT(pp::prettyPrint(mp.subpiece(str + 1)) == "b\"bc\"")
            (mp.subpiece(str + 1)).issue();
        TESTA_ASSERT(pp::prettyPrint(mp.subpiece(str + 1, str + 2)) == "b\"b\"")
            (mp.subpiece(str + 1, str + 2)).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(MutableMemPiece_subpiece);

void MutableMemPiece_length_get(const string&)
{
    {
        MutableMemPiece mp;
        TESTA_ASSERT(mp.length() == 0)
            (mp).issue();
    }
    {
        char str[] = "abc";
        MutableMemPiece mp(str, sizeof(str) - 1);
        TESTA_ASSERT(mp.length() == 3)
            (mp).issue();
        TESTA_ASSERT(mp.get(1) == 'b')
            (mp).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(MutableMemPiece_length_get);

int64_t to_int64(const int64_t& in)
{
    string s = pp::prettyPrint(in);
    return MemPiece::from(s).to<int64_t>();
}

void to_int64_tb(const string& name, function<void(const int64_t&)> cs)
{
    deque<int64_t> oracles;
    oracles.push_back(0);
    oracles.push_back(1);
    oracles.push_back(12);
    oracles.push_back(-1);
    oracles.push_back(-12);
    oracles.push_back(numeric_limits<int64_t>::min());
    oracles.push_back(numeric_limits<int64_t>::max());
    FOREACH_ITER(i, oracles) {
        cs(*i);
    }
}

void to_int64_verifier(const int64_t& res, const int64_t& in)
{
    TESTA_ASSERT(res == in)
        (res)
        (in)
        .issue();
}
TESTA_DEF_VERIFY_WITH_TB(MemPiece_ToInt64, to_int64_tb, to_int64_verifier, to_int64);

void MemPiece_ToInt64_errors(const string&)
{
    {
        int64_t x;
        Optional<string> err = MemPiece().to(x);
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(*err == "Empty piece of memory.")
            (*err).issue();
    }
    {
        int64_t x;
        Optional<string> err = MemPiece::from("-").to(x);
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(*err == "A single '-'.")
            (*err).issue();
    }
    {
        int64_t x;
        Optional<string> err = MemPiece::from("a").to(x);
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(*err == "Nondigital.")
            (*err).issue();
    }
    {
        int64_t x;
        Optional<string> err = MemPiece::from("9223372036854775808").to(x);
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(*err == "Overflow.")
            (*err).issue();
    }
    {
        int64_t x;
        Optional<string> err = MemPiece::from("-9223372036854775809").to(x);
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(*err == "Underflow.")
            (*err).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(MemPiece_ToInt64_errors);

} // namespace tablestore
} // namespace aliyun
