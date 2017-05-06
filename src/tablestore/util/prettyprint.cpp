/*
This file is picked from project testa [https://github.com/TimeExceed/testa.git]
Copyright (c) 2017, Taoda (tyf00@aliyun.com)
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice, this
  list of conditions and the following disclaimer in the documentation and/or
  other materials provided with the distribution.

* Neither the name of the {organization} nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#include "prettyprint.hpp"
#include <sstream>
#include <iomanip>
#include <cstdlib>

using namespace std;
#if __cplusplus < 201103L
using namespace std::tr1;
#endif


namespace pp {
namespace impl {

void SignedInteger::prettyPrint(string* out, int64_t x)
{
    ostringstream os;
    os << x;
    out->append(os.str());
}

void UnsignedInteger::prettyPrint(string* out, uint64_t x)
{
    ostringstream os;
    os << x;
    out->append(os.str());
}

void Boolean::prettyPrint(string* out, bool x)
{
    static const char kTrue[] = "true";
    static const char kFalse[] = "false";
    if (x) {
        out->append(kTrue, sizeof(kTrue) - 1);
    } else {
        out->append(kFalse, sizeof(kFalse) - 1);
    }
}

namespace {

void toHex(string* out, uint8_t x)
{
    static const char kAlphabet[] = "0123456789ABCDEF";
    if (x > 15) {
        abort();
    }
    out->push_back(kAlphabet[x]);
}

} // namespace

void Character::prettyPrint(string* out, char x)
{
    uint8_t xx = x;
    if (xx >= 32 && xx <= 127) {
        if (x == '\'') {
            out->push_back('\\');
            out->push_back('\'');
        } else if (x == '"') {
            out->push_back('\\');
            out->push_back('"');
        } else {
            out->push_back(x);
        }
    } else {
        out->push_back('\\');
        out->push_back('x');
        toHex(out, xx >> 4);
        toHex(out, xx & 0xF);
    }
}

void PrettyPrinter<StlStr, string>::operator()(string* out, const string& x) const
{
    out->push_back('\"');
    for(string::const_iterator i = x.begin(); i != x.end(); ++i) {
        Character::prettyPrint(out, *i);
    }
    out->push_back('\"');
}

void Floating::prettyPrint(string* out, double x)
{
    ostringstream oss;
    oss.setf(ios_base::fixed);
    oss << setprecision(4) << x;
    out->append(oss.str());
}

} // namespace impl
} // namespace pp

