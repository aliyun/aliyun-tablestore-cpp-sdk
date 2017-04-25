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

#include "testa.ipp"
#include <stdexcept>
#include <sstream>
#include <cstdlib>
#include <cstdio>

using namespace std;
#if __cplusplus < 201103L
using namespace std::tr1;
#endif

namespace testa {

shared_ptr<CaseMap> getCaseMap()
{
    static shared_ptr<CaseMap> result(new CaseMap());
    return result;
}

CaseFailIssuer::CaseFailIssuer(bool disable, const char* cond, const char* fn, int line)
  : mTrigger(!disable),
    mCondition(cond),
    mFilename(fn),
    mLine(line),
    mIssued(false),
    TESTA_PINGPONG_A(*this),
    TESTA_PINGPONG_B(*this)
{}

CaseFailIssuer::~CaseFailIssuer()
{
    if (!mIssued) {
        fprintf(stderr, "TESTA_ASSERT @ %s:%d does not invoke issue()\n", mFilename.c_str(), mLine);
        abort();
    }
}

void CaseFailIssuer::issue()
{
    issue(string());
}

void CaseFailIssuer::issue(const string& msg)
{
    mIssued = true;
    if (!mTrigger) {
        return;
    }
    ostringstream oss;
    oss << "Assertion @ " << mFilename << ":" << mLine << " fail: " << mCondition << endl;
    if (!msg.empty()) {
        oss << "Message: " << msg << endl;
    }
    for(int64_t i = 0, sz = mKeyValues.size(); i < sz; ++i) {
        if (i == 0) {
            oss << "Values: ";
        } else {
            oss << "        ";
        }
        oss << get<0>(mKeyValues[i]) << "=" << get<1>(mKeyValues[i]) << endl;
    }
    throw std::logic_error(oss.str());
}

CaseFailIssuer& CaseFailIssuer::append(const string& key, const string& value)
{
    mKeyValues.push_back(KV(key, value));
    return *this;
}

} // namespace testa

