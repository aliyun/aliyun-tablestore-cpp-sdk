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
#include "logging.ipp"
#include "tablestore/util/logger.hpp"
#include <cstdlib>
#include <stdint.h>

using namespace std;

namespace aliyun {
namespace tablestore {
namespace util {
namespace impl {

namespace {

int64_t calcSize(
    const string& file,
    const string& line,
    const string& func,
    const string& what,
    const deque<pair<string, string> >& xs)
{
    int64_t res = 0;

    res += 5; // "FILE:"
    res += file.size();
    res += 1; // "\t"
    res += 5; // "LINE:"
    res += line.size();
    res += 1; // "\t"
    res += 9; // "FUNCTION:"
    res += func.size();
    res += 1; // "\t"

    if (!what.empty()) {
        res += 5; // "what:"
        res += what.size();
        if (!xs.empty()) {
            res += 1; // "\t"
        }
    }

    if (!xs.empty()) {
        res += (xs.size() - 1) * 1; // "\t"
        for(int64_t i = 0, sz = xs.size(); i < sz; ++i) {
            res += xs[i].first.size();
            res += 1; // ":"
            res += xs[i].second.size();
        }
    }

    if (res < 0) {
        abort();
    }
    return res;
}

} // namespace

LogHelper::~LogHelper()
{
    const string kSep("\t");

    int64_t size = calcSize(mFile, mLine, mFunc, mWhat, mValues);
    string res;
    res.reserve(size + 1); // +1 for tailing '\0'

    res.append("FILE:");
    res.append(mFile);
    res.append(":");
    res.append(mLine);
    res.append(kSep);

    res.append("FUNCTION:");
    res.append(mFunc);
    res.append(kSep);

    if (!mWhat.empty()) {
        res.append("What:");
        res.append(mWhat);
        if (!mValues.empty()) {
            res.append(", ");
        }
    }

    for(int64_t i = 0, sz = mValues.size(); i < sz; ++i) {
        if (i > 0) {
            res.append(", ");
        }
        res.append(mValues[i].first);
        res.push_back(':');
        res.append(mValues[i].second);
    }

    mLogger.record(mLevel, res);
}

} // namespace impl
} // namespace util
} // namespace tablestore
} // namespace aliyun
