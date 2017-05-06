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
#include "screen_logger.hpp"
#include "tablestore/util/foreach.hpp"
#include <cstdio>

using namespace std;

ScreenLogger::ScreenLogger(LogLevel lvl)
  : mLevel(lvl)
{}

ScreenLogger::~ScreenLogger()
{
    FOREACH_ITER(it, mSubs) {
        delete it->second;
    }
    mSubs.clear();
}

ScreenLogger::LogLevel ScreenLogger::level() const
{
    return mLevel;
}

void ScreenLogger::record(LogLevel lvl, const string& msg)
{
    printf("%s\n", msg.c_str());
}

void ScreenLogger::flush()
{}

aliyun::tablestore::util::Logger* ScreenLogger::spawn(const string& key)
{
    map<string, Logger*>::iterator it = mSubs.find(key);
    if (it != mSubs.end()) {
        return it->second;
    }
    Logger* sub = new ScreenLogger(level());
    mSubs.insert(make_pair(key, sub));
    return sub;
}
