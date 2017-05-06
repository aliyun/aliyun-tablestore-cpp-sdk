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
#include "profiling.hpp"
#include <sys/time.h>
#include <sstream>

using namespace std;

namespace aliyun {
namespace tablestore {

// Profiling
Profiling::Profiling()
{
    static string startState = "START";
    KeepTimeWithState(startState);
}

void Profiling::KeepTime()
{
    string emptyState;
    KeepTimeWithState(emptyState);
}

void Profiling::KeepTimeWithState(const string& recState)
{
    timeval tv;
    gettimeofday(&tv, NULL);
    TimeRecord record;
    record.mRecordState = recState;
    record.mTimeInUsec = tv.tv_sec * 1000000 + tv.tv_usec;
    mRecordList.push_back(record);
}

int64_t Profiling::GetTotalTime() const
{
    if (!mRecordList.empty()) {
        return mRecordList.back().mTimeInUsec - mRecordList.front().mTimeInUsec;
    } else {
        return 0;
    }
}

const list<TimeRecord>& Profiling::GetRecordList() const
{
    return mRecordList;
}

std::string Profiling::GetProfilingInfo() const
{
    stringstream ss;
    string profilingInfo;
    typeof(mRecordList.begin()) iter = mRecordList.begin();
    for (; iter != mRecordList.end(); ++iter) {
        ss << iter->mRecordState << ": " << iter->mTimeInUsec << " ";
    }
    ss << "TotalTime: " << GetTotalTime();
    return ss.str();
}

} // end of tablestore
} // end of aliyun
