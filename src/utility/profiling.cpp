/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#include "profiling.h"
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
