/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#ifndef PROFILING_H
#define PROFILING_H

#include <string>
#include <list>
#include <stdint.h>

namespace aliyun {
namespace tablestore {

// timing
class TimeRecord
{
public:

    std::string mRecordState;
    int64_t mTimeInUsec;
};

class Profiling
{
public:

    Profiling();

    void KeepTime();

    void KeepTimeWithState(const std::string& recState);

    int64_t GetTotalTime() const;

    const std::list<TimeRecord>& GetRecordList() const;

    std::string GetProfilingInfo() const; 

private:

    std::list<TimeRecord> mRecordList;
};

} // end of tablestore
} // end of aliyun

#endif
