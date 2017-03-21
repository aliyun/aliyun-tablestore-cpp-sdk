/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#ifndef DEFAULT_RETRY_STRATEGY_IMPL_H
#define DEFAULT_RETRY_STRATEGY_IMPL_H

#include "ots/ots_retry_strategy.h"
#include <string>
#include <tr1/memory>

namespace aliyun {
namespace tablestore {

class DefaultRetryStrategy : public IRetryStrategy
{
public:

    DefaultRetryStrategy(
        int retryMaxTimes,
        int retryInternvalInMS);

    virtual ~DefaultRetryStrategy();

    virtual bool ShouldRetry(
        const std::string& requestType,
        const std::vector<Error>& errors,
        int httpStatus,
        int retries);

    virtual int64_t GetPauseDelay(
        const std::string& requestType,
        const std::vector<Error>& errors,
        int httpStatus,
        int retries);

private:

    bool IsClientError(int httpStatus);

    bool IsIdempotent(const std::string& requestType);

    bool IsErrorRetriable(
        const Error& error,
        int httpStatus,
        bool isIdempotent);

    int GetDelayTime(
        const std::string& errorCode,
        const std::string& errorMessage,
        int retryCount);

private:
    
    int mRetryMaxTimes;

    int mRetryIntervalInMS;
};

} // end of tablestore
} // end of aliyun

#endif
