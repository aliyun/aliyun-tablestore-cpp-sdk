/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#include "default_retry_strategy.h"
#include "ots_constants.h"
#include "../http/http_code.h"
#include "curl/curl.h"
#include <cstdlib>
#include <cmath>

using namespace std;

namespace aliyun {
namespace tablestore {

DefaultRetryStrategy::DefaultRetryStrategy(
    int retryMaxTimes,
    int retryInternvalInMS)
    : mRetryMaxTimes(retryMaxTimes)
    , mRetryIntervalInMS(retryInternvalInMS)
{
}

DefaultRetryStrategy::~DefaultRetryStrategy()
{
}

bool DefaultRetryStrategy::ShouldRetry(
    const string& requestType,
    const vector<Error>& errors,
    int httpStatus,
    int retries)
{
    if (retries > mRetryMaxTimes) {
        return false;
    }
    if (errors.empty()) {
        return false;
    }
    bool isIdempotent = IsIdempotent(requestType);
    if (IsClientError(httpStatus)) {
        return isIdempotent;
    } else {
        for (typeof(errors.begin()) iter = errors.begin(); iter != errors.end(); ++iter) {
            if (!IsErrorRetriable(*iter, httpStatus, isIdempotent)) {
                return false;
            }
        }
        return true;
    }
}

bool DefaultRetryStrategy::IsClientError(int httpStatus)
{
    return (httpStatus < 200);
}

bool DefaultRetryStrategy::IsIdempotent(const string& requestType)
{
    if (requestType == kAPIListTable ||
        requestType == kAPIDescribeTable ||
        requestType == kAPIGetRow ||
        requestType == kAPIBatchGetRow ||
        requestType == kAPIGetRange) {
        return true;
    }
    return false;
}

bool DefaultRetryStrategy::IsErrorRetriable(
    const Error& error,
    int httpStatus,
    bool isIdempotent)
{
    const string errorCode = error.GetCode();
    const string errorMessage = error.GetMessage();
    if (errorCode == "OTSRowOperationConflict" ||
        errorCode == "OTSNotEnoughCapacityUnit" || 
        errorCode == "OTSTableNotReady" ||
        errorCode == "OTSPartitionUnavailable" ||
        errorCode == "OTSServerBusy" ||
        (errorCode == "OTSQuotaExhausted" && errorMessage == "Too frequent table operations.")) {
        return true;
    }
    bool isServerError = (httpStatus >= 500 && httpStatus <= 599);
    if (isIdempotent && 
        (errorCode == "OTSTimeout" ||
         errorCode == "OTSInternalServerError" ||
         errorCode == "OTSServerUnavailable" ||
         isServerError)) {
        return true;
    }
    return false;
}

int64_t DefaultRetryStrategy::GetPauseDelay(
    const std::string& requestType,
    const std::vector<Error>& errors,
    int httpStatus,
    int retries)
{
    return (int64_t)(mRetryIntervalInMS * pow((double) 2, retries-1));
}

} // end of tablestore
} // end of aliyun
