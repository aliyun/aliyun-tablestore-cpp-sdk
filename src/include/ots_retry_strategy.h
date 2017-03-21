/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#ifndef OTS_RETRY_STRATEGY_H
#define OTS_RETRY_STRATEGY_H

#include "ots_types.h"
#include <string>
#include <tr1/memory>
#include <vector>

namespace aliyun {
namespace tablestore {

class IRetryStrategy 
{
public:

    virtual ~IRetryStrategy() {}
    
    /**
     * 是否需要发起第retries次重试。
     *
     * @param requestType 请求类型。
     * @param errors 错误列表，对于Batch操作可能会有多行错误。
     * @param httpStatus 请求HTTP状态。
     * @param retries 表示本次判断的为第retries次重试。
     * @return 是否需要发起第retries次重试。 
     */
    virtual bool ShouldRetry(
        const std::string& requestType,
        const std::vector<Error>& errors,
        int httpStatus,
        int retries) = 0;

    /**
     * 得到发起第retries次重试前延迟的时间。
     *
     * @param requestType 请求类型。
     * @param errors 错误列表，对于Batch操作可能会有多行错误。
     * @param httpStatus 请求HTTP状态。
     * @param retries 表示本次判断的为第retries次重试。
     * @return 发起第retries次重试前延迟的时间，单位毫秒。
     */
    virtual int64_t GetPauseDelay(
        const std::string& requestType,
        const std::vector<Error>& errors,
        int httpStatus,
        int retries) = 0;
};
typedef std::tr1::shared_ptr<IRetryStrategy> IRetryStrategyPtr;

} // end of tablestore
} // end of aliyun

#endif
