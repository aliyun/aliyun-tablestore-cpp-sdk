/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#ifndef OTS_CONFIG_H
#define OTS_CONFIG_H

#include "ots_retry_strategy.h"
#include <string>

namespace aliyun {
namespace tablestore {

class ClientConfiguration
{
public:

    /**
     * 默认构造函数，赋予默认值。
     */
    ClientConfiguration();

    /**
     * 转换所有配置为字符串。
     */
    std::string ToString() const;

public:
    /**
     * 最大连接数，请求时如果无空闲连接，Client会自动创建连接；如果连接大量空闲，Client会自动释放连接。
     * 默认值：5000。
     */
    int mMaxConnections;

    /**
     * 连接超时时间，单位为毫秒。
     * 注意：每次重试的超时时间是独立的，如果要精确控制连接超时时间，需要乘以重试次数。
     * 默认值：2000。
     */
    int mConnectTimeoutInMS;

    /**
     * 发送数据超时时间（不包含连接时间），单位为毫秒。
     * 注意：每次重试的超时时间是独立的，如果要精确控制请求超时时间，需要乘以重试次数。
     * 默认值：10000。
     */
    int mRequestTimeoutInMS;

    /**
     * 请求追踪阈值，单位为毫秒，对于总处理时间超过该阈值的请求，Client会打印日志，记录处理时间。
     * 默认值：100。
     */
    int mClientTracerThresholdInMS;

    /**
     * 是否检查响应的MD5值。
     * 默认值：false。
     */
    bool mEnableCheckResponseMD5;

    /**
     * 是否开启KeepAlive。
     * 默认值：true。
     */
    bool mEnableKeepAlive;

    /**
     * 最大重试次数。
     * 默认值：3。
     */
    int mRetryMaxTimes;

    /**
     * 重试单位间隔时间，单位为毫秒，如第一次重试间隔时间为T，第二次为2T，第三次为4T。
     * 默认值：100。
     */
    int mRetryIntervalInMS;

    /**
     * 重试策略，ShouldRetry决定是否需要重试，GetPauseDelay返回重试间隔时间。
     * 默认策略：1. 网络错误，读操作可以重试，写操作不可以重试。
     *           2. 对于4XX错误，如果ErrorCode为下列错误码，则读写操作都可以重试。
     *              "OTSRowOperationConflict" 
     *              "OTSNotEnoughCapacityUnit"
     *              "OTSTableNotReady"
     *              "OTSPartitionUnavailable"
     *              "OTSServerBusy"
     *              "OTSQuotaExhausted"
     *           3. 响应为5XX错误或ErrorCode为下列错误码时，读操作可以重试，写操作不可以重试。
     *              "OTSTimeout"
     *              "OTSInternalServerError"
     *              "OTSServerUnavailable"
     */
    IRetryStrategyPtr mRetryStrategy;
};

struct Credential
{
    /**
     * 访问OTS服务的Access Key ID。
     */
    std::string mAccessKeyId;

    /**
     * 访问OTS服务的Access Key Secret。
     */
    std::string mAccessKeySecret;

    /**
     * 阿里云STS token。如果没有使用STS，本项必须为空。
     */
    std::string mStsToken;

    
    Credential()
    {}

    /**
     * 用于主账号或子账号访问表格存储
     */
    explicit Credential(
        const std::string& accessKeyId,
        const std::string& accessKeySecret)
      : mAccessKeyId(accessKeyId),
        mAccessKeySecret(accessKeySecret)
    {}

    /**
     * 用于短期访问凭证(STS)访问表格存储
     */
    explicit Credential(
        const std::string& accessKeyId,
        const std::string& accessKeySecret,
        const std::string& stsToken)
      : mAccessKeyId(accessKeyId),
        mAccessKeySecret(accessKeySecret),
        mStsToken(stsToken)
    {}
};

} // end of tablestore
} // end of aliyun

#endif
