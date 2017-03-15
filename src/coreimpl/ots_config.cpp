/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#include "ots/ots_config.h"
#include <sstream>

using namespace std;

namespace aliyun {
namespace tablestore {

ClientConfiguration::ClientConfiguration()
  : mMaxConnections(5000),
    mConnectTimeoutInMS(2000),
    mRequestTimeoutInMS(10000),
    mClientTracerThresholdInMS(100),
    mEnableCheckResponseMD5(true),
    mEnableKeepAlive(true),
    mRetryMaxTimes(3),
    mRetryIntervalInMS(100)
{}

string ClientConfiguration::ToString() const
{
    stringstream ss; 
    ss << "MaxConnections: " << mMaxConnections << endl;
    ss << "ConnectTimeoutInMS: " << mConnectTimeoutInMS << endl;
    ss << "RequestTimeoutInMS: " << mRequestTimeoutInMS << endl;
    ss << "ClientTracerThresholdInMS: " << mClientTracerThresholdInMS << endl;
    ss << "EnableCheckResponseMD5: " << mEnableCheckResponseMD5 << endl;
    ss << "EnableKeepAlive: " << mEnableKeepAlive << endl;
    ss << "RetryMaxTimes: " << mRetryMaxTimes << endl;
    ss << "RetryIntervalInMS: " << mRetryIntervalInMS << endl;
    return ss.str();
}

} // end of tablestore
} // end of aliyun
