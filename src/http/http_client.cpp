/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#include "http_client.h"
#include "ots/ots_exception.h"
#include "curl/curl.h"

using namespace std;

namespace aliyun {
namespace tablestore {

// HttpConfig
HttpConfig::HttpConfig()
    : mMaxConnections(5000)
    , mConnectTimeoutInMS(2000)  // 2s
    , mRequestTimeoutInMS(10000) // 30s
    , mEnableKeepAlive(true)
{
}

void HttpConfig::SetMaxConnections(int maxConnections)
{
    if (maxConnections <= 0) {
        throw OTSClientException("MaxConnections should be larger than 0.");
    }
    mMaxConnections = maxConnections;
}

int HttpConfig::GetMaxConnections() const
{
    return mMaxConnections;
}

void HttpConfig::SetConnectTimeoutInMS(int connectTimeoutInMS)
{
    if (connectTimeoutInMS < 0) {
        throw OTSClientException("ConnectTimeoutInMS cannot be less than 0.");
    }
    mConnectTimeoutInMS = connectTimeoutInMS;
}

int HttpConfig::GetConnectTimeoutInMS() const
{
    return mConnectTimeoutInMS;
}

void HttpConfig::SetRequestTimeoutInMS(int requestTimeoutInMS)
{
    if (requestTimeoutInMS < 0) {
        throw OTSClientException("RequestTimeoutInMS cannot be less than 0.");
    }
    mRequestTimeoutInMS = requestTimeoutInMS;
}

int HttpConfig::GetRequestTimeoutInMS() const
{
    return mRequestTimeoutInMS;
}

void HttpConfig::SetEnableKeepAlive(bool enableKeepAlive)
{
    mEnableKeepAlive = enableKeepAlive;
}

bool HttpConfig::GetEnableKeepAlive() const
{
    return mEnableKeepAlive;
}


// HttpClient
HttpClient::HttpClient(const HttpConfig& httpConfig)
    : mHttpConfig(httpConfig)
    , mTotalConnections(0)
{
    curl_global_init(CURL_GLOBAL_ALL);
    pthread_mutex_init(&mConnectionLock, NULL);
}

HttpClient::~HttpClient()
{
    typeof(mConnectionList.begin()) iter = mConnectionList.begin();
    for (; iter != mConnectionList.end(); ++iter) {
        delete *iter;
    }
    curl_global_cleanup();
}

HttpConnection* HttpClient::GetConnection()
{
    HttpConnection* httpConn = NULL;
    pthread_mutex_lock(&mConnectionLock);
    if (mHttpConfig.GetEnableKeepAlive()) {
        if (!mConnectionList.empty()) {
            // has available connections
            httpConn = mConnectionList.front();
            mConnectionList.pop_front();
            httpConn->Reset();
        } else {
            // no available connections
            httpConn = NewConnection();
        }
    } else {
        // create a connection everytime if not keeping alive
        httpConn = NewConnection();
    }
    pthread_mutex_unlock(&mConnectionLock);
    return httpConn;
}

void HttpClient::AddConnection(HttpConnection* httpConn)
{
    pthread_mutex_lock(&mConnectionLock);
    if (mHttpConfig.GetEnableKeepAlive()) {
        mConnectionList.push_front(httpConn);
        int halfConnections = mTotalConnections / 2;
        if (mConnectionList.size() > (size_t)halfConnections) {
            // free 1/4 connections
            for (int i = 0; i < halfConnections / 2; ++i) {
                HttpConnection* tmpConn = mConnectionList.back();
                mConnectionList.pop_back();
                FreeConnection(tmpConn);
            }
        }
    } else {
        // free connection everytime if not keeping alive
        FreeConnection(httpConn);
    }
    pthread_mutex_unlock(&mConnectionLock);
}

HttpConnection* HttpClient::NewConnection()
{
    if (mTotalConnections < mHttpConfig.GetMaxConnections()) {
        HttpConnection* httpConn = new HttpConnection(
            mHttpConfig.GetConnectTimeoutInMS(), mHttpConfig.GetRequestTimeoutInMS());
        ++mTotalConnections;
        return httpConn;
    }
    return NULL;
}

void HttpClient::FreeConnection(HttpConnection* httpConn)
{
    delete httpConn;
    --mTotalConnections;
}

} // end of tablestore
} // end of aliyun
