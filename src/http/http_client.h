/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#ifndef HTTP_CLIENT_H
#define HTTP_CLIENT_H

#include "http_code.h"
#include "http_connection.h"
#include <pthread.h>
#include <list>

namespace aliyun {
namespace tablestore {

class HttpConfig
{
public:

    HttpConfig();

    void SetMinConnections(int minConnections);

    int GetMinConnections() const;

    void SetMaxConnections(int maxConnections);

    int GetMaxConnections() const;

    void SetConnectTimeoutInMS(int connectTimeoutInMS);

    int GetConnectTimeoutInMS() const;

    void SetRequestTimeoutInMS(int requestTimeoutInMS);

    int GetRequestTimeoutInMS() const;

    void SetEnableKeepAlive(bool enableKeepAlive);

    bool GetEnableKeepAlive() const;

private:

    int mMaxConnections;
    int mConnectTimeoutInMS;
    int mRequestTimeoutInMS;
    bool mEnableKeepAlive;
};

class HttpClient
{
public:

    HttpClient(const HttpConfig& httpConfig);

    ~HttpClient();

    HttpConnection* GetConnection();

    void AddConnection(HttpConnection* httpConn);

private:

    HttpConnection* NewConnection();

    void FreeConnection(HttpConnection* httpConn);

private:

    HttpConfig mHttpConfig;

    pthread_mutex_t mConnectionLock;
    std::list<HttpConnection*> mConnectionList; 
    int mTotalConnections;
};

} // end of tablestore
} // end of aliyun

#endif 
