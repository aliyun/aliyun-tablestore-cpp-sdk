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
#pragma once

#include "http_code.hpp"
#include "http_connection.hpp"
#include <list>
#include <pthread.h>

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

