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

#include <curl/curl.h>
#include <sys/types.h>
#include <string>
#include <map>

namespace aliyun {
namespace tablestore {

typedef std::map<std::string, std::string> HeadersMap;

enum HttpMethod
{
    HTTP_GET,
    HTTP_POST
};

class ConnectionState
{
public:

    std::string mTargetIP;
    int64_t mRequestNum;

    ConnectionState()
        : mRequestNum(0) {}
};

class HttpConnection
{
public:

    HttpConnection(
        int connectTimeoutInMS,
        int requestTimeoutInMS);

    ~HttpConnection();

    void Reset();

    void SetURL(const std::string& url);

    void SetHttpMethod(HttpMethod httpMethod);

    HttpMethod GetHttpMethod() const;

    void AddRequestHeader(const std::string& key, const std::string& value);

    void SetRequestHeaders(const HeadersMap& requestHeaders);

    void SetRequestBody(const std::string& requestBody);

    const HeadersMap& GetRequestHeaders() const;

    const std::string& GetRequestBody() const;

    int GetResponseStatus() const;

    const std::string& GetCurlMessage() const;

    const HeadersMap& GetResponseHeaders() const;

    const std::string& GetResponseBody() const;

    void SetConnectionState(const ConnectionState& connState);

    const ConnectionState& GetConnectionState() const;

    int SendRequest();

private:

    static size_t ReceiveBody(char *data, size_t size, size_t nmemb, void *mp);

    static size_t ReceiveHeader(char *data, size_t size, size_t nmemb, void *mp);

    static void StringTrim(std::string* value);

private:

    int mConnectTimeoutInMS;
    int mRequestTimeoutInMS;
    ConnectionState mConnectionState;

    CURL* mCurlHandle;
    struct curl_slist* mHeaderList;

    int mResponseStatus;
    HeadersMap mRequestHeaders;
    HeadersMap mResponseHeaders;
    std::string mRequestURL;
    std::string mRequestBody;
    std::string mResponseBody;
    std::string mCurlMessage;

    HttpMethod mHttpMethod;
};

} // end of tablestore
} // end of aliyun

