/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#ifndef HTTP_CONNECTION_H
#define HTTP_CONNECTION_H

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

#endif
