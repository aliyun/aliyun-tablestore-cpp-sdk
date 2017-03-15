/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#include "http_connection.h"
#include <curl/curlver.h>
#include <stdlib.h>

using namespace std;

#if LIBCURL_VERSION_MAJOR > 7 || (LIBCURL_VERSION_MAJOR == 7 && LIBCURL_VERSION_MINOR >= 17)
#define SET_TIMEOUT_OPTION(handle, connectTimeout, requestTimeout) \
    curl_easy_setopt(handle, CURLOPT_CONNECTTIMEOUT_MS, connectTimeout); \
    curl_easy_setopt(handle, CURLOPT_TIMEOUT_MS, requestTimeout);
#else
#define SET_TIMEOUT_OPTION(handle, connectTimeout, requestTimeout) \
    int tmpConnectTimeout = (connectTimeout + 999) / 1000; \
    curl_easy_setopt(handle, CURLOPT_CONNECTTIMEOUT, tmpConnectTimeout); \
    int tmpRequestTimeout = (requestTimeout + 999) / 1000; \
    curl_easy_setopt(handle, CURLOPT_TIMEOUT, tmpRequestTimeout);
#endif

namespace aliyun {
namespace tablestore {

HttpConnection::HttpConnection(
    int connectTimeoutInMS,
    int requestTimeoutInMS)
    : mConnectTimeoutInMS(connectTimeoutInMS)
    , mRequestTimeoutInMS(requestTimeoutInMS)
    , mCurlHandle(NULL)
    , mHeaderList(NULL)
    , mResponseStatus(0)
{
    mCurlHandle = curl_easy_init();
    curl_easy_setopt(mCurlHandle, CURLOPT_NOSIGNAL, 1);
    SetHttpMethod(HTTP_POST);
    SET_TIMEOUT_OPTION(mCurlHandle, mConnectTimeoutInMS, mRequestTimeoutInMS);
}

HttpConnection::~HttpConnection()
{
    if (mHeaderList != NULL) {
        curl_slist_free_all(mHeaderList);
    }
    if (mCurlHandle != NULL) {
        curl_easy_cleanup(mCurlHandle);
    }
}

void HttpConnection::Reset()
{
    mResponseStatus = 0;
    mRequestHeaders.clear();
    mResponseHeaders.clear();
    mRequestURL.clear();
    mRequestBody.clear();
    mResponseBody.clear();
    mCurlMessage.clear();

    curl_slist_free_all(mHeaderList);
    mHeaderList = NULL;
    curl_easy_reset(mCurlHandle);
    curl_easy_setopt(mCurlHandle, CURLOPT_NOSIGNAL, 1);
    SetHttpMethod(HTTP_POST);
    SET_TIMEOUT_OPTION(mCurlHandle, mConnectTimeoutInMS, mRequestTimeoutInMS);
}

void HttpConnection::SetURL(const std::string& url)
{
    mRequestURL = url;
    curl_easy_setopt(mCurlHandle, CURLOPT_URL, mRequestURL.c_str());
}

void HttpConnection::SetHttpMethod(HttpMethod httpMethod)
{
    if (httpMethod == HTTP_GET) {
        curl_easy_setopt(mCurlHandle, CURLOPT_HTTPGET, 1);
    } else {
        curl_easy_setopt(mCurlHandle, CURLOPT_POST, 1);
    }
    mHttpMethod = httpMethod;
}

HttpMethod HttpConnection::GetHttpMethod() const
{
    return mHttpMethod;
}

void HttpConnection::AddRequestHeader(const string& key, const string& value)
{
    mRequestHeaders[key] = value;
    string strParam = key + ":" + value;
    mHeaderList = curl_slist_append(mHeaderList, strParam.c_str());
}

void HttpConnection::SetRequestHeaders(const HeadersMap& requestHeaders)
{
    //mHeaderList = curl_slist_append(mHeaderList, "Connection:Keep-Alive");
    typeof(requestHeaders.begin()) iter = requestHeaders.begin();
    for (; iter != requestHeaders.end(); ++iter) {
        string strParam = iter->first + ":" + iter->second;
        mHeaderList = curl_slist_append(mHeaderList, strParam.c_str());
    }
}

void HttpConnection::SetRequestBody(const std::string& requestBody)
{
    mRequestBody = requestBody;
    curl_easy_setopt(mCurlHandle, CURLOPT_POSTFIELDS, mRequestBody.c_str());
    curl_easy_setopt(mCurlHandle, CURLOPT_POSTFIELDSIZE_LARGE, mRequestBody.size());
}

const HeadersMap& HttpConnection::GetRequestHeaders() const
{
    return mRequestHeaders;
}

const string& HttpConnection::GetRequestBody() const
{
    return mRequestBody;
}

int HttpConnection::GetResponseStatus() const
{
    return mResponseStatus;
}

const string& HttpConnection::GetCurlMessage() const
{
    return mCurlMessage;
}

const HeadersMap& HttpConnection::GetResponseHeaders() const
{
    return mResponseHeaders;
}

const std::string& HttpConnection::GetResponseBody() const
{
    return mResponseBody;
}

void HttpConnection::SetConnectionState(const ConnectionState& connState)
{
    mConnectionState = connState;
}

const ConnectionState& HttpConnection::GetConnectionState() const
{
    return mConnectionState;
}

int HttpConnection::SendRequest()
{
    mResponseHeaders.clear();
    mResponseBody.clear();

    curl_easy_setopt(mCurlHandle, CURLOPT_HTTPHEADER, mHeaderList);
    curl_easy_setopt(mCurlHandle, CURLOPT_READFUNCTION, NULL);
    curl_easy_setopt(mCurlHandle, CURLOPT_HEADERFUNCTION, ReceiveHeader);
    curl_easy_setopt(mCurlHandle, CURLOPT_WRITEHEADER, (void *)&mResponseHeaders);
    curl_easy_setopt(mCurlHandle, CURLOPT_WRITEFUNCTION, ReceiveBody);
    curl_easy_setopt(mCurlHandle, CURLOPT_WRITEDATA, (void *)&mResponseBody);

    CURLcode curlCode = curl_easy_perform(mCurlHandle);
    if (curlCode != CURLE_OK) {
        mResponseStatus = curlCode;
        mCurlMessage = curl_easy_strerror(curlCode);
    } else {
        curlCode = curl_easy_getinfo(mCurlHandle, CURLINFO_RESPONSE_CODE, &mResponseStatus);
        if (curlCode != CURLE_OK) {
            mResponseStatus = curlCode;
            mCurlMessage = curl_easy_strerror(curlCode);
        }
    }
    return mResponseStatus;
}

size_t HttpConnection::ReceiveBody(char *data, size_t size, size_t nmemb, void *mp)
{
    size_t totalSize = size * nmemb;
    string* responseBody = static_cast<string*>(mp); 
    responseBody->append(data, totalSize);
    return totalSize;
}

size_t HttpConnection::ReceiveHeader(char *data, size_t size, size_t nmemb, void *mp)
{
    size_t totalSize = size * nmemb;
    map<string, string>* responseHeaders = reinterpret_cast<map<string, string> *>(mp);
    string lineValue(data, totalSize);
    size_t pos = lineValue.find(":");
    if (pos != string::npos) {
        string key = lineValue.substr(0, pos);
        StringTrim(&key);
        string value = lineValue.substr(pos + 1);
        StringTrim(&value);
        (*responseHeaders)[key] = value;
    }
    return totalSize;
}

void HttpConnection::StringTrim(std::string* value)
{
    if (value != NULL) {
        value->erase(0, value->find_first_not_of(" \r\n"));
        value->erase(value->find_last_not_of(" \r\n") + 1);
    }
}

} // end of tablestore
} // end of aliyun
