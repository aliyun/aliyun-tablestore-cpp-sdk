#include "http_curl.h"
#include "common_util.h"

#include <cmath>
#include <cstdlib>
#include <iostream>

extern "C" {
#include <curl/curl.h>
}

using namespace std;
using namespace aliyun::openservices::ots;

HttpCurl::HttpCurl(uint64_t timeout)
    : mCurl(NULL), mTimeout(timeout)
{
    //initialize libcurl and set timtout time
    mCurl = curl_easy_init();
    curl_easy_setopt(mCurl, CURLOPT_TIMEOUT, mTimeout);
    curl_easy_setopt(mCurl, CURLOPT_NOSIGNAL, 1);
}

HttpCurl::~HttpCurl()
{
    if (mCurl != NULL) {
        curl_easy_cleanup(mCurl);
        mCurl = NULL;
    }
}

void HttpCurl::SendRequest(const HttpRequest& syncRequest, HttpResponse* syncResponse)
{
    //initialize response status
    syncResponse->mHttpStatus = 0;
    syncResponse->mResponseHeaders.clear();
    syncResponse->mResponseBody.clear();
    
    //add all header options to curl_slist
    struct curl_slist* headerList = NULL;     
    map<string, string>::const_iterator iter = syncRequest.mRequestHeaders.begin();
    for (; iter != syncRequest.mRequestHeaders.end(); ++iter) {
        string strParam;
        strParam.reserve((iter->first).size() + (iter->second).size() + 10);
        strParam.append(iter->first);
        strParam.append(":");
        strParam.append(iter->second);
        headerList = curl_slist_append(headerList, strParam.c_str());
    }

    int retCode = 0;
    if (syncRequest.mHttpMethod == HTTP_POST) {
        retCode = Post(syncRequest.mRequestUrl, headerList, syncRequest.mRequestBody, 
                    &syncResponse->mResponseHeaders, &syncResponse->mResponseBody); 
    } else {
        retCode = Get(syncRequest.mRequestUrl, headerList, &syncResponse->mResponseHeaders, 
                    &syncResponse->mResponseBody);
    }

    //free slist
    curl_slist_free_all(headerList);

    if (retCode != CURLE_OK) {
        // if request failed, return the negative error code which is defined by cURL.
        syncResponse->mHttpStatus = -abs(retCode);
    } else {
        retCode = curl_easy_getinfo(mCurl, CURLINFO_RESPONSE_CODE, &syncResponse->mHttpStatus);
        if (retCode != CURLE_OK) {
            // ditto
            syncResponse->mHttpStatus = -abs(retCode);
        }
    }
}

int HttpCurl::Post(const string& url,
            struct curl_slist* postHeader, 
            const string& postBody,
            map<string, string> *respHeader,
            string* respBody)
{
    curl_easy_setopt(mCurl, CURLOPT_POST, 1);
    curl_easy_setopt(mCurl, CURLOPT_POSTFIELDS, postBody.c_str());
    curl_easy_setopt(mCurl, CURLOPT_POSTFIELDSIZE_LARGE, postBody.size());
    return SetThenPerform(url, postHeader, respHeader, respBody);
}

int HttpCurl::Get(const string& url,
            struct curl_slist* getHeader, 
            map<string, string> *respHeader,
            string* respBody)
{
    curl_easy_setopt(mCurl, CURLOPT_HTTPGET, 1);
    return SetThenPerform(url, getHeader, respHeader, respBody);
}

int HttpCurl::SetThenPerform(const string& url,
            struct curl_slist* header, 
            map<string, string> *respHeader,
            string* respBody)
{
    curl_easy_setopt(mCurl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(mCurl, CURLOPT_HTTPHEADER, header);
    curl_easy_setopt(mCurl, CURLOPT_READFUNCTION, NULL);
    curl_easy_setopt(mCurl, CURLOPT_HEADERFUNCTION, ReceiveHeader);
    curl_easy_setopt(mCurl, CURLOPT_WRITEHEADER, (void *)respHeader);
    curl_easy_setopt(mCurl, CURLOPT_WRITEFUNCTION, ReceiveBody);
    curl_easy_setopt(mCurl, CURLOPT_WRITEDATA, (void *)respBody);
    CURLcode curlCode = curl_easy_perform(mCurl);
    return curlCode;
}

size_t HttpCurl::ReceiveBody(char *data, size_t size, size_t nmemb, void *mp)
{
    uint32_t totalSize = size * nmemb;
    string* respBody = static_cast<string*>(mp); 
    respBody->append(data, totalSize);
    return totalSize;
}

size_t HttpCurl::ReceiveHeader(char *data, size_t size, size_t nmemb, void *mp)
{
    uint32_t totalSize = size * nmemb;
    map<string, string>* headerMap = reinterpret_cast<map<string, string> *>(mp);
    string lineValue(data, totalSize);
    size_t pos = lineValue.find(':');
    if (pos != string::npos) {
        string key = lineValue.substr(0, pos);
        StringTrim(&key);
        string value = lineValue.substr(pos + 1);
        StringTrim(&value);
        (*headerMap)[key] = value;
    }
    return totalSize;
}

