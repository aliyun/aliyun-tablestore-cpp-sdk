#ifndef HTTP_CURL_H
#define HTTP_CURL_H

#include "http_types.h"
#include <string>

struct curl_slist;
typedef void CURL;

namespace aliyun {
namespace openservices {
namespace ots {

class HttpCurl
{
public:
    // ctor: create HttpCurl, socket and connect timeout are required.
    HttpCurl(uint64_t timeout);

    // dtor: need to release the resource of HttpCurl
    ~HttpCurl();

    // Send http request synchronously
    void SendRequest(const HttpRequest& syncRequest, HttpResponse* syncResponse);

private:
    // Post method for HTTP request 
    int Post(const std::string& url,
                struct curl_slist* postHeader, 
                const std::string& postBody, 
                std::map<std::string, std::string> *respHeader,
                std::string* respBody);

    // Get method for HTTP request 
    int Get(const std::string& url,
                struct curl_slist* postHeader,
                std::map<std::string, std::string> *respHeader,
                std::string* respBody);

    // Set the common optiions for POST and GET, then perform and return CURLCode
    int SetThenPerform(const std::string& url,
                struct curl_slist* header, 
                std::map<std::string, std::string> *respHeader,
                std::string* respBody);

    // The static function will be called when CURLOPT_WRITEFUNCTION is set and
    // the request is responsed. It is used to receive the body data of response.
    static size_t ReceiveBody(char *data, size_t size, size_t nmemb, void *mp);

    // The static function will be called when CULROPT_HEADERFUNCTION is set and
    // the request. It is used to receive the response header, and parse it line 
    // by line. The data is the pointer of map<string, string>.
    static size_t ReceiveHeader(char *data, size_t size, size_t nmemb, void *mp);

private:
    CURL* mCurl;

    // Timeout duration in seconds
    int64_t mTimeout;
};

} //end of ots
} //end of openservices
} //end of aliyun

#endif
