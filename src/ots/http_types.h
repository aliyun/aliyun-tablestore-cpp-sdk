#ifndef HTTP_TYPES_H
#define HTTP_TYPES_H

#include <map>
#include <string>
#include <stdint.h>

namespace aliyun {
namespace openservices {
namespace ots {

/**
 * Two methods for http request: GET and POST.
 */
enum HttpMethod
{
    HTTP_GET = 0,
    HTTP_POST = 1
};

/**
 * HttpRequest is the first parameter for SendRequest. The URL, method name, 
 * HTTP header, HTTP body are required. 
 */
struct HttpRequest
{
    HttpMethod mHttpMethod;
    std::string mRequestUrl;
    std::map<std::string, std::string> mRequestHeaders;
    std::string mRequestBody;
};

/**
 * HttpResponse is the second parameter for SendRequest. The error code and error 
 * message will be returned for every operations. And data and size are required
 * for operations which receive response data. Of course, the receive flag is needed.
 */
struct HttpResponse
{
    long mHttpStatus;
    std::map<std::string, std::string> mResponseHeaders;
    std::string mResponseBody;
};

} //end of ots
} //end of openservices
} //end of aliyun

#endif 
