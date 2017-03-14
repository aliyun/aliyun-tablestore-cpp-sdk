/**
 * HttpClient: realize the basic function of http client, including sending request
 * and receving response via POST or GET.
 */

#ifndef HTTP_CLIENT_H
#define HTTP_CLIENT_H

#include "http_curl.h"
#include "http_mock.h"
#include "http_types.h"

#include <map>
#include <tr1/memory>
#include <string>
#include <sys/types.h>

//#define HTTP_MOCK_TEST

namespace aliyun {
namespace openservices {
namespace ots {

//HttpClient provides the interface to send http request and get http response.
//Meanwhile, it needs configuration like connection timeout, retry strategy, url,
class HttpClient
{
public:
    //ctor: The unit of timeout is second(s); retryStrategy is NULL by default.
    //@param timeout The timeout duration of the request, 30s by default
    HttpClient(uint64_t timeout = 30);

    //dtor: release HttpWrapper and RetryStrategy
    ~HttpClient();

    //Send http request synchronically.
    //For HttpException, it applys NoRetryStrategy by default
    void SendRequest(const HttpRequest& syncRequest, HttpResponse *syncResponse);

private:
    //HttpWrapper is generated for mocking http.
    void* mHttpWrapper;
};
typedef std::tr1::shared_ptr<HttpClient> HttpClientPtr;

} //end of ots
} //end of openservices
} //end of aliyun

#endif 
