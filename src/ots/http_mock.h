#ifndef HTTP_MOCK_H
#define HTTP_MOCK_H

#include "http_types.h"

#include <string>
#include <sys/types.h>

namespace aliyun {
namespace openservices {
namespace ots {

class HttpMock
{
public:
    //ctor: create HttpMock, timeout are required.
    HttpMock(uint64_t timeout);

    //dtor: need to release the resource of HttpMock
    ~HttpMock();

    //Send http request synchronically 
    void SendRequest(const HttpRequest& syncRequest, HttpResponse *syncResponse);

private:
    uint64_t mTimeout;
};

} //end of ots
} //end of openservices
} //end of aliyun

#endif
