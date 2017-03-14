#include "http_client.h"

using namespace std;
using namespace aliyun::openservices::ots;

HttpClient::HttpClient(uint64_t timeout)
    :mHttpWrapper(NULL)
{
    //First, create HttpMock (for testing) or HttpCurl
#ifdef HTTP_MOCK_TEST
    HttpMock* tmpMock = new HttpMock(timeout);
    mHttpWrapper = static_cast<void*>(tmpMock);    
#else
    HttpCurl* tmpCurl = new HttpCurl(timeout);
    mHttpWrapper = static_cast<void*>(tmpCurl);
#endif
}

HttpClient::~HttpClient()
{
#ifdef HTTP_MOCK_TEST
    delete (HttpMock*)mHttpWrapper;
#else
    delete (HttpCurl*)mHttpWrapper;
#endif
}

void HttpClient::SendRequest(const HttpRequest& syncRequest, HttpResponse *syncResponse)
{
#ifdef HTTP_MOCK_TEST
    ((HttpMock*)mHttpWrapper)->SendRequest(syncRequest, syncResponse);
#else
    ((HttpCurl*)mHttpWrapper)->SendRequest(syncRequest, syncResponse);
#endif
}

