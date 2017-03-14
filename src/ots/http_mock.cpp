#include "http_mock.h"

#include <iostream>
#include <stdlib.h>
#include <time.h>

using namespace std;
using namespace aliyun::openservices::ots;

HttpMock::HttpMock(uint64_t timeout)
    : mTimeout(timeout)
{
    //Nothing to init
    cout << "HttpMock is called" << endl;
}

HttpMock::~HttpMock()
{
    //Nothing to init
}

void HttpMock::SendRequest(const HttpRequest& syncRequest, HttpResponse *syncResponse)
{
    srand(time(0));
    int type = rand() % 99;
    syncResponse->mHttpStatus = -(type+1);
}

