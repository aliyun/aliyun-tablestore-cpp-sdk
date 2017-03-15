/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#include "ots_helper.h"
#include "ots/ots_exception.h"

using namespace std;

namespace aliyun {
namespace tablestore {

// OTSException

OTSException::OTSException(
    const string& errrorCode,
    const string& message,
    const string& requestId,
    const string& traceId,
    int32_t httpStatus)
    : mErrorCode(errrorCode)
    , mMessage(message)
    , mRequestId(requestId)
    , mTraceId(traceId)
    , mHttpStatus(httpStatus)
{
    mWhat = "ErrorCode: " + mErrorCode + 
        ", Message: " + mMessage + 
        ", RequestId: " + mRequestId + 
        ", TraceId: " + mTraceId + 
        ", HttpStatus: " + OTSHelper::Int64ToString(mHttpStatus);
}

OTSException::~OTSException() throw()
{
}

const char* OTSException::what() const throw()
{
    return mWhat.c_str();
}

string OTSException::GetErrorCode() const
{
    return mErrorCode;
}

string OTSException::GetMessage() const
{
    return mMessage;
}

string OTSException::GetRequestId() const
{
    return mRequestId;
}

string OTSException::GetTraceId() const
{
    return mTraceId;
}

int32_t OTSException::GetHttpStatus() const
{
    return mHttpStatus;
}

// OTSClientException

OTSClientException::OTSClientException(const string& message)
    : mMessage(message)
{
    mWhat = "Message: " + mMessage;
}

OTSClientException::OTSClientException(
    const string& message,
    const string& traceId)
    : mMessage(message)
    , mTraceId(traceId)
{
    mWhat = "Message: " + mMessage;
    if (!mTraceId.empty()) {
        mWhat += ", TraceId: " + mTraceId;
    }
}

OTSClientException::~OTSClientException() throw()
{
}

const char* OTSClientException::what() const throw()
{
    return mWhat.c_str();
}

string OTSClientException::GetMessage() const
{
    return mMessage;
}

string OTSClientException::GetTraceId() const
{
    return mTraceId;
}

} // end of tablestore
} // end of aliyun
