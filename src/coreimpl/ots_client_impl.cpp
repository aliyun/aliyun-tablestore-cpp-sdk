/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#include "ots_client_impl.h"
#include "ots_protocol_builder.h"
#include "ots_row_iterator.h"
#include "ots_constants.h"
#include "ots_helper.h"
#include "default_retry_strategy.h"
#include <math.h>
#include <iostream>
#include <unistd.h>

using namespace std;
using namespace std::tr1;
using namespace com::aliyun::tablestore;
using namespace google::protobuf;

namespace aliyun {
namespace tablestore {

namespace {

class ScopedConnection
{
public:
    explicit ScopedConnection(
        HttpClient* httpClient,
        HttpConnection* httpConn);

    ~ScopedConnection();

private:
    HttpClient* mHttpClient;
    HttpConnection* mHttpConnection;
};

ScopedConnection::ScopedConnection(
    HttpClient* httpClient,
    HttpConnection* httpConn)
    : mHttpClient(httpClient)
    , mHttpConnection(httpConn)
{
}

ScopedConnection::~ScopedConnection()
{
    mHttpClient->AddConnection(mHttpConnection);
}

} // namespace

OTSClientImpl::OTSClientImpl(
    const string& endpoint,
    const string& instanceName,
    const Credential& auth,
    const ClientConfiguration& clientConfig)
  : mEndpoint(endpoint),
    mAuth(auth),
    mInstanceName(instanceName),
    mClientConfig(clientConfig)
{
    Initialize();
}

namespace {

inline bool IsBlank(char c)
{
    return (c == ' ' || c == '\t' || c == '\n' || c == '\r');
}

inline string Trim(const string& s)
{
    const char* b = s.data();
    const char* e = b + s.size();
    for(; b < e && IsBlank(*b); ++b) {
    }
    for(; b < e && IsBlank(*(e - 1)); --e) {
    }
    if (b == s.data() && e == s.data() + s.size()) {
        return s;
    } else {
        return string(b, e - b);
    }
}

inline void ValidateCrlfAttack(const string& s, const string& msg)
{
    if (s.find('\n') != string::npos || s.find('\r') != string::npos) {
        // defense CRLF attack
        throw OTSClientException(msg);
    }
}

inline void ValidateAuth(const string& s, const string& msg)
{
    if (s.empty()) {
        throw OTSClientException(msg);
    }
    ValidateCrlfAttack(s, msg);
}

} // namespace

void OTSClientImpl::Initialize()
{
    srand(time(NULL));

    vector<string> endpointFields;
    OTSHelper::ParseEndpoint(mEndpoint, &endpointFields);
    if (endpointFields.empty()) {
        throw OTSClientException("Invalid Endpoint.");
    }
    mProtocol = endpointFields[0];
    mAddress = endpointFields[1];
    mPort = endpointFields[2];
    mEndpoint = mProtocol + "://" + mAddress + ":" + mPort;

    mAuth.mAccessKeyId = Trim(mAuth.mAccessKeyId);
    mAuth.mAccessKeySecret = Trim(mAuth.mAccessKeySecret);
    mAuth.mStsToken = Trim(mAuth.mStsToken);
    ValidateAuth(mAuth.mAccessKeyId, "Invalid AccessKeyId.");
    ValidateAuth(mAuth.mAccessKeySecret, "Invalid AccessKeySecret.");
    ValidateCrlfAttack(mAuth.mStsToken, "Invalid STS Token.");

    // check instanceName
    if (mInstanceName.empty()) {
        throw OTSClientException("Invalid InstanceName.");
    }

    // config for retrying
    if (mClientConfig.mRetryMaxTimes < 0) {
        throw OTSClientException("RetryMaxTimes cannot be less than 0.");
    }
    if (mClientConfig.mRetryIntervalInMS < 0) {
        throw OTSClientException("RetryInternvalInMS cannot be less than 0.");
    }
    if (mClientConfig.mRetryStrategy.get() == NULL) {
        mClientConfig.mRetryStrategy.reset(new DefaultRetryStrategy(
            mClientConfig.mRetryMaxTimes, mClientConfig.mRetryIntervalInMS));
    }

    // config for http client
    HttpConfig httpConfig;
    httpConfig.SetMaxConnections(mClientConfig.mMaxConnections);
    httpConfig.SetConnectTimeoutInMS(mClientConfig.mConnectTimeoutInMS);
    httpConfig.SetRequestTimeoutInMS(mClientConfig.mRequestTimeoutInMS);
    httpConfig.SetEnableKeepAlive(mClientConfig.mEnableKeepAlive);
    mHttpClient.reset(new HttpClient(httpConfig));
}

OTSClientImpl::~OTSClientImpl()
{
}

CreateTableResultPtr OTSClientImpl::CreateTable(const CreateTableRequestPtr& requestPtr)
    throw (OTSException, OTSClientException)
{
    RequestContext<CreateTableRequestPtr, CreateTableResultPtr> requestContext;
    requestContext.mRequestType = kAPICreateTable;
    requestContext.mRequestPtr = requestPtr;
    HandleRequest(requestContext);
    return requestContext.mResultPtr;
}

ListTableResultPtr OTSClientImpl::ListTable()
    throw (OTSException, OTSClientException)
{
    RequestContext<ListTableRequestPtr, ListTableResultPtr> requestContext;
    requestContext.mRequestType = kAPIListTable;
    HandleRequest(requestContext);
    return requestContext.mResultPtr;
}

DescribeTableResultPtr OTSClientImpl::DescribeTable(const DescribeTableRequestPtr& requestPtr)
    throw (OTSException, OTSClientException)
{
    RequestContext<DescribeTableRequestPtr, DescribeTableResultPtr> requestContext;
    requestContext.mRequestType = kAPIDescribeTable;
    requestContext.mRequestPtr = requestPtr;
    HandleRequest(requestContext);
    return requestContext.mResultPtr;
}

DeleteTableResultPtr OTSClientImpl::DeleteTable(const DeleteTableRequestPtr& requestPtr)
    throw (OTSException, OTSClientException)
{
    RequestContext<DeleteTableRequestPtr, DeleteTableResultPtr> requestContext;
    requestContext.mRequestType = kAPIDeleteTable;
    requestContext.mRequestPtr = requestPtr;
    HandleRequest(requestContext);
    return requestContext.mResultPtr;
}

UpdateTableResultPtr OTSClientImpl::UpdateTable(const UpdateTableRequestPtr& requestPtr)
    throw (OTSException, OTSClientException)
{
    RequestContext<UpdateTableRequestPtr, UpdateTableResultPtr> requestContext;
    requestContext.mRequestType = kAPIUpdateTable;
    requestContext.mRequestPtr = requestPtr;
    HandleRequest(requestContext);
    return requestContext.mResultPtr;
}

GetRowResultPtr OTSClientImpl::GetRow(const GetRowRequestPtr& requestPtr)
    throw (OTSException, OTSClientException)
{
    RequestContext<GetRowRequestPtr, GetRowResultPtr> requestContext;
    requestContext.mRequestType = kAPIGetRow;
    requestContext.mRequestPtr = requestPtr;
    HandleRequest(requestContext);
    return requestContext.mResultPtr;
}

PutRowResultPtr OTSClientImpl::PutRow(const PutRowRequestPtr& requestPtr)
    throw (OTSException, OTSClientException)
{
    RequestContext<PutRowRequestPtr, PutRowResultPtr> requestContext;
    requestContext.mRequestType = kAPIPutRow;
    requestContext.mRequestPtr = requestPtr;
    HandleRequest(requestContext);
    return requestContext.mResultPtr;
}

UpdateRowResultPtr OTSClientImpl::UpdateRow(const UpdateRowRequestPtr& requestPtr)
    throw (OTSException, OTSClientException)
{
    RequestContext<UpdateRowRequestPtr, UpdateRowResultPtr> requestContext;
    requestContext.mRequestType = kAPIUpdateRow;
    requestContext.mRequestPtr = requestPtr;
    HandleRequest(requestContext);
    return requestContext.mResultPtr;
}

DeleteRowResultPtr OTSClientImpl::DeleteRow(const DeleteRowRequestPtr& requestPtr)
    throw (OTSException, OTSClientException)
{
    RequestContext<DeleteRowRequestPtr, DeleteRowResultPtr> requestContext;
    requestContext.mRequestType = kAPIDeleteRow;
    requestContext.mRequestPtr = requestPtr;
    HandleRequest(requestContext);
    return requestContext.mResultPtr;
}

BatchGetRowResultPtr OTSClientImpl::BatchGetRow(const BatchGetRowRequestPtr& requestPtr)
    throw (OTSException, OTSClientException)
{
    RequestContext<BatchGetRowRequestPtr, BatchGetRowResultPtr> requestContext;
    requestContext.mRequestType = kAPIBatchGetRow;
    requestContext.mRequestPtr = requestPtr;
    HandleRequest(requestContext);
    return requestContext.mResultPtr;
}

BatchWriteRowResultPtr OTSClientImpl::BatchWriteRow(const BatchWriteRowRequestPtr& requestPtr)
    throw (OTSException, OTSClientException)
{
    RequestContext<BatchWriteRowRequestPtr, BatchWriteRowResultPtr> requestContext;
    requestContext.mRequestType = kAPIBatchWriteRow;
    requestContext.mRequestPtr = requestPtr;
    HandleRequest(requestContext);
    return requestContext.mResultPtr;
}

GetRangeResultPtr OTSClientImpl::GetRange(const GetRangeRequestPtr& requestPtr)
    throw (OTSException, OTSClientException)
{
    RequestContext<GetRangeRequestPtr, GetRangeResultPtr> requestContext;
    requestContext.mRequestType = kAPIGetRange;
    requestContext.mRequestPtr = requestPtr;
    HandleRequest(requestContext);
    return requestContext.mResultPtr;
}

RowIteratorPtr OTSClientImpl::GetRangeIterator(const GetRangeRequestPtr& requestPtr)
    throw (OTSException, OTSClientException)
{
    RowIteratorPtr rowIteratorPtr(new OTSRowIterator(requestPtr, (void*)this));
    return rowIteratorPtr;
}

ComputeSplitsBySizeResultPtr OTSClientImpl::ComputeSplitsBySize(const ComputeSplitsBySizeRequestPtr& requestPtr)
    throw (OTSException, OTSClientException)
{
    RequestContext<ComputeSplitsBySizeRequestPtr, ComputeSplitsBySizeResultPtr> requestContext;
    requestContext.mRequestType = kAPIComputeSplitPointsBySize;
    requestContext.mRequestPtr = requestPtr;
    HandleRequest(requestContext);
    return requestContext.mResultPtr;
}

template<typename RequestPtr, typename ResultPtr>
void OTSClientImpl::HandleRequest(RequestContext<RequestPtr, ResultPtr>& context)
{
    // Acquire one connection.
    context.mHttpConnection = TryWaitConnection();
    ScopedConnection scopedConn(mHttpClient.get(), context.mHttpConnection);
    context.mRequestURL = mEndpoint + "/" + context.mRequestType;
    context.mHttpConnection->SetURL(context.mRequestURL);
    // convert request to protobuf request
    OTSProtocolBuilder::BuildProtobufRequest(context.mRequestPtr, &context.mPBRequestPtr);

    bool shouldRetry = false;
    do {
        PreProcessInternal(context); 
        ProcessInternal(context);

        // retry according to http status and errors
        vector<Error> requestErrors;
        bool isBatchOperation = IsBatchOperation(context.mRequestType);
        if (context.mResponseInfo.mStatus == HTTP_OK && isBatchOperation) {
            OTSProtocolBuilder::MergeBatchResponse(
                context.mRequestType,
                context.mInitPBRequestPtr,
                context.mLastPBResponsePtr,
                context.mPBRequestPtr,
                context.mPBResponsePtr,
                &requestErrors);
            context.mSkipSerializeBody = false;
        } else {
            Error error;
            error.SetCode(context.mResponseInfo.mErrorCode);
            error.SetMessage(context.mResponseInfo.mErrorMessage);
            requestErrors.push_back(error);
        }
        context.mRetryCount++;
        shouldRetry = mClientConfig.mRetryStrategy->ShouldRetry(
            context.mRequestType,
            requestErrors,
            context.mResponseInfo.mStatus,
            context.mRetryCount);
        if (shouldRetry) {
            int64_t pauseDelayTime = mClientConfig.mRetryStrategy->GetPauseDelay(
                context.mRequestType,
                requestErrors,
                context.mResponseInfo.mStatus,
                context.mRetryCount);
            usleep(pauseDelayTime * 1000); //ms -> us
        }
    } while (shouldRetry);

    FinishProcessInternal(context);
}

template<typename RequestPtr, typename ResultPtr>
void OTSClientImpl::PreProcessInternal(RequestContext<RequestPtr, ResultPtr>& context)
{
    if (!context.mSkipSerializeBody) {
        // Generate request body
        if (!context.mPBRequestPtr->SerializeToString(&context.mRequestBody)) {
            throw OTSClientException("Required fields are missing.");
        }
        context.mHttpConnection->SetRequestBody(context.mRequestBody);
        context.mSkipSerializeBody = true;

        // Calculate MD5 of request body.
        string httpBodyMD5;
        context.mBodyMD5Base64.clear();
        OTSHelper::MD5String(context.mRequestBody, &httpBodyMD5);
        OTSHelper::Base64Encode(httpBodyMD5, &context.mBodyMD5Base64);
        context.mHttpConnection->AddRequestHeader(kOTSContentMD5, context.mBodyMD5Base64);
    }

    // Set http header options.
    string ios8601TimeStr = OTSHelper::ISO8601TimeString();
    context.mHttpConnection->AddRequestHeader(kOTSDate, ios8601TimeStr);
    context.mHttpConnection->AddRequestHeader(kOTSAPIVersion, kAPIVersion);
    context.mHttpConnection->AddRequestHeader(kOTSAccessKeyId, mAuth.mAccessKeyId);
    if (!mAuth.mStsToken.empty()) {
        context.mHttpConnection->AddRequestHeader(kOTSStsToken, mAuth.mStsToken);
    }
    context.mHttpConnection->AddRequestHeader(kOTSInstanceName, mInstanceName);

    if (context.mResponseInfo.mTraceId.empty()) {
        context.mResponseInfo.mTraceId = OTSHelper::UUIDString();
    }
    context.mHttpConnection->AddRequestHeader(kOTSTraceId, context.mResponseInfo.mTraceId);
    context.mHttpConnection->AddRequestHeader(kUserAgent, kSDKUserAgent);

    // Generate the signature.
    CreateSignature(context.mRequestType, context.mHttpConnection->GetHttpMethod(),
                    context.mHttpConnection->GetRequestHeaders(), 
                    &context.mSignature);
    context.mHttpConnection->AddRequestHeader(kOTSSignature, context.mSignature);

    context.mProfiling.KeepTimeWithState(kProfPreProcess);
}

namespace {

class InnerError
{
public:
    enum ErrorLevel
    {
        NORMAL, // no error at all
        HEADER_READY_ERROR, // all required headers are ready in response
        HEADER_MISSING_ERROR, // at least one of required headers is missing
    };

    ErrorLevel mError;
    string mMessage;

    InnerError()
      : mError(NORMAL)
    {}

    explicit InnerError(ErrorLevel lvl, const string& msg)
      : mError(lvl),
        mMessage(msg)
    {}
};

#define OTS_TRY(result) \
    do { \
        InnerError err = (result); \
        if (err.mError != InnerError::NORMAL) { \
            return err; \
        } \
    } while(false)

inline bool IsSuccessfulResponse(int64_t httpStatus)
{
    return (httpStatus >= 200 && httpStatus < 300);
}

template<typename RequestPtr, typename ResultPtr>
InnerError ParseRequestId(
    RequestContext<RequestPtr, ResultPtr>& context,
    const HeadersMap& headers)
{
    static const char kErrMsg[] = "No x-ots-requestid in response header.";
    
    HeadersMap::const_iterator iter = headers.find(kOTSRequestId);
    if (iter == headers.end()) {
        return InnerError(InnerError::HEADER_MISSING_ERROR, kErrMsg);
    }
    context.mResponseInfo.mRequestId = iter->second;
    return InnerError();
}

template<typename RequestPtr, typename ResultPtr>
InnerError ValidateContent(
    RequestContext<RequestPtr, ResultPtr>& context,
    const HeadersMap& headers)
{
    static const char kErrNoContentMd5[] = "No x-ots-contentmd5 in response header.";
    static const char kErrContentCorruption[] = "Response content MD5 mismatch.";

    HeadersMap::const_iterator iter = headers.find(kOTSContentMD5);
    if (iter == headers.end()) {
        return InnerError(InnerError::HEADER_MISSING_ERROR, kErrNoContentMd5);
    }
    string respContentMD5 = iter->second;

    string httpBodyMD5;
    string bodyMD5Base64;
    OTSHelper::MD5String(context.mHttpConnection->GetResponseBody(), &httpBodyMD5);
    OTSHelper::Base64Encode(httpBodyMD5, &bodyMD5Base64);
    if (respContentMD5 != bodyMD5Base64) {
        return InnerError(InnerError::HEADER_READY_ERROR, kErrContentCorruption);
    }
    return InnerError();
}

template<typename RequestPtr, typename ResultPtr>
void LogTraceInfo(
    RequestContext<RequestPtr, ResultPtr>& context,
    const HeadersMap& headers)
{
}

template<typename RequestPtr, typename ResultPtr>
InnerError ParseSuccessfulResponse(
    RequestContext<RequestPtr, ResultPtr>& context,
    const HeadersMap& headers,
    bool enableResponseMd5)
{
    OTS_TRY(ParseRequestId(context, headers));
    if (enableResponseMd5) {
        OTS_TRY(ValidateContent(context, headers));
    }
    LogTraceInfo(context, headers);

    OTSProtocolBuilder::ParseProtobufResponse(
        context.mHttpConnection->GetResponseBody(),
        &context.mResultPtr,
        &context.mPBResponsePtr); // may throw

    return InnerError();
}

template<typename RequestPtr, typename ResultPtr>
InnerError ParseErroneousResponse(
    RequestContext<RequestPtr, ResultPtr>& context,
    const HeadersMap& headers,
    bool enableResponseMd5)
{
    OTS_TRY(ParseRequestId(context, headers));
    if (enableResponseMd5) {
        OTS_TRY(ValidateContent(context, headers));
    }
    LogTraceInfo(context, headers);

    Error error;
    OTSProtocolBuilder::ParseErrorResponse(
        context.mHttpConnection->GetResponseBody(),
        &error,
        &context.mPBResponsePtr); // may throw
    context.mResponseInfo.mErrorCode = error.GetCode(); 
    context.mResponseInfo.mErrorMessage = error.GetMessage(); 

    return InnerError();
}

#undef OTS_TRY
} // namespace

template<typename RequestPtr, typename ResultPtr>
void OTSClientImpl::ProcessInternal(RequestContext<RequestPtr, ResultPtr>& context)
{
    // send request
    context.mResponseInfo.mStatus = context.mHttpConnection->SendRequest(); 
    const HeadersMap& responseHeaders = context.mHttpConnection->GetResponseHeaders();
    context.mProfiling.KeepTimeWithState(kProfSendRequest);

    // output original response data.
    if (IsSuccessfulResponse(context.mResponseInfo.mStatus)) {
        InnerError err = ParseSuccessfulResponse(
            context,
            responseHeaders,
            mClientConfig.mEnableCheckResponseMD5);
        if (err.mError != InnerError::NORMAL) {
            throw OTSClientException(err.mMessage);
        }
    } else {
        InnerError err = ParseErroneousResponse(
            context,
            responseHeaders,
            mClientConfig.mEnableCheckResponseMD5);
        switch(err.mError) {
        case InnerError::NORMAL: break; // pass
        case InnerError::HEADER_READY_ERROR: {
            throw OTSClientException(err.mMessage);
            break;
        }
        case InnerError::HEADER_MISSING_ERROR: {
            context.mResponseInfo.mErrorMessage = context.mHttpConnection->GetCurlMessage();
            break;
        }
        }
    }

    context.mProfiling.KeepTimeWithState(kProfPostProcess);
}

template<typename RequestPtr, typename ResultPtr>
void OTSClientImpl::ParseProtobufResult(RequestContext<RequestPtr, ResultPtr>& context)
{
    OTSProtocolBuilder::ParseProtobufResult(context.mPBResponsePtr, &context.mResultPtr);
}

void OTSClientImpl::ParseProtobufResult(
        RequestContext<BatchWriteRowRequestPtr, BatchWriteRowResultPtr>& context)
{
    OTSProtocolBuilder::ParseProtobufResult(context.mInitPBRequestPtr,
            context.mPBResponsePtr, &context.mResultPtr);
}

template<typename RequestPtr, typename ResultPtr>
void OTSClientImpl::FinishProcessInternal(RequestContext<RequestPtr, ResultPtr>& context)
{
    if (context.mResponseInfo.mStatus == HTTP_OK) {
        ParseProtobufResult(context);
        context.mResultPtr->SetRequestId(context.mResponseInfo.mRequestId);
        context.mResultPtr->SetTraceId(context.mResponseInfo.mTraceId);
    }
    context.mProfiling.KeepTimeWithState(kProfFinishProcess);

    static int thresholdInUsec = mClientConfig.mClientTracerThresholdInMS * 1000;
    if (context.mProfiling.GetTotalTime() >= thresholdInUsec) {
    }

    if (context.mResponseInfo.mStatus > HTTP_OK) {
        throw OTSException(
            context.mResponseInfo.mErrorCode,
            context.mResponseInfo.mErrorMessage,
            context.mResponseInfo.mRequestId,
            context.mResponseInfo.mTraceId,
            context.mResponseInfo.mStatus);
    } else if (context.mResponseInfo.mStatus < HTTP_OK) {
        throw OTSClientException(context.mResponseInfo.mErrorMessage);
    } 
}

bool OTSClientImpl::IsBatchOperation(const string& requestType)
{
    return (requestType == kAPIBatchGetRow || requestType == kAPIBatchWriteRow);
}

HttpConnection* OTSClientImpl::TryWaitConnection()
{
    int64_t totalTimeInUsec = 0;
    int64_t baseTimeInUsec = 2000;
    static int connTimeoutInUsec = mClientConfig.mConnectTimeoutInMS * 1000;
    HttpConnection* httpConn = NULL;
    for (int i = 0; i < 30 && httpConn == NULL; ++i) {
        httpConn = mHttpClient->GetConnection();
        if (httpConn != NULL) {
            break;
        }
        int64_t sleepTimeInUsec = baseTimeInUsec * (int)pow(2, i);
        if (totalTimeInUsec + sleepTimeInUsec >= connTimeoutInUsec) {
            // wait for too long time
            break;
        }
        usleep(sleepTimeInUsec);
        totalTimeInUsec += sleepTimeInUsec;
    }
    if (httpConn == NULL) {
        throw OTSClientException("No available connection.");
    }
    return httpConn;
}

void OTSClientImpl::CreateSignature(const string& operation, 
                                    HttpMethod method,
                                    const map<string, string>& headers,
                                    string* signature) const
{
    // CanonicalURI
    string plainText = "/" + operation + "\n";

    // HttpRequestMethod
    if (method == HTTP_POST) {
        plainText += "POST\n"; 
    } else {
        plainText += "GET\n"; 
    }

    plainText += "\n"; 

    //CanonicalHeaders
    map<string, string>::const_iterator headerIter = headers.begin();
    for (; headerIter != headers.end(); ++headerIter) {
        if (OTSHelper::StartsWith(headerIter->first, kOTSHeaderPrefix)) {
            plainText += headerIter->first + ":" + headerIter->second + "\n";
        } 
    }

    //calculate digest via hmacsha1
    string tmpDigest;
    OTSHelper::HmacSha1(mAuth.mAccessKeySecret, plainText, &tmpDigest);
    OTSHelper::Base64Encode(tmpDigest, signature);
}

} // end of tablestore
} // end of aliyun
