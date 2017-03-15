/* 
BSD 3-Clause License

Copyright (c) 2017, Alibaba Cloud
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
#include "client.hpp"
#include "../impl/ots_exception.hpp"
#include "../impl/profiling.hpp"
#include "../impl/ots_protocol_builder.hpp"
#include "../impl/ots_constants.hpp"
#include "../impl/ots_helper.hpp"
#include "../http/http_client.hpp"
#include "../protocol/table_store.pb.h"
#include "core/retry.hpp"
#include "util/try.hpp"
#include <tr1/memory>
#include <memory>
#include <cmath>

extern "C" {
#include <unistd.h> // for usleep
}

using namespace std;
using namespace std::tr1;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {

namespace {

class ResponseInfo
{
public:
    explicit ResponseInfo()
      : mStatus(0)
    {}

public:
    int mStatus;
    std::string mErrorCode;
    std::string mErrorMessage;
    std::string mRequestId;
    std::string mTraceId;
    std::string mTraceInfo;
};

template<class Request, class Result>
class RequestContext
{
public:

    RequestContext()
        : mHttpConnection(NULL)
        , mRetryCount(0)
        , mSkipSerializeBody(false)
    {}

public:
    HttpConnection* mHttpConnection;
    Action mRequestType;
    std::string mRequestURL;
    ResponseInfo mResponseInfo;
    Profiling mProfiling;
    int mRetryCount;

    // avoid repeated computation.
    std::string mRequestBody;
    std::string mBodyMD5Base64;
    std::string mSignature;

    MessagePtr mPBRequestPtr;
    const Request* mRequestPtr;
    Result* mResultPtr;

    ProtocolBuilder<Request, Result> mProtoBuilder;
    
    // for BatchGetRow/BatchWriteRow retry
    MessagePtr mInitPBRequestPtr;
    MessagePtr mLastPBResponsePtr;
    bool mSkipSerializeBody;
};

class ClientImpl: public ISyncClient
{
public:
    explicit ClientImpl(const Endpoint&, const Credential&, const ClientOptions&);

    Optional<Error> listTable(ListTableResponse*, const ListTableRequest&);
    Optional<Error> createTable(
        CreateTableResponse*, const CreateTableRequest&);
    Optional<Error> deleteTable(
        DeleteTableResponse*, const DeleteTableRequest&);
    Optional<Error> describeTable(
        DescribeTableResponse*, const DescribeTableRequest&);
    Optional<Error> updateTable(UpdateTableResponse*, const UpdateTableRequest&);
    Optional<Error> computeSplitsBySize(ComputeSplitsBySizeResponse*,
        const ComputeSplitsBySizeRequest&);
    Optional<Error> putRow(PutRowResponse*, const PutRowRequest&);
    Optional<Error> getRow(GetRowResponse*, const GetRowRequest&);
    Optional<Error> getRange(GetRangeResponse*, const GetRangeRequest&);
    Optional<Error> getRangeIterator(
        Iterator<MoveHolder<Row>, Error>**, const GetRangeRequest&);
    Optional<Error> updateRow(UpdateRowResponse*, const UpdateRowRequest&);
    Optional<Error> deleteRow(DeleteRowResponse*, const DeleteRowRequest&);
    Optional<Error> batchGetRow(
        BatchGetRowResponse*, const BatchGetRowRequest&);
    Optional<Error> batchWriteRow(
        BatchWriteRowResponse*, const BatchWriteRowRequest&);

private:
    template<class Request, class Result>
    void HandleRequest(RequestContext<Request, Result>& context);
    template<class Request, class Result>
    void PreProcessInternal(RequestContext<Request, Result>& context);
    template<class Request, class Result>
    void ProcessInternal(RequestContext<Request, Result>& context);
    template<class Request, class Result>
    void FinishProcessInternal(RequestContext<Request, Result>& context);
    template<class Request, class Result>
    void ParseProtobufResult(RequestContext<Request, Result>& context);

    HttpConnection* TryWaitConnection();
    void CreateSignature(Action,
        HttpMethod method,
        const std::map<std::string, std::string>& headers,
        std::string* signature) const;

private:
    Endpoint mEndpoint;
    Credential mCredential;
    ClientOptions mOptions;
    std::auto_ptr<HttpClient> mHttpClient;
};

ClientImpl::ClientImpl(
    const Endpoint& ep, const Credential& cr, const ClientOptions& opts)
  : mEndpoint(ep),
    mCredential(cr),
    mOptions(opts)
{
    HttpConfig httpConfig;
    httpConfig.SetMaxConnections(mOptions.maxConnections());
    httpConfig.SetConnectTimeoutInMS(mOptions.connectTimeout().toMsec());
    httpConfig.SetRequestTimeoutInMS(mOptions.requestTimeout().toMsec());
    httpConfig.SetEnableKeepAlive(true);
    mHttpClient.reset(new HttpClient(httpConfig));
}

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

bool isBatchAction(Action act)
{
    return act == API_BATCH_GET_ROW || act == API_BATCH_WRITE_ROW;
}

template<class Request, class Result>
void ClientImpl::HandleRequest(RequestContext<Request, Result>& context)
{
    // Acquire one connection.
    context.mHttpConnection = TryWaitConnection();
    ScopedConnection scopedConn(mHttpClient.get(), context.mHttpConnection);

    context.mRequestURL = mEndpoint.endpoint() + "/" + pp::prettyPrint(context.mRequestType);
    context.mHttpConnection->SetURL(context.mRequestURL);
    // convert request to protobuf request
    context.mPBRequestPtr = context.mProtoBuilder.serialize(*context.mRequestPtr);

    bool shouldRetry = false;
    auto_ptr<IRetryStrategy> retryStrategy(mOptions.retryStrategy()->clone());
    do {
        PreProcessInternal(context); 
        ProcessInternal(context);

        if (context.mResponseInfo.mStatus == HTTP_OK) {
            if (isBatchAction(context.mRequestType)) {
                // should retry on every single failed operation in this request.
                // however, this logic will be replaced very soon.
                // so, just leave blank here.
            }
        } else {
            Error error;
            *error.mutableErrorCode() = context.mResponseInfo.mErrorCode;
            *error.mutableMessage() = context.mResponseInfo.mErrorMessage;
            shouldRetry = retryStrategy->shouldRetry(context.mRequestType, error);
            if (shouldRetry) {
                sleepFor(retryStrategy->nextPause());
            }
        }
    } while (shouldRetry);
    context.mRetryCount = retryStrategy->retries();

    FinishProcessInternal(context);
}

template<class Request, class Result>
void ClientImpl::PreProcessInternal(RequestContext<Request, Result>& context)
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
    string ios8601TimeStr = UtcTime::now().toIso8601();
    context.mHttpConnection->AddRequestHeader(kOTSDate, ios8601TimeStr);
    context.mHttpConnection->AddRequestHeader(kOTSAPIVersion, kAPIVersion);
    context.mHttpConnection->AddRequestHeader(kOTSAccessKeyId, mCredential.accessKeyId());
    if (!mCredential.securityToken().empty()) {
        context.mHttpConnection->AddRequestHeader(kOTSStsToken, mCredential.securityToken());
    }
    context.mHttpConnection->AddRequestHeader(kOTSInstanceName, mEndpoint.instanceName());

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

#define OTS_INNER_TRY(result) \
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

template<class Requesst, class Result>
InnerError ParseRequestId(
    RequestContext<Requesst, Result>& context,
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

template<class Requesst, class Result>
InnerError ValidateContent(
    RequestContext<Requesst, Result>& context,
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

template<class Requesst, class Result>
InnerError ParseSuccessfulResponse(
    RequestContext<Requesst, Result>& context,
    const HeadersMap& headers,
    bool enableResponseMd5)
{
    OTS_INNER_TRY(ParseRequestId(context, headers));
    if (enableResponseMd5) {
        OTS_INNER_TRY(ValidateContent(context, headers));
    }

    context.mProtoBuilder.parse(context.mHttpConnection->GetResponseBody());

    return InnerError();
}

template<class Requesst, class Result>
InnerError ParseErroneousResponse(
    RequestContext<Requesst, Result>& context,
    const HeadersMap& headers,
    bool enableResponseMd5)
{
    OTS_INNER_TRY(ParseRequestId(context, headers));
    if (enableResponseMd5) {
        OTS_INNER_TRY(ValidateContent(context, headers));
    }

    Error error;
    ParseErrorResponse(&error, context.mHttpConnection->GetResponseBody());
    context.mResponseInfo.mErrorCode = error.errorCode(); 
    context.mResponseInfo.mErrorMessage = error.message(); 
    return InnerError();
}

#undef OTS_INNER_TRY

template<class Requesst, class Result>
void ClientImpl::ProcessInternal(RequestContext<Requesst, Result>& context)
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
            mOptions.checkResponseDigest());
        if (err.mError != InnerError::NORMAL) {
            throw OTSClientException(err.mMessage);
        }
    } else {
        InnerError err = ParseErroneousResponse(
            context,
            responseHeaders,
            mOptions.checkResponseDigest());
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

template<class Request, class Result>
void ClientImpl::ParseProtobufResult(RequestContext<Request, Result>& context)
{
    context.mProtoBuilder.deserialize(context.mResultPtr);
}

template<class Request, class Result>
void ClientImpl::FinishProcessInternal(RequestContext<Request, Result>& context)
{
    if (context.mResponseInfo.mStatus == HTTP_OK) {
        ParseProtobufResult(context);
        *context.mResultPtr->mutableRequestId() = context.mResponseInfo.mRequestId;
        *context.mResultPtr->mutableTraceId() = context.mResponseInfo.mTraceId;
    }
    context.mProfiling.KeepTimeWithState(kProfFinishProcess);

    int64_t thresholdInUsec = mOptions.traceThreshold().toUsec();
    if (context.mProfiling.GetTotalTime() >= thresholdInUsec) {
        // TODO: tracing
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

HttpConnection* ClientImpl::TryWaitConnection()
{
    int64_t totalTimeInUsec = 0;
    int64_t baseTimeInUsec = 2000;
    int connTimeoutInUsec = mOptions.connectTimeout().toUsec();
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

void ClientImpl::CreateSignature(Action act,
    HttpMethod method,
    const map<string, string>& headers,
    string* signature) const
{
    // CanonicalURI
    string plainText = "/" + pp::prettyPrint(act) + "\n";

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
        if (MemPiece::from(headerIter->first).startsWith(MemPiece::from(kOTSHeaderPrefix))) {
            plainText += headerIter->first + ":" + headerIter->second + "\n";
        } 
    }

    //calculate digest via hmacsha1
    string tmpDigest;
    OTSHelper::HmacSha1(mCredential.accessKeySecret(), plainText, &tmpDigest);
    OTSHelper::Base64Encode(tmpDigest, signature);
}


Optional<Error> ClientImpl::listTable(
    ListTableResponse* resp, const ListTableRequest& req)
{
    TRY(req.validate());
    try {
        RequestContext<ListTableRequest, ListTableResponse> requestContext;
        requestContext.mRequestType = API_LIST_TABLE;
        requestContext.mRequestPtr = &req;
        requestContext.mResultPtr = resp;
        HandleRequest(requestContext);
    } catch(const OTSException& ex) {
        return Optional<Error>(Error(
                ex.GetHttpStatus(),
                ex.GetErrorCode(),
                ex.GetMessage(),
                ex.GetTraceId(),
                ex.GetRequestId()));
    } catch(const OTSClientException& ex) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                ex.GetMessage(),
                ex.GetTraceId()));
    }
    return Optional<Error>();
}

Optional<Error> ClientImpl::createTable(
    CreateTableResponse* resp, const CreateTableRequest& req)
{
    TRY(req.validate());
    try {
        RequestContext<CreateTableRequest, CreateTableResponse> requestContext;
        requestContext.mRequestType = API_CREATE_TABLE;
        requestContext.mRequestPtr = &req;
        requestContext.mResultPtr = resp;
        HandleRequest(requestContext);
    } catch(const OTSException& ex) {
        return Optional<Error>(Error(
                ex.GetHttpStatus(),
                ex.GetErrorCode(),
                ex.GetMessage(),
                ex.GetTraceId(),
                ex.GetRequestId()));
    } catch(const OTSClientException& ex) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                ex.GetMessage(),
                ex.GetTraceId()));
    }
    return Optional<Error>();
}

Optional<Error> ClientImpl::deleteTable(
    DeleteTableResponse* resp, const DeleteTableRequest& req)
{
    TRY(req.validate());
    try {
        RequestContext<DeleteTableRequest, DeleteTableResponse> requestContext;
        requestContext.mRequestType = API_DELETE_TABLE;
        requestContext.mRequestPtr = &req;
        requestContext.mResultPtr = resp;
        HandleRequest(requestContext);
    } catch(const OTSException& ex) {
        return Optional<Error>(Error(
                ex.GetHttpStatus(),
                ex.GetErrorCode(),
                ex.GetMessage(),
                ex.GetTraceId(),
                ex.GetRequestId()));
    } catch(const OTSClientException& ex) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                ex.GetMessage(),
                ex.GetTraceId()));
    }
    return Optional<Error>();
}

Optional<Error> ClientImpl::describeTable(
    DescribeTableResponse* resp, const DescribeTableRequest& req)
{
    TRY(req.validate());
    try {
        RequestContext<DescribeTableRequest, DescribeTableResponse> requestContext;
        requestContext.mRequestType = API_DESCRIBE_TABLE;
        requestContext.mRequestPtr = &req;
        requestContext.mResultPtr = resp;
        HandleRequest(requestContext);
    } catch(const OTSException& ex) {
        return Optional<Error>(Error(
                ex.GetHttpStatus(),
                ex.GetErrorCode(),
                ex.GetMessage(),
                ex.GetTraceId(),
                ex.GetRequestId()));
    } catch(const OTSClientException& ex) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                ex.GetMessage(),
                ex.GetTraceId()));
    }
    return Optional<Error>();
}

Optional<Error> ClientImpl::updateTable(
    UpdateTableResponse* resp, const UpdateTableRequest& req)
{
    TRY(req.validate());
    try {
        RequestContext<UpdateTableRequest, UpdateTableResponse> requestContext;
        requestContext.mRequestType = API_UPDATE_TABLE;
        requestContext.mRequestPtr = &req;
        requestContext.mResultPtr = resp;
        HandleRequest(requestContext);
    } catch(const OTSException& ex) {
        return Optional<Error>(Error(
                ex.GetHttpStatus(),
                ex.GetErrorCode(),
                ex.GetMessage(),
                ex.GetTraceId(),
                ex.GetRequestId()));
    } catch(const OTSClientException& ex) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                ex.GetMessage(),
                ex.GetTraceId()));
    }
    return Optional<Error>();
}

Optional<Error> ClientImpl::computeSplitsBySize(
    ComputeSplitsBySizeResponse* resp, const ComputeSplitsBySizeRequest& req)
{
    TRY(req.validate());
    try {
        RequestContext<ComputeSplitsBySizeRequest, ComputeSplitsBySizeResponse> requestContext;
        requestContext.mRequestType = API_COMPUTE_SPLIT_POINTS_BY_SIZE;
        requestContext.mRequestPtr = &req;
        requestContext.mResultPtr = resp;
        HandleRequest(requestContext);
    } catch(const OTSException& ex) {
        return Optional<Error>(Error(
                ex.GetHttpStatus(),
                ex.GetErrorCode(),
                ex.GetMessage(),
                ex.GetTraceId(),
                ex.GetRequestId()));
    } catch(const OTSClientException& ex) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                ex.GetMessage(),
                ex.GetTraceId()));
    }
    return Optional<Error>();
}

Optional<Error> ClientImpl::putRow(PutRowResponse* resp, const PutRowRequest& req)
{
    TRY(req.validate());
    try {
        RequestContext<PutRowRequest, PutRowResponse> requestContext;
        requestContext.mRequestType = API_PUT_ROW;
        requestContext.mRequestPtr = &req;
        requestContext.mResultPtr = resp;
        HandleRequest(requestContext);
    } catch(const OTSException& ex) {
        return Optional<Error>(Error(
                ex.GetHttpStatus(),
                ex.GetErrorCode(),
                ex.GetMessage(),
                ex.GetTraceId(),
                ex.GetRequestId()));
    } catch(const OTSClientException& ex) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                ex.GetMessage(),
                ex.GetTraceId()));
    }
    return Optional<Error>();
}

Optional<Error> ClientImpl::getRow(GetRowResponse* resp, const GetRowRequest& req)
{
    TRY(req.validate());
    try {
        RequestContext<GetRowRequest, GetRowResponse> requestContext;
        requestContext.mRequestType = API_GET_ROW;
        requestContext.mRequestPtr = &req;
        requestContext.mResultPtr = resp;
        HandleRequest(requestContext);
    } catch(const OTSException& ex) {
        return Optional<Error>(Error(
                ex.GetHttpStatus(),
                ex.GetErrorCode(),
                ex.GetMessage(),
                ex.GetTraceId(),
                ex.GetRequestId()));
    } catch(const OTSClientException& ex) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                ex.GetMessage(),
                ex.GetTraceId()));
    }
    return Optional<Error>();
}

Optional<Error> ClientImpl::getRange(GetRangeResponse* resp, const GetRangeRequest& req)
{
    TRY(req.validate());
    try {
        RequestContext<GetRangeRequest, GetRangeResponse> requestContext;
        requestContext.mRequestType = API_GET_RANGE;
        requestContext.mRequestPtr = &req;
        requestContext.mResultPtr = resp;
        HandleRequest(requestContext);
    } catch(const OTSException& ex) {
        return Optional<Error>(Error(
                ex.GetHttpStatus(),
                ex.GetErrorCode(),
                ex.GetMessage(),
                ex.GetTraceId(),
                ex.GetRequestId()));
    } catch(const OTSClientException& ex) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                ex.GetMessage(),
                ex.GetTraceId()));
    }
    return Optional<Error>();
}

class GetRangeIterator: public Iterator<MoveHolder<Row>, Error>
{
public:
    explicit GetRangeIterator(ISyncClient* client, const GetRangeRequest& req)
      : mClient(client),
        mRequest(req),
        mIdx(0)
    {
    }

    Optional<Error> initialize()
    {
        for(;;) {
            TRY(mClient->getRange(&mResponse, mRequest));
            if (mResponse.rows().size() > 0) {
                break;
            }
            if (!mResponse.nextStart().present()) {
                break;
            }
            moveAssign(mRequest.mutableQueryCriterion()->mutableInclusiveStart(),
                util::move(**mResponse.mutableNextStart()));
        }
        return Optional<Error>();
    }

    bool valid() const throw()
    {
        return mIdx < mResponse.rows().size();
    }
    
    MoveHolder<Row> get() throw()
    {
        OTS_ASSERT(mIdx < mResponse.rows().size())
            (mIdx)(mResponse.rows().size());
        return util::move((*mResponse.mutableRows())[mIdx]);
    }

    Optional<Error> moveNext()
    {
        ++mIdx;
        if (mIdx < mResponse.rows().size()) {
            return Optional<Error>();
        }
        for(;;) {
            if (!mResponse.nextStart().present()) {
                break;
            }
            moveAssign(mRequest.mutableQueryCriterion()->mutableInclusiveStart(),
                util::move(**mResponse.mutableNextStart()));
            TRY(mClient->getRange(&mResponse, mRequest));
            if (mResponse.rows().size() > 0) {
                mIdx = 0;
                break;
            }
        }
        return Optional<Error>();
    }

private:
    ISyncClient* mClient;
    GetRangeRequest mRequest;
    GetRangeResponse mResponse;
    int64_t mIdx;
};

Optional<Error> ClientImpl::getRangeIterator(
    Iterator<MoveHolder<Row>, Error>** result, const GetRangeRequest& req)
{
    auto_ptr<GetRangeIterator> iter(new GetRangeIterator(this, req));
    TRY(iter->initialize());
    *result = iter.release();
    return Optional<Error>();
}

Optional<Error> ClientImpl::updateRow(
    UpdateRowResponse* resp, const UpdateRowRequest& req)
{
    TRY(req.validate());
    try {
        RequestContext<UpdateRowRequest, UpdateRowResponse> requestContext;
        requestContext.mRequestType = API_UPDATE_ROW;
        requestContext.mRequestPtr = &req;
        requestContext.mResultPtr = resp;
        HandleRequest(requestContext);
    } catch(const OTSException& ex) {
        return Optional<Error>(Error(
                ex.GetHttpStatus(),
                ex.GetErrorCode(),
                ex.GetMessage(),
                ex.GetTraceId(),
                ex.GetRequestId()));
    } catch(const OTSClientException& ex) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                ex.GetMessage(),
                ex.GetTraceId()));
    }
    return Optional<Error>();
}

Optional<Error> ClientImpl::deleteRow(
    DeleteRowResponse* resp, const DeleteRowRequest& req)
{
    TRY(req.validate());
    try {
        RequestContext<DeleteRowRequest, DeleteRowResponse> requestContext;
        requestContext.mRequestType = API_DELETE_ROW;
        requestContext.mRequestPtr = &req;
        requestContext.mResultPtr = resp;
        HandleRequest(requestContext);
    } catch(const OTSException& ex) {
        return Optional<Error>(Error(
                ex.GetHttpStatus(),
                ex.GetErrorCode(),
                ex.GetMessage(),
                ex.GetTraceId(),
                ex.GetRequestId()));
    } catch(const OTSClientException& ex) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                ex.GetMessage(),
                ex.GetTraceId()));
    }
    return Optional<Error>();
}

Optional<Error> ClientImpl::batchGetRow(
    BatchGetRowResponse* resp, const BatchGetRowRequest& req)
{
    TRY(req.validate());
    try {
        RequestContext<BatchGetRowRequest, BatchGetRowResponse> requestContext;
        requestContext.mRequestType = API_BATCH_GET_ROW;
        requestContext.mRequestPtr = &req;
        requestContext.mResultPtr = resp;
        HandleRequest(requestContext);
    } catch(const OTSException& ex) {
        return Optional<Error>(Error(
                ex.GetHttpStatus(),
                ex.GetErrorCode(),
                ex.GetMessage(),
                ex.GetTraceId(),
                ex.GetRequestId()));
    } catch(const OTSClientException& ex) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                ex.GetMessage(),
                ex.GetTraceId()));
    }
    return Optional<Error>();
}

Optional<Error> ClientImpl::batchWriteRow(
    BatchWriteRowResponse* resp, const BatchWriteRowRequest& req)
{
    TRY(req.validate());
    try {
        RequestContext<BatchWriteRowRequest, BatchWriteRowResponse> requestContext;
        requestContext.mRequestType = API_BATCH_WRITE_ROW;
        requestContext.mRequestPtr = &req;
        requestContext.mResultPtr = resp;
        HandleRequest(requestContext);
    } catch(const OTSException& ex) {
        return Optional<Error>(Error(
                ex.GetHttpStatus(),
                ex.GetErrorCode(),
                ex.GetMessage(),
                ex.GetTraceId(),
                ex.GetRequestId()));
    } catch(const OTSClientException& ex) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                ex.GetMessage(),
                ex.GetTraceId()));
    }
    return Optional<Error>();
}

} // namespace

Optional<Error> ISyncClient::create(
    ISyncClient** result,
    const Endpoint& ep, const Credential& cr, const ClientOptions& opts)
{
    TRY(ep.validate());
    TRY(cr.validate());
    TRY(opts.validate());

    *result = new ClientImpl(ep, cr, opts);

    return Optional<Error>();
}

} // namespace core
} // namespace tablestore
} // namespace aliyun
