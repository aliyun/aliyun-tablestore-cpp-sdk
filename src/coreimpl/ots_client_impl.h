/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#ifndef OTS_CLIENT_IMPL_H
#define OTS_CLIENT_IMPL_H

#include "../http/http_client.h"
#include "../protocol/table_store.pb.h"
#include "ots_protocol_builder.h"
#include "../utility/profiling.h"
#include "ots/ots_config.h"
#include <iostream>
#include <string>
#include <sys/types.h>
#include <tr1/memory>
#include <vector>

using namespace com::aliyun;

namespace aliyun {
namespace tablestore {

class ResponseInfo
{
public:

    ResponseInfo()
        : mStatus(0) {}

public:

    int mStatus;
    std::string mErrorCode;
    std::string mErrorMessage;
    std::string mRequestId;
    std::string mTraceId;
    std::string mTraceInfo;
};

template<typename RequestPtr, typename ResultPtr>
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
    std::string mRequestType;
    std::string mRequestURL;
    ResponseInfo mResponseInfo;
    Profiling mProfiling;
    int mRetryCount;

    // avoid repeated computation.
    std::string mRequestBody;
    std::string mBodyMD5Base64;
    std::string mSignature;

    MessagePtr mPBRequestPtr;
    MessagePtr mPBResponsePtr;
    RequestPtr mRequestPtr;
    ResultPtr mResultPtr;

    // for BatchGetRow/BatchWriteRow retry
    MessagePtr mInitPBRequestPtr;
    MessagePtr mLastPBResponsePtr;
    bool mSkipSerializeBody;
};

class OTSClientImpl
{
public:
    explicit OTSClientImpl(
        const std::string& endpoint,
        const std::string& instanceName,
        const Credential& auth,
        const ClientConfiguration& clientConfig);

    ~OTSClientImpl();

    CreateTableResultPtr CreateTable(const CreateTableRequestPtr& requestPtr)
        throw (OTSException, OTSClientException);

    ListTableResultPtr ListTable()
        throw (OTSException, OTSClientException);

    DescribeTableResultPtr DescribeTable(const DescribeTableRequestPtr& requestPtr)
        throw (OTSException, OTSClientException);

    DeleteTableResultPtr DeleteTable(const DeleteTableRequestPtr& requestPtr)
        throw (OTSException, OTSClientException);

    UpdateTableResultPtr UpdateTable(const UpdateTableRequestPtr& requestPtr)
        throw (OTSException, OTSClientException);

    GetRowResultPtr GetRow(const GetRowRequestPtr& requestPtr)
        throw (OTSException, OTSClientException);

    PutRowResultPtr PutRow(const PutRowRequestPtr& requestPtr)
        throw (OTSException, OTSClientException);

    UpdateRowResultPtr UpdateRow(const UpdateRowRequestPtr& requestPtr)
        throw (OTSException, OTSClientException);

    DeleteRowResultPtr DeleteRow(const DeleteRowRequestPtr& requestPtr)
        throw (OTSException, OTSClientException);

    BatchGetRowResultPtr BatchGetRow(const BatchGetRowRequestPtr& requestPtr)
        throw (OTSException, OTSClientException);

    BatchWriteRowResultPtr BatchWriteRow(const BatchWriteRowRequestPtr& requestPtr)
        throw (OTSException, OTSClientException);

    GetRangeResultPtr GetRange(const GetRangeRequestPtr& requestPtr)
        throw (OTSException, OTSClientException);

    RowIteratorPtr GetRangeIterator(const GetRangeRequestPtr& rangeRequestPtr)
        throw (OTSException, OTSClientException);

    ComputeSplitsBySizeResultPtr ComputeSplitsBySize(const ComputeSplitsBySizeRequestPtr&)
        throw (OTSException, OTSClientException);

private:

    void Initialize();

    template<typename RequestPtr, typename ResultPtr>
    void HandleRequest(RequestContext<RequestPtr, ResultPtr>& context);

    template<typename RequestPtr, typename ResultPtr>
    void PreProcessInternal(RequestContext<RequestPtr, ResultPtr>& context);

    template<typename RequestPtr, typename ResultPtr>
    void ProcessInternal(RequestContext<RequestPtr, ResultPtr>& context);

    template<typename RequestPtr, typename ResultPtr>
    void FinishProcessInternal(RequestContext<RequestPtr, ResultPtr>& context);

    template<typename RequestPtr, typename ResultPtr>
    void ParseProtobufResult(RequestContext<RequestPtr, ResultPtr>& context);

    void ParseProtobufResult(RequestContext<BatchWriteRowRequestPtr, BatchWriteRowResultPtr>& context);

    bool IsBatchOperation(const std::string& requestType);

    HttpConnection* TryWaitConnection();

    void CreateSignature(const std::string& operation, 
                         HttpMethod method,
                         const std::map<std::string, std::string>& headers,
                         std::string* signature) const;

private:

    std::string mEndpoint;
    std::string mProtocol;
    std::string mAddress;
    std::string mPort;

    Credential mAuth;
    std::string mInstanceName;

    ClientConfiguration mClientConfig;

    std::auto_ptr<HttpClient> mHttpClient;
};

} // end of tablestore
} // end of aliyun

#endif
