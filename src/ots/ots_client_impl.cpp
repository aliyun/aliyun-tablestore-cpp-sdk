#include "ots_client_impl.h"

#include "common_util.h"
#include "ots_exception_impl.h"
#include "ots_factory_impl.h"
#include "ots_types_impl.h"

#include <cstdio>

using namespace std;
using namespace com::aliyun;
using namespace google::protobuf;

namespace aliyun {
namespace openservices {
namespace ots {

char const * const OTS_API_VERSION = "2014-08-08";

namespace {
    const string kAPIVersion = "x-ots-apiversion";
    const string kDate = "x-ots-date";
    const string kAccessId = "x-ots-accesskeyid";
    const string kInstanceName = "x-ots-instancename";
    const string kContentMD5 = "x-ots-contentmd5";
    const string kSignature = "x-ots-signature";
    const string kRequestCompressType = "x-ots-request-compress-type";
    const string kRequestCompressSize = "x-ots-request-compress-size";
    const string kResponseCompressType = "x-ots-response-compress-type";
    const string kResponseCompressSize = "x-ots-response-compress-size";
    const string kRequestId = "x-ots-requestid";
    const string kAuthorization = "Authorization";
    const string kLocation = "Location";
    const string kCompressDefalte = "deflate";

    const string kCreateTable = "CreateTable";
    const string kDescribeTable = "DescribeTable";
    const string kUpdateTable = "UpdateTable";
    const string kListTable = "ListTable";
    const string kDeleteTable = "DeleteTable";
    const string kGetRow = "GetRow";
    const string kPutRow = "PutRow";
    const string kUpdateRow = "UpdateRow";
    const string kDeleteRow = "DeleteRow";
    const string kBatchGetRow = "BatchGetRow";
    const string kBatchWriteRow = "BatchWriteRow";
    const string kGetRange = "GetRange";
}

OTSClientImpl::OTSClientImpl(const OTSConfig& otsConfig)
:   mEndPoint(otsConfig.mEndPoint), mAccessId(otsConfig.mAccessId),
    mAccessKey(otsConfig.mAccessKey), mInstanceName(otsConfig.mInstanceName)
{
    EnsureNotEmptyString(mEndPoint, "EndPoint");
    EnsureNotEmptyString(mAccessId, "AccessId");
    EnsureNotEmptyString(mAccessKey, "AccessKey");
    EnsureNotEmptyString(mInstanceName, "InstanceName");

    //Init HTTP client and retry strategy.
    mRequestCompressType = otsConfig.mRequestCompressType;
    mResponseCompressType = otsConfig.mResponseCompressType;
    if (otsConfig.mRequestTimeout < 0) {
        string msg = "The timeout cannot be less than 0";
        OTS_THROW("OTSClient", "ClientParameterError", msg.c_str()); 
    }
    if (otsConfig.mMaxErrorRetry < 0) {
        string msg = "The maximum count for retrying cannot be less than 0";
        OTS_THROW("OTSClient", "ClientParameterError", msg.c_str()); 
    }
    mHttpClientPtr.reset(new HttpClient(otsConfig.mRequestTimeout));
    mRetryStrategyPtr.reset(new OTSRetryStrategy(otsConfig.mMaxErrorRetry));
}

OTSClientImpl::~OTSClientImpl()
{
    //Nothing to do
}

void OTSClientImpl::CreateTable(const CreateTableRequest& request)
{
    const CreateTableRequestImpl& reqImpl = 
        dynamic_cast<const CreateTableRequestImpl&>(request); 
    HandleRequestNoResult(kCreateTable, reqImpl.GetPBMessage());
}

void OTSClientImpl::DescribeTable(
            const std::string& tableName,
            DescribeTableResponse* response)
{
    EnsureNotNullPointer(response, "response");

    cloudservice::ots2::DescribeTableRequest pbRequest;
    pbRequest.set_table_name(tableName);
    DescribeTableResponseImpl* respImpl = dynamic_cast<DescribeTableResponseImpl*>(response);
    HandleRequestWithResult(kDescribeTable, &pbRequest, respImpl->GetPBMessage());
    respImpl->ParseFromPBMessage();
}

void OTSClientImpl::UpdateTable(
            const UpdateTableRequest& request,
            UpdateTableResponse* response)
{
    EnsureNotNullPointer(response, "response");

    const UpdateTableRequestImpl& reqImpl = dynamic_cast<const UpdateTableRequestImpl&>(request); 
    UpdateTableResponseImpl* respImpl = dynamic_cast<UpdateTableResponseImpl*>(response);
    HandleRequestWithResult(kUpdateTable, reqImpl.GetPBMessage(), respImpl->GetPBMessage());
}

void OTSClientImpl::ListTable(ListTableResponse* response)
{
    EnsureNotNullPointer(response, "response");

    cloudservice::ots2::ListTableRequest pbRequest;
    ListTableResponseImpl* respImpl = dynamic_cast<ListTableResponseImpl*>(response);
    HandleRequestWithResult(kListTable, &pbRequest, respImpl->GetPBMessage());
}

void OTSClientImpl::DeleteTable(const string& tableName)
{
    cloudservice::ots2::DeleteTableRequest pbRequest;
    pbRequest.set_table_name(tableName);
    HandleRequestNoResult(kDeleteTable, &pbRequest);
}

void OTSClientImpl::GetRow(
            const GetRowRequest& request,
            GetRowResponse* response)
{
    EnsureNotNullPointer(response, "response");

    const GetRowRequestImpl& reqImpl = 
        dynamic_cast<const GetRowRequestImpl&>(request); 
    GetRowResponseImpl* respImpl = dynamic_cast<GetRowResponseImpl*>(response);
    HandleRequestWithResult(kGetRow, reqImpl.GetPBMessage(), respImpl->GetPBMessage());
    respImpl->ParseFromPBMessage();
}

void OTSClientImpl::PutRow(
            const PutRowRequest& request,
            PutRowResponse* response)
{
    EnsureNotNullPointer(response, "response");

    const PutRowRequestImpl& reqImpl = 
        dynamic_cast<const PutRowRequestImpl&>(request); 
    PutRowResponseImpl* respImpl = dynamic_cast<PutRowResponseImpl*>(response);
    HandleRequestWithResult(kPutRow, reqImpl.GetPBMessage(), respImpl->GetPBMessage());
    respImpl->ParseFromPBMessage();
}

void OTSClientImpl::UpdateRow(
            const UpdateRowRequest& request,
            UpdateRowResponse* response)
{
    EnsureNotNullPointer(response, "response");

    const UpdateRowRequestImpl& reqImpl = 
        dynamic_cast<const UpdateRowRequestImpl&>(request); 
    UpdateRowResponseImpl* respImpl = dynamic_cast<UpdateRowResponseImpl*>(response);
    HandleRequestWithResult(kUpdateRow, reqImpl.GetPBMessage(), respImpl->GetPBMessage());
    respImpl->ParseFromPBMessage();
}

void OTSClientImpl::DeleteRow(
            const DeleteRowRequest& request,
            DeleteRowResponse* response)
{
    EnsureNotNullPointer(response, "response");

    const DeleteRowRequestImpl& reqImpl = 
        dynamic_cast<const DeleteRowRequestImpl&>(request); 
    DeleteRowResponseImpl* respImpl = dynamic_cast<DeleteRowResponseImpl*>(response);
    HandleRequestWithResult(kDeleteRow, reqImpl.GetPBMessage(), respImpl->GetPBMessage());
    respImpl->ParseFromPBMessage();
}

void OTSClientImpl::BatchGetRow(
            const BatchGetRowRequest& request,
            BatchGetRowResponse* response)
{
    EnsureNotNullPointer(response, "response");

    const BatchGetRowRequestImpl& reqImpl = 
        dynamic_cast<const BatchGetRowRequestImpl&>(request); 
    BatchGetRowResponseImpl* respImpl = dynamic_cast<BatchGetRowResponseImpl*>(response);
    HandleRequestWithResult(kBatchGetRow, reqImpl.GetPBMessage(), respImpl->GetPBMessage());
    respImpl->ParseFromPBMessage();
}

void OTSClientImpl::BatchWriteRow(
            const BatchWriteRowRequest& request,
            BatchWriteRowResponse* response)
{
    EnsureNotNullPointer(response, "response");
    
    const BatchWriteRowRequestImpl& reqImpl = 
        dynamic_cast<const BatchWriteRowRequestImpl&>(request); 
    BatchWriteRowResponseImpl* respImpl = dynamic_cast<BatchWriteRowResponseImpl*>(response);
    HandleRequestWithResult(kBatchWriteRow, reqImpl.GetPBMessage(), respImpl->GetPBMessage());
    respImpl->ParseFromPBMessage();
}

void OTSClientImpl::GetRange(
            const GetRangeRequest& request,
            GetRangeResponse* response)
{
    EnsureNotNullPointer(response, "response");

    const GetRangeRequestImpl& reqImpl = 
        dynamic_cast<const GetRangeRequestImpl&>(request); 
    GetRangeResponseImpl* respImpl = dynamic_cast<GetRangeResponseImpl*>(response);
    HandleRequestWithResult(kGetRange, reqImpl.GetPBMessage(), respImpl->GetPBMessage());
    respImpl->ParseFromPBMessage();
}

void OTSClientImpl::GetRangeByIterator(
            const GetRangeRequest& request, 
            int64_t* consumed_capacity_unit, 
            RowIterator* iterator,
            int32_t row_count)
{
    EnsureNotNullPointer(iterator, "iterator");

    const GetRangeRequestImpl& reqImpl =
        dynamic_cast<const GetRangeRequestImpl&>(request);
    cloudservice::ots2::GetRangeRequest pbRequest;
    pbRequest.CopyFrom(*(reqImpl.GetPBMessage()));
    if (row_count > 0) {
        pbRequest.set_limit(row_count);
    }
    RowIteratorImpl* iterImpl =  dynamic_cast<RowIteratorImpl*>(iterator);
    HandleRequestWithResult(kGetRange, &pbRequest, iterImpl->GetPBMessage());
    iterImpl->InitForFirstTime(this, &pbRequest, consumed_capacity_unit, row_count);
}

void OTSClientImpl::GetNextRange(
            const cloudservice::ots2::GetRangeRequest* pbRequest,
            cloudservice::ots2::GetRangeResponse* pbResponse)
{
    HandleRequestWithResult(kGetRange, pbRequest, pbResponse);
}

void OTSClientImpl::EnsureNotEmptyString(const string& str, const string& name)
{
    if (str.empty()) {
        string errorMessage = name + " is empty";
        OTS_THROW("OTSClient", "ClientParameterError", errorMessage.c_str());
    }
}

void OTSClientImpl::EnsureNotNullPointer(void* ptr, const string& name)
{
    if (NULL == ptr) {
        string errorMessage = name + " is null";
        OTS_THROW("OTSClient", "ClientParameterError", errorMessage.c_str());
    }
}

void OTSClientImpl::ValidHasInitialized(const Message* pbMessage)
{
    if (pbMessage != NULL && pbMessage->IsInitialized() == false) {
        OTS_THROW("OTSClient", "ClientParameterError", "Request has not been initialized");
    }
}

string OTSClientImpl::GetCompressTypeString(CompressType type) const
{
    if (type == COMPRESS_DEFLATE) {
        return "deflate";
    } else {
        OTS_THROW("OTSClient", "ClientParameterError", "CompressType is invalid");
    }
}

CompressType OTSClientImpl::GetCompressType(const std::string& compressString) const
{
    if (compressString == "") {
        return COMPRESS_NO;
    } else if (compressString == "defalte") {
        return COMPRESS_DEFLATE;
    } else {
        string msg = "CompressType " + compressString + " is unsupported";
        OTS_THROW("OTSClient", "OTSInvalidResponse", msg.c_str());
    }
}

bool OTSClientImpl::FindInMap(
            const map<string, string>& anyMap,
            const string& key,
            string* value)
{
    map<string, string>::const_iterator iter = anyMap.find(key);
    if (iter != anyMap.end()) {
        *value = iter->second;
        return true;
    } else {
        return false;
    }
}

void OTSClientImpl::PackHttpRequest(
            const string& operation, 
            const Message* pbMessage,
            HttpRequest* httpRequest)
{
    ValidHasInitialized(pbMessage);

    // Handle http body according to the compress type.
    if (pbMessage != NULL) {
        if (mRequestCompressType != COMPRESS_NO) {
            // compress by deflate
            string tmpHttpBody;
            pbMessage->SerializeToString(&tmpHttpBody);
            httpRequest->mRequestHeaders[kRequestCompressType] = 
                GetCompressTypeString(mRequestCompressType);
            httpRequest->mRequestHeaders[kRequestCompressSize] = 
                Int64ToString(tmpHttpBody.size());
            if (DeflateCompress(tmpHttpBody, &httpRequest->mRequestBody) != true) {
                OTS_THROW(operation.c_str(), "OTSParameterInvalid", "fail to compress data"); 
            }
        } else {
            pbMessage->SerializeToString(&httpRequest->mRequestBody);
        }
    } else {
        httpRequest->mRequestBody = "";        
    }

    // Set the compress type for response data.
    if (mResponseCompressType != COMPRESS_NO) {
        httpRequest->mRequestHeaders[kResponseCompressType] = 
            GetCompressTypeString(mResponseCompressType);
    }

    PackHttpRequestInternal(operation, httpRequest);
}

void OTSClientImpl::PackHttpRequestInternal(
            const string& operation, 
            HttpRequest* httpRequest)
{
    httpRequest->mHttpMethod = HTTP_POST;
    httpRequest->mRequestUrl = mEndPoint + "/" + operation;

    // Calculate MD5 of Http body.
    string httpBodyMD5;
    string bodyMD5Base64;
    MD5String(httpRequest->mRequestBody, &httpBodyMD5);
    Base64Encode(httpBodyMD5, &bodyMD5Base64);
    httpRequest->mRequestHeaders[kContentMD5] = bodyMD5Base64;
    
    // Set Http header options.
    httpRequest->mRequestHeaders[kDate] = UTCTime2String();
    httpRequest->mRequestHeaders[kAPIVersion] = OTS_API_VERSION; 
    httpRequest->mRequestHeaders[kAccessId] = mAccessId;
    httpRequest->mRequestHeaders[kInstanceName] = mInstanceName;

    // Generate the signature.
    string requestSignature;
    map<string, string> emptyQuerys;
    CreateSignature(operation, httpRequest->mHttpMethod, emptyQuerys, 
                httpRequest->mRequestHeaders, &requestSignature); 
    httpRequest->mRequestHeaders[kSignature] = requestSignature;
}

void OTSClientImpl::CreateSignature(
            const string& operation, 
            HttpMethod method, 
            const map<string, string>& querys,
            const map<string, string>& headers,
            string* signature)
{
    // CanonicalURI
    string plainText = "/" + operation + "\n";

    // HttpRequestMethod
    if (method == HTTP_POST) {
        plainText += "POST\n"; 
    } else {
        plainText += "GET\n"; 
    }

    // CanonicalQueryString
    if (!querys.empty()) {
        string tmpKey;
        string queryString;
        map<string, string> tmpQuerys;

        map<string, string>::const_iterator queryIter = querys.begin();
        for (; queryIter != querys.end(); ++queryIter) {
            tmpKey = EncodeURL(queryIter->first) + "=" + 
                     EncodeURL(queryIter->second);
            tmpQuerys[tmpKey] = "";
        }

        map<string, string>::iterator tmpIter = tmpQuerys.begin();
        while (tmpIter != tmpQuerys.end()) {
            plainText += tmpIter->first;
            if (++tmpIter != tmpQuerys.end()) {
                plainText += "&"; 
            }
        }
    }
    plainText += "\n"; 

    //CanonicalHeaders
    map<string, string>::const_iterator headerIter = headers.begin();
    for (; headerIter != headers.end(); ++headerIter) {
        plainText += headerIter->first + ":" + headerIter->second + "\n"; 
    }

    //calculate digest via hmacsha1
    string tmpDigest;
    HmacSha1(mAccessKey, plainText, &tmpDigest);
    Base64Encode(tmpDigest, signature);
}

void OTSClientImpl::HandleRequestNoResult(
            const string& operation,
            const Message* pbRequest)
{
    //pack http request, then send it to service
    HttpRequest httpRequest;
    HttpResponse httpResponse;
    PackHttpRequest(operation, pbRequest, &httpRequest);
    InvokeSendRequest(operation, httpRequest, &httpResponse);
}

void OTSClientImpl::HandleRequestWithResult(
            const string& operation, 
            const Message* pbRequest,
            Message* pbResponse)
{
    //pack http request, then send it to service
    HttpRequest httpRequest;
    HttpResponse httpResponse;
    PackHttpRequest(operation, pbRequest, &httpRequest);
    InvokeSendRequest(operation, httpRequest, &httpResponse);
    ParseHttpResponse(operation, httpResponse, pbResponse);
}

void OTSClientImpl::InvokeSendRequest(
            const string& operation, 
            const HttpRequest& httpRequest, 
            HttpResponse *httpResponse)
{
    int retryCount = 0;
    bool needRetry = false;
    RequestStatus tmpStatus;

    do {
        // Clear status record
        needRetry = false; 
        tmpStatus.mErrorCode = "";
        tmpStatus.mErrorMessage = "";
        tmpStatus.mRequestId = "";

        // Send request via HttpClient
        mHttpClientPtr->SendRequest(httpRequest, httpResponse); 
 
        if (httpResponse->mHttpStatus < 0) {
            tmpStatus.mErrorCode = "OTSNetworkError";
            tmpStatus.mErrorMessage = "Network error, CURLcode:" + Int64ToString(-httpResponse->mHttpStatus);
        } else {
            bool isContinueCheck = false; 
            try {
                ValidateResponse(operation, *httpResponse, &tmpStatus);
                isContinueCheck = true;
            } catch (const OTSExceptionImpl& e) {
                if (httpResponse->mHttpStatus == 403) {
                    try {
                        HandleBadRequest(*httpResponse, &tmpStatus);
                        if (tmpStatus.mErrorCode != "OTSAuthFailed") {
                            tmpStatus.mErrorCode = "OTSInvalidResponse";
                            tmpStatus.mErrorMessage = "Check response header fail, HTTPcode:" + Int64ToString(-httpResponse->mHttpStatus);
                        } else {
                            isContinueCheck = true;
                        }
                    } catch (const OTSExceptionImpl& e) {
                        tmpStatus.mErrorCode = e.GetErrorCode();
                        tmpStatus.mErrorMessage = e.GetErrorMessage();
                    }
                } else {
                    tmpStatus.mErrorCode = e.GetErrorCode();
                    tmpStatus.mErrorMessage = e.GetErrorMessage();
                }
            }

            if (isContinueCheck) {
                try {
                    if (httpResponse->mHttpStatus == 200) {
                        tmpStatus.mErrorCode = "OK";
                    } else if (httpResponse->mHttpStatus == 400 ||
                               httpResponse->mHttpStatus == 403 ||
                               httpResponse->mHttpStatus == 404 ||
                               httpResponse->mHttpStatus == 405 ||
                               httpResponse->mHttpStatus == 408 ||
                               httpResponse->mHttpStatus == 409 ||
                               httpResponse->mHttpStatus == 413 ||
                               httpResponse->mHttpStatus == 500 ||
                               httpResponse->mHttpStatus == 503) {
                        HandleBadRequest(*httpResponse, &tmpStatus);    
                    } else {
                        tmpStatus.mErrorCode = "OTSUnknownError";
                        tmpStatus.mErrorMessage = "Unknown error, HTTPcode:" + Int64ToString(-httpResponse->mHttpStatus);
                    }
                } catch (const OTSExceptionImpl& e) {
                    tmpStatus.mErrorCode = e.GetErrorCode();
                    tmpStatus.mErrorMessage = e.GetErrorMessage();
                }
            }
        } 
        // Should retry?
        if (retryCount < mRetryStrategyPtr->GetMaxRetry() &&
            mRetryStrategyPtr->ShouldRetry(operation, tmpStatus.mErrorCode))
        {
            needRetry = true;
            ++retryCount;
        }
    } while(needRetry);

    //handle errors
    if (tmpStatus.mErrorCode != "OK") {
        char buf[64];
        snprintf(buf, sizeof(buf), "%ld", httpResponse->mHttpStatus); 
        OTS_THROW(
            operation.c_str(), 
            tmpStatus.mErrorCode.c_str(), 
            tmpStatus.mErrorMessage.c_str(), 
            tmpStatus.mRequestId.c_str(), 
            string(buf).c_str()); 
    }
}

void OTSClientImpl::HandleBadRequest(
            const HttpResponse& httpResponse,
            RequestStatus *reqStatus)
{
    if (!FindInMap(httpResponse.mResponseHeaders, kRequestId, &(reqStatus->mRequestId))) {
        string errorMessage = kRequestId + " is not found";
        OTS_THROW("", "OTSInvalidResponse", errorMessage.c_str()); 
    }

    cloudservice::ots2::Error pbMessage;
    ParsePBMessage(httpResponse, &pbMessage);
    reqStatus->mErrorCode = pbMessage.code();
    reqStatus->mErrorMessage = pbMessage.message();
}

void OTSClientImpl::HandleMovedRequest(
            const HttpResponse& httpResponse,
            RequestStatus *reqStatus)
{
    if (!FindInMap(httpResponse.mResponseHeaders, kRequestId, &(reqStatus->mRequestId))) {
        string errorMessage = kRequestId + " is not found";
        OTS_THROW("", "OTSInvalidResponse", errorMessage.c_str()); 
    }
    if (!FindInMap(httpResponse.mResponseHeaders, kLocation, &(reqStatus->mLocation))) {
        string errorMessage = kLocation + " is not found";
        OTS_THROW("", "OTSInvalidResponse", errorMessage.c_str());
    }
    reqStatus->mErrorCode = "OTSServiceMoved";
    reqStatus->mErrorMessage = "Service is moved to the location: " + reqStatus->mLocation;
}

void OTSClientImpl::ValidateResponse(
            const string& operation,
            const HttpResponse& httpResponse,
            RequestStatus *reqStatus)
{
    if (!FindInMap(httpResponse.mResponseHeaders, kRequestId, &(reqStatus->mRequestId))) {
        string errorMessage = kRequestId + " is not found";
        OTS_THROW("", "OTSInvalidResponse", errorMessage.c_str()); 
    }

    string respDate;
    if (!FindInMap(httpResponse.mResponseHeaders, kDate, &respDate)) {
        string errorMessage = kDate + " is invalid";
        OTS_THROW("", "OTSInvalidResponse", errorMessage.c_str());
    }

    // Make sure the response date within 15 minutes.
    int64_t respDateSeconds = UTCString2LocalTime(respDate);
    if (respDateSeconds < 0) {
        string errorMessage = kDate + " is invalid";
        OTS_THROW("", "OTSInvalidResponse", errorMessage.c_str());
    }
    int64_t clientDateSeconds = UTCTimeInSecond();
    if (clientDateSeconds < 0) {
        OTS_THROW("", "ClientInternalError", "fail to get UTC time");
    }
    int64_t dateDiff = respDateSeconds - clientDateSeconds;
    if (dateDiff < -900 || dateDiff > 900 ) {
        string errorMessage = kDate + " is not in proper range";
        OTS_THROW("", "OTSInvalidResponse", errorMessage.c_str());
    }

}

void OTSClientImpl::ParseHttpResponse(
            const string& operation,
            const HttpResponse& httpResponse, 
            Message* pbMessage)
{
    string requestId;
    if (!FindInMap(httpResponse.mResponseHeaders, kRequestId, &requestId)) {
        string errorMessage = kRequestId + " is not found";
        OTS_THROW("", "OTSInvalidResponse", errorMessage.c_str()); 
    }

    try {
        ParsePBMessage(httpResponse, pbMessage);
    } catch (const OTSExceptionImpl& e) {
        const string& errorCode = e.GetErrorCode(); 
        const string& errorMessage = e.GetErrorMessage(); 
        OTS_THROW(operation.c_str(), errorCode.c_str(), 
                    errorMessage.c_str(), requestId.c_str());
    }
}

void OTSClientImpl::ParsePBMessage(
            const HttpResponse& httpResponse, 
            Message* pbMessage)
{
    ValidateContentMD5(httpResponse);

    string decompBody;
    const string* rawBody = &httpResponse.mResponseBody;
    string strCompressType;
    if (FindInMap(httpResponse.mResponseHeaders, kResponseCompressType, &strCompressType)) {
        // decompress
        DecompressResponse(httpResponse, strCompressType, &decompBody);
        rawBody = &decompBody;
    }
    if (!pbMessage->ParseFromString(*rawBody)) {
        OTS_THROW("", "OTSInvalidResponse", "fail to deserialize response");
    }
}

void OTSClientImpl::ValidateContentMD5(const HttpResponse& httpResponse)
{
    string bodyMD5;
    if (!FindInMap(httpResponse.mResponseHeaders, kContentMD5, &bodyMD5)) {
        string errorMessage = kContentMD5 + " is not found";
        OTS_THROW("", "OTSInvalidResponse", errorMessage.c_str());
    }
    string tmpPlainMD5, tmpBodyMD5;
    MD5String(httpResponse.mResponseBody, &tmpPlainMD5);
    Base64Encode(tmpPlainMD5, &tmpBodyMD5);
    if (tmpBodyMD5 != bodyMD5) {
        OTS_THROW("", "OTSInvalidResponse", "md5 of response is invalid");
    }
}

void OTSClientImpl::DecompressResponse(
            const HttpResponse& httpResponse,
            const string& compressType,
            std::string* decompBody)
{
    if (compressType != kCompressDefalte) {
        OTS_THROW("", "OTSInvalidResponse", "compress type of response is unsupported");
    }
    string strCompSize;
    if (!FindInMap(httpResponse.mResponseHeaders, kResponseCompressSize, &strCompSize)) {
        string errorMessage = kResponseCompressSize + " is not found";
        OTS_THROW("", "OTSInvalidResponse", errorMessage.c_str());
    }
    int64_t compressSize;
    if (!StringToInt64(strCompSize, &compressSize)) {
        string errorMessage = kResponseCompressSize + " is invalid integer";
        OTS_THROW("", "OTSInvalidResponse", errorMessage.c_str());
    }
    if (!DeflateDecompress(httpResponse.mResponseBody, compressSize, decompBody)) {
        OTS_THROW("", "OTSInvalidResponse", "fail to decompress response");
    }
    if (decompBody->size() != (uint64_t)compressSize) {
        OTS_THROW("", "OTSInvalidResponse", "compress size of response does not match");
    }
}

} // end of ots
} // end of openservices
} // end of aliyun
