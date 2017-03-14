#ifndef OTS_CLIENT_IMPL_H
#define OTS_CLIENT_IMPL_H

#include "http_client.h"
#include "include/ots_client.h"
#include "ots_protocol_2.pb.h"
#include "ots_retry_strategy.h"

#include <iostream>
#include <string>
#include <sys/types.h>
#include <tr1/memory>
#include <vector>

namespace aliyun {
namespace openservices {
namespace ots {

struct RequestStatus
{
    std::string mErrorCode;
    std::string mErrorMessage;
    std::string mRequestId;
    std::string mLocation;
};

class OTSClientImpl
{
    public:
    /**
     * @param otsConfig 用户自定义的配置选项。
     */
    OTSClientImpl(const OTSConfig& otsConfig);

    ~OTSClientImpl();

    /**
     * 根据CreateTableRequest创建表，包含TableMeta和CapacityUnit上限。
     *
     * @param request 请求参数。
     * @return Void。
     */
    void CreateTable(const CreateTableRequest& request);

    /**
     * 获取表的描述信息，包括表的结构和读写capacity上限。
     *
     * @param tableName 表名。
     * @param response 响应结果。
     * @return Void。
     */
    void DescribeTable(
                const std::string& tableName,
                DescribeTableResponse* response);

    /**
     * 更新表的属性信息，现在只支持修改读写Capacity上限。
     *
     * @param request 请求参数。
     * @param response 返回结果。
     * @return Void。
     */
    void UpdateTable(
                const UpdateTableRequest& request,
                UpdateTableResponse* response);

    /**
     * 列出所有的表，如果操作成功，返回所有表的名称列表。
     *
     * @param response 返回结果。
     * @return Void。
     */
    void ListTable(ListTableResponse* response);

    /**
     * 删除一个表。
     *
     * @param tableName 表名。
     * @return Void。
     */
    void DeleteTable(const std::string& tableName);

    /**
     * 获取整行数据或部分列数据。
     *
     * @param request 请求参数。
     * @param response 返回结果。
     * @return Void。
     */
    void GetRow(
                const GetRowRequest& request,
                GetRowResponse* response);

    /**
     * 插入一行数据。
     *
     * @param request 请求参数。
     * @param response 返回结果。
     * @return Void。
     */
    void PutRow(
                const PutRowRequest& request,
                PutRowResponse* response);

    /**
     * 更新一行数据。
     *
     * @param request 请求参数。
     * @param response 返回结果。
     * @return Void。
     */
    void UpdateRow(
                const UpdateRowRequest& request,
                UpdateRowResponse* response);

    /**
     * 删除一行数据。
     *
     * @param request 请求参数。
     * @param response 返回结果。
     * @return Void。
     */
    void DeleteRow(
                const DeleteRowRequest& request,
                DeleteRowResponse* response);

    /**
     * 批量查询一张表的多行数据。
     *
     * @param request 请求参数。
     * @param response 返回结果。
     * @return Void。
     */
    void BatchGetRow(
                const BatchGetRowRequest& request,
                BatchGetRowResponse* response);

    /**
     * 批量插入、更新或删除多行数据。
     *
     * @param request 请求参数。
     * @param response 返回结果。
     * @return Void。
     */
    void BatchWriteRow(
                const BatchWriteRowRequest& request,
                BatchWriteRowResponse* response);

    /**
     * 针对某张表，根据范围条件获取多行数据。
     *
     * @param request 请求参数。
     * @param response 返回结果。
     * @return Void。
     */
    void GetRange(
                const GetRangeRequest& request,
                GetRangeResponse* response);

    /**
     * 针对某张表，根据范围条件获取多行数据，结果通过iterator形式返回。
     *
     * @param request 请求参数。
     * @param consumed_capacity_unit 本次操作消耗的capacity数量之和，实时更新。
     * @param iterator 用于获取结果的迭代器。
     */
    void GetRangeByIterator(
                const GetRangeRequest& request,
                int64_t* consumed_capacity_unit,
                RowIterator* iterator,
                int32_t row_count);

    /**
     * 直接传入PB对象，执行GetRange，并返回PB对象。
     */
    void GetNextRange(
                const com::aliyun::cloudservice::ots2::GetRangeRequest* pbRequest,
                com::aliyun::cloudservice::ots2::GetRangeResponse* pbResponse);

private:
    //If parameter is null, OTSException will be thrown.  
    void EnsureNotEmptyString(const std::string& str, const std::string& name);

    void EnsureNotNullPointer(void* ptr, const std::string& name);

    //Check if the protobuf message is initialized
    void ValidHasInitialized(const google::protobuf::Message* message);

    std::string GetCompressTypeString(CompressType type) const;

    CompressType GetCompressType(const std::string& compressString) const;

    //Find a value from a map
    bool FindInMap(
                const std::map<std::string, std::string>& anyMap, 
                const std::string& key,
                std::string* value);


    //Fill all neccessary parameter of HttpRequest
    void PackHttpRequest(
                const std::string& operation,
                const google::protobuf::Message* pbMessage,
                HttpRequest* httpRequest);

    //Fill all neccessary parameter of HttpRequest
    void PackHttpRequestInternal(
                const std::string& operation,
                HttpRequest* httpRequest);

    //Generate the signature
    void CreateSignature(
                const std::string& operation, 
                HttpMethod method, 
                const std::map<std::string, std::string>& querys,
                const std::map<std::string, std::string>& headers,
                std::string* signature);

    //Handle the request without return
    void HandleRequestNoResult(
                const std::string& operation, 
                const google::protobuf::Message* pbRequest);

    //Handle the request with return
    void HandleRequestWithResult(
                const std::string& operation, 
                const google::protobuf::Message* pbRequest,
                google::protobuf::Message* pbResponse);

    //Invoke HttpClient to send HttpRequest and receive HttpResponse
    void InvokeSendRequest(
                const std::string& opeartion, 
                const HttpRequest& httpRequest, 
                HttpResponse* httpResponse);

    //Handle the response, which will not return users.
    void HandleBadRequest(
                const HttpResponse& httpResponse, 
                RequestStatus* reqStatus);

    //Hanle "301 Moved Permanently"
    void HandleMovedRequest(
                const HttpResponse& httpResponse, 
                RequestStatus* reqStatus);

    // Validate the date and signature of response.
    void ValidateResponse(
                const std::string& operation,
                const HttpResponse& httpResponse,
                RequestStatus* reqStatus);

    //Parse response body to protobuf.
    void ParseHttpResponse(
                const std::string& operation, 
                const HttpResponse& httpResponse, 
                google::protobuf::Message* pbMessage);

    void ParsePBMessage(
                const HttpResponse& httpResponse,
                google::protobuf::Message* pbMessage);

    void ValidateContentMD5(const HttpResponse& httpResponse);

    void DecompressResponse(
                const HttpResponse& httpResponse,
                const std::string& CompressType,
                std::string* decompBody);

private:
    //Configuration: the end point of service; the id and sceret of access key. 
    std::string mEndPoint;
    std::string mAccessId;
    std::string mAccessKey;
    std::string mInstanceName;

    //Compress Type for request and response
    CompressType mRequestCompressType;
    CompressType mResponseCompressType;

    //HttpClient, send request and receive response
    HttpClientPtr mHttpClientPtr;

    //RetryStrategy
    OTSRetryStrategyPtr mRetryStrategyPtr;
};

} // end of ots
} // end of openservices
} // end of aliyun

#endif

