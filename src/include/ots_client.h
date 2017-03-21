/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#ifndef OTS_CLIENT_H
#define OTS_CLIENT_H

#include "ots_common.h"
#include "ots_config.h"
#include "ots_exception.h"
#include "ots_request.h"
#include <string>

namespace aliyun {
namespace tablestore {

class OTSClient
{
public:
    
    /**
     * 根据配置创建一个OTSClient实例。
     *
     * @param endpoint
     *      OTS服务的endpoint。
     * @param accessKeyId
     *      访问OTS服务的Access ID。
     * @param accessKeySecret
     *      访问OTS服务的Access Key。
     * @param instanceName
     *      访问OTS服务的实例。 
     */
    OTSClient(
        const std::string& endpoint,
        const std::string& accessKeyId,
        const std::string& accessKeySecret,
        const std::string& instanceName);

    /**
     * 根据配置创建一个OTSClient实例。
     *
     * @param endpoint
     *      OTS服务的endpoint。
     * @param accessKeyId
     *      访问OTS服务的Access ID。
     * @param accessKeySecret
     *      访问OTS服务的Access Key。
     * @param instanceName
     *      访问OTS服务的实例。 
     * @param clientConfig
     *      客户端配置信息。
     */
    OTSClient(
        const std::string& endpoint,
        const std::string& accessKeyId,
        const std::string& accessKeySecret,
        const std::string& instanceName,
        const ClientConfiguration& clientConfig);

    /**
     * 根据配置创建一个OTSClient实例。
     *
     * @param endpoint
     *      OTS服务的endpoint。
     * @param instanceName
     *      访问OTS服务的实例。 
     * @param auth
     *      认证信息
     * @param clientConfig
     *      客户端配置信息。
     */
    OTSClient(
        const std::string& endpoint,
        const std::string& instanceName,
        const Credential& auth,
        const ClientConfiguration& clientConfig);

    ~OTSClient();

    /**
     * 创建一张表。
     *
     * @param requestPtr
     *      请求参数。
     */
    CreateTableResultPtr CreateTable(const CreateTableRequestPtr& requestPtr)
        throw (OTSException, OTSClientException);

    /**
     * 获取该实例的所有表。
     */
    ListTableResultPtr ListTable()
        throw (OTSException, OTSClientException);

    /**
     * 获取一张表的描述信息。
     *
     * @param requestPtr
     *      请求参数。
     */
    DescribeTableResultPtr DescribeTable(const DescribeTableRequestPtr& requestPtr)
        throw (OTSException, OTSClientException);

    /**
     * 根据大小近似地获取表的切分点。
     *
     * @param requestPtr
     *      请求参数。
     */
    ComputeSplitsBySizeResultPtr ComputeSplitsBySize(const ComputeSplitsBySizeRequestPtr& requestPtr)
        throw (OTSException, OTSClientException);

    /**
     * 删除一张表。
     *
     * @param requestPtr
     *      请求参数。
     */
    DeleteTableResultPtr DeleteTable(const DeleteTableRequestPtr& requestPtr)
        throw (OTSException, OTSClientException);

    /**
     * 更新一张表的信息。
     *
     * @param requestPtr
     *      请求参数。
     */
    UpdateTableResultPtr UpdateTable(const UpdateTableRequestPtr& requestPtr)
        throw (OTSException, OTSClientException);

    /**
     * 获取一行。
     *
     * @param requestPtr
     *      请求参数。
     */
    GetRowResultPtr GetRow(const GetRowRequestPtr& requestPtr)
        throw (OTSException, OTSClientException);

    /**
     * 插入一行。
     *
     * @param requestPtr
     *      请求参数。
     */
    PutRowResultPtr PutRow(const PutRowRequestPtr& requestPtr)
        throw (OTSException, OTSClientException);

    /**
     * 更新一行。
     *
     * @param requestPtr
     *      请求参数。
     */
    UpdateRowResultPtr UpdateRow(const UpdateRowRequestPtr& requestPtr)
        throw (OTSException, OTSClientException);

    /**
     * 删除一行。
     *
     * @param requestPtr
     *      请求参数。
     */
    DeleteRowResultPtr DeleteRow(const DeleteRowRequestPtr& requestPtr)
        throw (OTSException, OTSClientException);

    /**
     * 批量获取多行。
     *
     * @param requestPtr
     *      请求参数。
     */
    BatchGetRowResultPtr BatchGetRow(const BatchGetRowRequestPtr& requestPtr)
        throw (OTSException, OTSClientException);

    /**
     * 批量写入多行。
     *
     * @param requestPtr
     *      请求参数。
     */
    BatchWriteRowResultPtr BatchWriteRow(const BatchWriteRowRequestPtr& requestPtr)
        throw (OTSException, OTSClientException);

    /**
     * 范围查询。
     *
     * @param requestPtr
     *      请求参数。
     */
    GetRangeResultPtr GetRange(const GetRangeRequestPtr& requestPtr)
        throw (OTSException, OTSClientException);

    /**
     * 创建范围查询迭代器。
     *
     * @param getRangeRequestPtr
     *      请求参数。
     */
    RowIteratorPtr GetRangeIterator(const GetRangeRequestPtr& requestPtr)
        throw (OTSException, OTSClientException);

private:

    void* mClientImpl;
};

} // end of tablestore
} // end of aliyun

#endif
