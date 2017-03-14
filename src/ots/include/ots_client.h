#ifndef OTS_CLIENT_H
#define OTS_CLIENT_H

#include "ots_exception.h"
#include "ots_factory.h"
#include "ots_types.h"

#include <string>
#include <stdint.h>

namespace aliyun {
namespace openservices {
namespace ots {

/**
 * 请求数据和响应数据的压缩类型。
 */
enum CompressType
{
    /* 无压缩。*/
    COMPRESS_NO = 0,
    /* 使用zlib提供的deflate算法进行压缩。 */
    COMPRESS_DEFLATE = 1
};

/**
 * OTSClient的配置选项，目前包括：
 * 1. 请求超时时间，默认为30s；
 * 2. 最大重试次数，默认为30s，仅针对读接口；
 * 3. 请求内容的压缩类型，默认为COMPRESS_NO；
 * 4. 响应内容的压缩类型，默认为COMPRESS_NO。
 */
class OTSConfig
{
public:
    OTSConfig();

    /**
     * OTS服务地址，如http://10.10.10.10:80/
     */
    std::string mEndPoint;

    /**
     * 访问OTS服务使用的AccessId。
     */
    std::string mAccessId;

    /**
     * 访问OTS服务使用的AccessKey。
     */
    std::string mAccessKey;

    /**
     * 想要访问的InstanceName。
     */
    std::string mInstanceName;

    /**
     * 请求超时时间（单位秒），默认为30s，如果为0表示永不超时。
     */
    int32_t mRequestTimeout;

    /**
     * 最大错误重试次数，默认为3次，仅针对读接口。
     */
    int32_t mMaxErrorRetry;

    /**
     * 请求内容的压缩类型，目前仅支持COMPRESS_NO和COMPRESS_DEFLATE。
     */
    CompressType mRequestCompressType;

    /**
     * 响应内容的压缩类型，目前仅支持COMPRESS_NO和COMPRESS_DEFLATE。
     */
    CompressType mResponseCompressType;
};

/**
 * @class OTSClient
 * OTSClient支持的操作包括：创建/删除表、获取表的结构、调整表的Capacity上限、插入/
 * 获取/更新/删除一行数据、批量修改多行数据、根据范围条件获取多行数据。
 */
class OTSClient : private Uncopyable
{
public:
    OTSClient(const OTSConfig& otsConfig);

    ~OTSClient();

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
     * @param row_count 表示获取该范围内的行数。默认为0，表示无限制。
     */
    void GetRangeByIterator(
                const GetRangeRequest& request, 
                int64_t* consumed_read_capacity, 
                RowIterator* iterator,
                int32_t row_count = 0);

private:
    void* mClientImpl;
};

} //end of ots
} //end of openservices
} //end of aliyun

#endif
