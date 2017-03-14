#ifndef OTS_STATIC_INDEX_CLIENT_H
#define OTS_STATIC_INDEX_CLIENT_H

#include "exceptional.h"
#include "ots/ots_client.h"
#include <string>

namespace static_index {

class Logger;
class PutRowRequest;
class PutRowResponse;
class BatchWriteRequest;
class BatchWriteResponse;
class GetRangeRequest;
class GetRangeResponse;
class BatchGetRowRequest;
class BatchGetRowResponse;

class ClientDelegate
{
private:
    ClientDelegate(const ClientDelegate&);
    ClientDelegate& operator=(const ClientDelegate&);

public:
    ClientDelegate() {}
    static ClientDelegate* New(
        Logger*, const aliyun::openservices::ots::OTSConfig& otsConfig);

    virtual ~ClientDelegate() {}

    /**
     * 获取整行数据或部分列数据。
     *
     * @param request 请求参数。
     * @param response 返回结果。
     * @return Void。
     */
    // virtual void GetRow(
    //     const aliyun::openservices::ots::GetRowRequest& request,
    //     aliyun::openservices::ots::GetRowResponse* response) =0;

    /**
     * 插入一行数据。
     */
    virtual Exceptional PutRow(PutRowResponse*, const PutRowRequest&) =0;

    // /**
    //  * 更新一行数据。
    //  *
    //  * @param request 请求参数。
    //  * @param response 返回结果。
    //  * @return Void。
    //  */
    // virtual void UpdateRow(
    //     const aliyun::openservices::ots::UpdateRowRequest& request,
    //     aliyun::openservices::ots::UpdateRowResponse* response) =0;

    // /**
    //  * 删除一行数据。
    //  *
    //  * @param request 请求参数。
    //  * @param response 返回结果。
    //  * @return Void。
    //  */
    // virtual void DeleteRow(
    //     const aliyun::openservices::ots::DeleteRowRequest& request,
    //     aliyun::openservices::ots::DeleteRowResponse* response) =0;

    /**
     * 批量查询一张表的多行数据。
     *
     * @param request 请求参数。
     * @param response 返回结果。
     * @return Void。
     */
    virtual Exceptional BatchGetRow(BatchGetRowResponse*, const BatchGetRowRequest&) =0;

    /**
     * 批量插入、更新或删除多行数据。
     */
    virtual Exceptional BatchWrite(BatchWriteResponse*, const BatchWriteRequest&) =0;

    /**
     * 针对某张表，根据范围条件获取多行数据。
     *
     * @param request 请求参数。
     * @param response 返回结果。
     * @return Void。
     */
    virtual Exceptional GetRange(GetRangeResponse*, const GetRangeRequest&) =0;
};

} // namespace static_index


#endif /* OTS_STATIC_INDEX_CLIENT_H */

