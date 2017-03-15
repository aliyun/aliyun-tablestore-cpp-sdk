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
#pragma once

#include "types.hpp"
#include "error.hpp"
#include "util/optional.hpp"
#include <string>

namespace aliyun {
namespace tablestore {
namespace core {

/**
 * An interface to synchronized client to aliyun TableStore.
 */
class ISyncClient
{
public:
    virtual ISyncClient() {}

    /**
     * Create a synchronized client for production use.
     * 
     * If no error is detected during creation, @p result will be set to
     * the client; otherwise, the error will be returned and 
     * @p result keep untouched.
     */
    static util::Optional<Error> create(
        ISyncClient** result,
        const Endpoint&, const Credential&, const ClientOptions&);


    // table operations
    virtual util::Optional<Error> createTable(CreateTableResponse*, const CreateTableRequest&) =0;

    // data operations
    

    // /**
    //  * 获取该实例的所有表。
    //  */
    // ListTableResultPtr ListTable()
    //     throw (OTSException, OTSClientException);

    // /**
    //  * 获取一张表的描述信息。
    //  *
    //  * @param requestPtr
    //  *      请求参数。
    //  */
    // DescribeTableResultPtr DescribeTable(const DescribeTableRequestPtr& requestPtr)
    //     throw (OTSException, OTSClientException);

    // /**
    //  * 根据大小近似地获取表的切分点。
    //  *
    //  * @param requestPtr
    //  *      请求参数。
    //  */
    // ComputeSplitsBySizeResultPtr ComputeSplitsBySize(const ComputeSplitsBySizeRequestPtr& requestPtr)
    //     throw (OTSException, OTSClientException);

    // /**
    //  * 删除一张表。
    //  *
    //  * @param requestPtr
    //  *      请求参数。
    //  */
    // DeleteTableResultPtr DeleteTable(const DeleteTableRequestPtr& requestPtr)
    //     throw (OTSException, OTSClientException);

    // /**
    //  * 更新一张表的信息。
    //  *
    //  * @param requestPtr
    //  *      请求参数。
    //  */
    // UpdateTableResultPtr UpdateTable(const UpdateTableRequestPtr& requestPtr)
    //     throw (OTSException, OTSClientException);

    // /**
    //  * 获取一行。
    //  *
    //  * @param requestPtr
    //  *      请求参数。
    //  */
    // GetRowResultPtr GetRow(const GetRowRequestPtr& requestPtr)
    //     throw (OTSException, OTSClientException);

    // /**
    //  * 插入一行。
    //  *
    //  * @param requestPtr
    //  *      请求参数。
    //  */
    // PutRowResultPtr PutRow(const PutRowRequestPtr& requestPtr)
    //     throw (OTSException, OTSClientException);

    // /**
    //  * 更新一行。
    //  *
    //  * @param requestPtr
    //  *      请求参数。
    //  */
    // UpdateRowResultPtr UpdateRow(const UpdateRowRequestPtr& requestPtr)
    //     throw (OTSException, OTSClientException);

    // /**
    //  * 删除一行。
    //  *
    //  * @param requestPtr
    //  *      请求参数。
    //  */
    // DeleteRowResultPtr DeleteRow(const DeleteRowRequestPtr& requestPtr)
    //     throw (OTSException, OTSClientException);

    // /**
    //  * 批量获取多行。
    //  *
    //  * @param requestPtr
    //  *      请求参数。
    //  */
    // BatchGetRowResultPtr BatchGetRow(const BatchGetRowRequestPtr& requestPtr)
    //     throw (OTSException, OTSClientException);

    // /**
    //  * 批量写入多行。
    //  *
    //  * @param requestPtr
    //  *      请求参数。
    //  */
    // BatchWriteRowResultPtr BatchWriteRow(const BatchWriteRowRequestPtr& requestPtr)
    //     throw (OTSException, OTSClientException);

    // /**
    //  * 范围查询。
    //  *
    //  * @param requestPtr
    //  *      请求参数。
    //  */
    // GetRangeResultPtr GetRange(const GetRangeRequestPtr& requestPtr)
    //     throw (OTSException, OTSClientException);

    // /**
    //  * 创建范围查询迭代器。
    //  *
    //  * @param getRangeRequestPtr
    //  *      请求参数。
    //  */
    // RowIteratorPtr GetRangeIterator(const GetRangeRequestPtr& requestPtr)
    //     throw (OTSException, OTSClientException);
};

} // namespace core
} // namespace tablestore
} // namespace aliyun
