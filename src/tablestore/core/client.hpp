#pragma once
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
#include "tablestore/core/types.hpp"
#include "tablestore/core/error.hpp"
#include "tablestore/util/optional.hpp"
#include "tablestore/util/iterator.hpp"
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
    virtual ~ISyncClient() {}

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
    virtual util::Optional<Error> listTable(
        ListTableResponse*, const ListTableRequest&) =0;
    virtual util::Optional<Error> createTable(
        CreateTableResponse*, const CreateTableRequest&) =0;
    virtual util::Optional<Error> deleteTable(
        DeleteTableResponse*, const DeleteTableRequest&) =0;
    virtual util::Optional<Error> describeTable(
        DescribeTableResponse*, const DescribeTableRequest&) =0;
    virtual util::Optional<Error> updateTable(
        UpdateTableResponse*, const UpdateTableRequest&) =0;

    // point write
    virtual util::Optional<Error> putRow(PutRowResponse*, const PutRowRequest&) =0;
    virtual util::Optional<Error> updateRow(
        UpdateRowResponse*, const UpdateRowRequest&) =0;
    virtual util::Optional<Error> deleteRow(
        DeleteRowResponse*, const DeleteRowRequest&) =0;

    /**
     * Write a batch of rows.
     *
     * If there occurs request-level error, returns the return;
     * If there are row-level errors, put them into their corresponding
     * result of row.
     */
    virtual util::Optional<Error> batchWriteRow(
        BatchWriteRowResponse*, const BatchWriteRowRequest&) =0;

    // // point query
    virtual util::Optional<Error> getRow(GetRowResponse*, const GetRowRequest&) =0;

    /**
     * Fetch a batch of rows.
     *
     * If there occurs request-level error, returns the return;
     * If there are row-level errors, put them into their corresponding
     * result of row.
     */
    virtual util::Optional<Error> batchGetRow(
        BatchGetRowResponse*, const BatchGetRowRequest&) =0;

    // range query
    /**
     * Create an iterator which scans the range.
     *
     * Caveats:
     * 1. User should take the ownership of the iterator.
     */
    virtual util::Optional<Error> getRangeIterator(
        util::Iterator<util::MoveHolder<Row>, Error>**, const GetRangeRequest&) =0;

    /**
     * A single turn-around of GetRange.
     * Strongly recommend use getRangeIterator() instead.
     */
    virtual util::Optional<Error> getRange(
        GetRangeResponse*, const GetRangeRequest&) =0;

    // other operations
    virtual util::Optional<Error> computeSplitsBySize(
        ComputeSplitsBySizeResponse*, const ComputeSplitsBySizeRequest&) =0;
};

} // namespace core
} // namespace tablestore
} // namespace aliyun
