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

#include "../protocol/table_store_filter.pb.h"
#include "../protocol/table_store.pb.h"
#include "ots_condition.hpp"
#include "core/types.hpp"
#include "util/mempiece.hpp"
#include <string>
#include <tr1/memory>
#include <deque>
#include <stdint.h>

using namespace com::aliyun::tablestore;

namespace aliyun {
namespace tablestore {
namespace core {

typedef std::tr1::shared_ptr<google::protobuf::Message> MessagePtr;

void ToError(const protocol::Error&, Error*);

protocol::PrimaryKeyType ToPrimaryKeyType(PrimaryKeyType type);
PrimaryKeyType ToPrimaryKeyType(protocol::PrimaryKeyType type);

protocol::PrimaryKeyOption ToPrimaryKeyOption(PrimaryKeyOption option);
PrimaryKeyOption ToPrimaryKeyOption(protocol::PrimaryKeyOption option);
    
// TableMeta 
void ToTableMeta(const TableMeta&, protocol::TableMeta*);
void ToTableMeta(const protocol::TableMeta&, TableMeta*);

void ToCapacityUnit(const CapacityUnit&, protocol::ReservedThroughput*);
void ToCapacityUnit(const protocol::CapacityUnit&, CapacityUnit*);

void ToTableOptions(const TableOptions&, protocol::TableOptions*);
void ToTableOptions(const protocol::TableOptions&, TableOptions&);
void ToTimeRange(const TimeRange&, protocol::TimeRange*);

protocol::Direction ToDirection(RangeQueryCriterion::Direction);

protocol::ReturnType ToReturnType(RowChange::ReturnType);

// filter
protocol::ComparatorType ToComparatorType(SingleColumnCondition::Relation);
protocol::LogicalOperator ToLogicalOperator(CompositeColumnCondition::Operator);
protocol::FilterType ToFilterType(ColumnCondition::Type);
protocol::RowExistenceExpectation ToRowExistence(Condition::RowExistenceExpectation);
std::string ToSingleCondition(const SingleColumnCondition*);
std::string ToCompositeCondition(const CompositeColumnCondition*);
std::string ToFilter(const ColumnCondition*);

// Error
void ParseErrorResponse(Error*, const std::string&);

template<class Req, class Resp>
class ProtocolBuilder {};

template<>
class ProtocolBuilder<CreateTableRequest, CreateTableResponse>
{
public:
    MessagePtr serialize(const CreateTableRequest&);
    void parse(const std::string&);
    void deserialize(CreateTableResponse*);

private:
    MessagePtr mPbResult;
};

template<>
class ProtocolBuilder<ListTableRequest, ListTableResponse>
{
public:
    MessagePtr serialize(const ListTableRequest&);
    void parse(const std::string&);
    void deserialize(ListTableResponse*);

private:
    MessagePtr mPbResult;
};

template<>
class ProtocolBuilder<DeleteTableRequest, DeleteTableResponse>
{
public:
    MessagePtr serialize(const DeleteTableRequest&);
    void parse(const std::string&);
    void deserialize(DeleteTableResponse*);

private:
    MessagePtr mPbResult;
};

template<>
class ProtocolBuilder<DescribeTableRequest, DescribeTableResponse>
{
public:
    MessagePtr serialize(const DescribeTableRequest&);
    void parse(const std::string&);
    void deserialize(DescribeTableResponse*);

private:
    MessagePtr mPbResult;
};

template<>
class ProtocolBuilder<UpdateTableRequest, UpdateTableResponse>
{
public:
    MessagePtr serialize(const UpdateTableRequest&);
    void parse(const std::string&);
    void deserialize(UpdateTableResponse*);

private:
    MessagePtr mPbResult;
};

template<>
class ProtocolBuilder<ComputeSplitsBySizeRequest, ComputeSplitsBySizeResponse>
{
public:
    MessagePtr serialize(const ComputeSplitsBySizeRequest&);
    void parse(const std::string&);
    void deserialize(ComputeSplitsBySizeResponse*);

private:
    MessagePtr mPbResult;
};

template<>
class ProtocolBuilder<PutRowRequest, PutRowResponse>
{
public:
    MessagePtr serialize(const PutRowRequest&);
    void parse(const std::string&);
    void deserialize(PutRowResponse*);

private:
    MessagePtr mPbResult;
};

template<>
class ProtocolBuilder<GetRowRequest, GetRowResponse>
{
public:
    MessagePtr serialize(const GetRowRequest&);
    void parse(const std::string&);
    void deserialize(GetRowResponse*);

private:
    MessagePtr mPbResult;
};

template<>
class ProtocolBuilder<GetRangeRequest, GetRangeResponse>
{
public:
    MessagePtr serialize(const GetRangeRequest&);
    void parse(const std::string&);
    void deserialize(GetRangeResponse*);

private:
    MessagePtr mPbResult;
};

template<>
class ProtocolBuilder<UpdateRowRequest, UpdateRowResponse>
{
public:
    MessagePtr serialize(const UpdateRowRequest&);
    void parse(const std::string&);
    void deserialize(UpdateRowResponse*);

private:
    MessagePtr mPbResult;
};

template<>
class ProtocolBuilder<DeleteRowRequest, DeleteRowResponse>
{
public:
    MessagePtr serialize(const DeleteRowRequest&);
    void parse(const std::string&);
    void deserialize(DeleteRowResponse*);

private:
    MessagePtr mPbResult;
};

template<>
class ProtocolBuilder<BatchGetRowRequest, BatchGetRowResponse>
{
public:
    explicit ProtocolBuilder() : mRequest(NULL) {}

    MessagePtr serialize(const BatchGetRowRequest&);
    void parse(const std::string&);
    void deserialize(BatchGetRowResponse*);

private:
    MessagePtr mPbResult;
    const BatchGetRowRequest* mRequest;
};

template<>
class ProtocolBuilder<BatchWriteRowRequest, BatchWriteRowResponse>
{
public:
    struct Index
    {
        std::deque<int64_t> mPuts;
        std::deque<int64_t> mUpdates;
        std::deque<int64_t> mDeletes;
    };

    typedef std::map<std::string, Index, util::QuasiLexicographicLess<std::string> > Indice;

    explicit ProtocolBuilder() : mRequest(NULL) {}

    MessagePtr serialize(const BatchWriteRowRequest&);
    void parse(const std::string&);
    void deserialize(BatchWriteRowResponse*);

private:
    MessagePtr mPbResult;
    const BatchWriteRowRequest* mRequest;
    Indice mIndice;
};

} // namespace core
} // namespace tablestore
} // namespace aliyun

