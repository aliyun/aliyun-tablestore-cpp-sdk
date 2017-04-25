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
#include "ots_protocol_builder.hpp"
#include "../plainbuffer/plain_buffer_builder.hpp"
#include "ots_exception.hpp"
#include "util/prettyprint.hpp"
#include "util/assert.hpp"
#include "util/move.hpp"
#include "util/timestamp.hpp"
#include "util/foreach.hpp"
#include <map>
#include <deque>

using namespace std;
using namespace std::tr1;
using namespace google::protobuf;
using namespace com::aliyun::tablestore;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {

void ToError(
    const protocol::Error& pbError,
    Error* error)
{
    *error->mutableErrorCode() = pbError.code();
    if (pbError.has_message()) {
        *error->mutableMessage() = pbError.message();
    }
}

protocol::PrimaryKeyType ToPrimaryKeyType(PrimaryKeyType type)
{
    switch (type) {
    case PKT_INTEGER: return protocol::INTEGER;
    case PKT_STRING: return protocol::STRING;
    case PKT_BINARY: return protocol::BINARY;
    }
    throw OTSClientException(
        "Unsupported primary key type: " + pp::prettyPrint(type));
}

protocol::PrimaryKeyOption ToPrimaryKeyOption(
    PrimaryKeyOption option)
{
    switch (option) {
    case PKO_AUTO_INCREMENT: return protocol::AUTO_INCREMENT;
    case PKO_NONE:
        OTS_ASSERT(false)(option);
    }
    throw OTSClientException(
        "Unsupported primary key option: " + pp::prettyPrint(option));
}

PrimaryKeyType ToPrimaryKeyType(protocol::PrimaryKeyType type)
{
    switch (type) {
    case protocol::INTEGER: return PKT_INTEGER;
    case protocol::STRING: return PKT_STRING;
    case protocol::BINARY: return PKT_BINARY;
    }
    throw OTSClientException(
        "Unsupported primary key type: " + pp::prettyPrint((int) type));
}

PrimaryKeyOption ToPrimaryKeyOption(
    protocol::PrimaryKeyOption option)
{
    switch (option) {
    case protocol::AUTO_INCREMENT: return PKO_AUTO_INCREMENT;
    }
    throw OTSClientException(
        "Unsupported primary key option: " + pp::prettyPrint((int) option));
}

void ToTableMeta(
    const TableMeta& tableMeta,
    protocol::TableMeta* pbTableMeta)
{
    pbTableMeta->set_table_name(tableMeta.tableName());

    // Primary key
    const Schema& schema = tableMeta.schema();
    for(int64_t i = 0, sz = schema.size(); i < sz; ++i) {
        protocol::PrimaryKeySchema* primaryKeySchema = pbTableMeta->add_primary_key();
        primaryKeySchema->set_name(schema[i].name());
        primaryKeySchema->set_type(ToPrimaryKeyType(schema[i].type()));
        if (schema[i].option() != PKO_NONE) {
            primaryKeySchema->set_option(ToPrimaryKeyOption(schema[i].option()));
        }
    }
}

void ToTableMeta(
    const protocol::TableMeta& pb,
    TableMeta* result)
{
    *result->mutableTableName() = pb.table_name();

    for(int64_t i = 0, sz = pb.primary_key_size(); i < sz; ++i) {
        const protocol::PrimaryKeySchema& primaryKeySchema = pb.primary_key(i);
        Schema& schema = *result->mutableSchema();
        *schema.append().mutableName() = primaryKeySchema.name();
        *schema.back().mutableType() = ToPrimaryKeyType(primaryKeySchema.type());
        if (primaryKeySchema.has_option()) {
            *schema.back().mutableOption() =
                ToPrimaryKeyOption(primaryKeySchema.option());
        }
    }
}

void ToCapacityUnit(
    const CapacityUnit& throughput,
    protocol::ReservedThroughput* pbThroughput) 
{
    protocol::CapacityUnit* pbCapacityUnit = pbThroughput->mutable_capacity_unit();
    if (throughput.read().present()) {
        pbCapacityUnit->set_read(*throughput.read());
    }
    if (throughput.write().present()) {
        pbCapacityUnit->set_write(*throughput.write());
    }
}

void ToCapacityUnit(
    const protocol::CapacityUnit& pbThroughput,
    CapacityUnit* capacity)
{
    if (pbThroughput.has_read()) {
        *capacity->mutableRead() = pbThroughput.read();
    }
    if (pbThroughput.has_write()) {
        *capacity->mutableWrite() = pbThroughput.write();
    }
}

void ToTableOptions(
    const TableOptions& options,
    protocol::TableOptions* pbOptions)
{
    if (options.timeToLive().present()) {
        pbOptions->set_time_to_live(options.timeToLive()->toSec());
    } else {
        pbOptions->set_time_to_live(-1);
    }
    if (options.maxVersions().present()) {
        pbOptions->set_max_versions(*options.maxVersions());
    }
    if (options.bloomFilterType().present()) {
        pbOptions->set_bloom_filter_type((protocol::BloomFilterType)(*options.bloomFilterType()));
    }
    if (options.blockSize().present()) {
        pbOptions->set_block_size(*options.blockSize());
    }
    if (options.maxTimeDeviation().present()) {
        pbOptions->set_deviation_cell_version_in_sec(options.maxTimeDeviation()->toSec());
    }
}

void ToTableOptions(
    const protocol::TableOptions& pbOptions,
    TableOptions& options) 
{
    if (pbOptions.has_time_to_live()) {
        *options.mutableTimeToLive() = Duration::fromSec(pbOptions.time_to_live());
    }
    if (pbOptions.has_max_versions()) {
        *options.mutableMaxVersions() = pbOptions.max_versions();
    }
    if (pbOptions.has_bloom_filter_type()) {
        *options.mutableBloomFilterType() =
            static_cast<BloomFilterType>(pbOptions.bloom_filter_type());
    }
    if (pbOptions.has_block_size()) {
        *options.mutableBlockSize() = pbOptions.block_size();
    }
    if (pbOptions.has_deviation_cell_version_in_sec()) {
        *options.mutableMaxTimeDeviation() = Duration::fromSec(
            pbOptions.deviation_cell_version_in_sec());
    }
}

void ToTimeRange(
    const TimeRange& timeRange,
    protocol::TimeRange* pbTimeRange)
{
    int64_t tsStart = timeRange.start().toMsec();
    int64_t tsEnd = timeRange.end().toMsec();
    if (tsStart + 1 == tsEnd) {
        pbTimeRange->set_specific_time(tsStart);
    } else {
        pbTimeRange->set_start_time(tsStart);
        pbTimeRange->set_end_time(tsEnd);
    }
}

protocol::Direction ToDirection(
    RangeQueryCriterion::Direction direction)
{
    switch (direction) {
    case RangeQueryCriterion::FORWARD:
        return protocol::FORWARD;
    case RangeQueryCriterion::BACKWARD:
        return protocol::BACKWARD;
    }
    throw OTSClientException(
        "Unsupported direction: " + pp::prettyPrint(direction));
}

// about filter
protocol::ComparatorType ToComparatorType(
    SingleColumnCondition::Relation rel)
{
    switch (rel) {
    case SingleColumnCondition::EQUAL: return protocol::CT_EQUAL;
    case SingleColumnCondition::NOT_EQUAL: return protocol::CT_NOT_EQUAL;
    case SingleColumnCondition::GREATER_THAN: return protocol::CT_GREATER_THAN;
    case SingleColumnCondition::GREATER_EQUAL: return protocol::CT_GREATER_EQUAL;
    case SingleColumnCondition::LESS_THAN: return protocol::CT_LESS_THAN;
    case SingleColumnCondition::LESS_EQUAL: return protocol::CT_LESS_EQUAL;
    }
    throw OTSClientException(
        "Unsupported compare operator: " + pp::prettyPrint(rel));
}

protocol::LogicalOperator ToLogicalOperator(
    CompositeColumnCondition::Operator op)
{
    switch (op) {
    case CompositeColumnCondition::NOT: return protocol::LO_NOT;
    case CompositeColumnCondition::AND: return protocol::LO_AND;
    case CompositeColumnCondition::OR: return protocol::LO_OR;
    }
    throw OTSClientException(
        "Unsupported logic operator: " + pp::prettyPrint(op));
}

protocol::FilterType ToFilterType(
    ColumnCondition::Type cct)
{
    switch (cct) {
    case COMPOSITE_COLUMN_CONDITION:
        return protocol::FT_COMPOSITE_COLUMN_VALUE;
    case SINGLE_COLUMN_CONDITION:
        return protocol::FT_SINGLE_COLUMN_VALUE;
    }
    throw OTSClientException(
        "Unsupported filter type: " + pp::prettyPrint(cct));
}

protocol::RowExistenceExpectation ToRowExistence(
    Condition::RowExistenceExpectation exp)
{
    switch (exp) {
    case Condition::IGNORE: return protocol::IGNORE;
    case Condition::EXPECT_EXIST: return protocol::EXPECT_EXIST;
    case Condition::EXPECT_NOT_EXIST: return protocol::EXPECT_NOT_EXIST;
    }
    throw OTSClientException(
        "Unsupported RowExistenceExpectation type: " + pp::prettyPrint(exp));
}

protocol::ReturnType ToReturnType(RowChange::ReturnType rt)
{
    switch (rt) {
    case RowChange::RT_NONE: return protocol::RT_NONE;
    case RowChange::RT_PRIMARY_KEY: return protocol::RT_PK;
    }
    throw OTSClientException("Unsupported ReturnContent: " + pp::prettyPrint(rt));
}

string ToSingleCondition(const SingleColumnCondition* cc)
{
    protocol::SingleColumnValueFilter pbRelationFilter;
    pbRelationFilter.set_column_name(cc->columnName());
    pbRelationFilter.set_comparator(ToComparatorType(cc->relation()));
    string valueBuffer = PlainBufferBuilder::SerializeColumnValue(cc->columnValue());
    pbRelationFilter.set_column_value(valueBuffer);
    pbRelationFilter.set_filter_if_missing(!cc->passIfMissing());
    pbRelationFilter.set_latest_version_only(cc->latestVersionOnly());
    string relationFilterStr;
    if (!pbRelationFilter.SerializeToString(&relationFilterStr)) {
        throw OTSClientException("Invalid filter.");
    }
    return relationFilterStr;
}

string ToCompositeCondition(
    const CompositeColumnCondition* cc)
{
    protocol::CompositeColumnValueFilter pbCompositeFilter;
    pbCompositeFilter.set_combinator(ToLogicalOperator(cc->op()));
    for(int64_t i = 0, sz = cc->children().size(); i < sz; ++i) {
        const ColumnCondition& subcc = *(cc->children()[i]);
        protocol::Filter* pbSubFilter = pbCompositeFilter.add_sub_filters();
        pbSubFilter->set_type(ToFilterType(subcc.type()));
        switch(subcc.type()) {
        case ColumnCondition::SINGLE:
            pbSubFilter->set_filter(ToSingleCondition(
                    dynamic_cast<const SingleColumnCondition*>(&subcc)));
            break;
        case ColumnCondition::COMPOSITE:
            pbSubFilter->set_filter(ToCompositeCondition(
                    dynamic_cast<const CompositeColumnCondition*>(&subcc)));
            break;
        }
    }
    string compositeFilterStr;
    if (!pbCompositeFilter.SerializeToString(&compositeFilterStr)) {
        throw OTSClientException("Invalid filter.");
    }
    return compositeFilterStr;
}

string ToFilter(const ColumnCondition* cc)
{
    protocol::Filter pbFilter;
    pbFilter.set_type(ToFilterType(cc->type()));
    switch(cc->type()) {
    case ColumnCondition::SINGLE:
        pbFilter.set_filter(ToSingleCondition(
                dynamic_cast<const SingleColumnCondition*>(cc)));
        break;
    case ColumnCondition::COMPOSITE:
        pbFilter.set_filter(ToCompositeCondition(
                dynamic_cast<const CompositeColumnCondition*>(cc)));
        break;
    }
    string filterStr;
    if (!pbFilter.SerializeToString(&filterStr)) {
        throw OTSClientException("Invalid filter.");
    }
    return filterStr;
}

void ParseErrorResponse(
    Error* error,
    const std::string& responseBody)
{
    auto_ptr<protocol::Error> pbError(new protocol::Error());
    if (!pbError->ParseFromString(responseBody)) {
        throw OTSClientException("Invalid response body.");
    }
    *error->mutableErrorCode() = pbError->code();
    *error->mutableMessage() = pbError->message();
}


MessagePtr ProtocolBuilder<CreateTableRequest, CreateTableResponse>::serialize(
    const CreateTableRequest& req)
{
    protocol::CreateTableRequest* pbRequest = new protocol::CreateTableRequest();
    MessagePtr ret(pbRequest);

    ToTableMeta(req.meta(), pbRequest->mutable_table_meta());
    if (req.options().reservedThroughput().present()) {
        ToCapacityUnit(*req.options().reservedThroughput(), pbRequest->mutable_reserved_throughput());
    }
    ToTableOptions(req.options(), pbRequest->mutable_table_options());
    if (req.shardSplitPoints().size() > 0) {
        const IVector<PrimaryKey>& shardSplits = req.shardSplitPoints();
        string lastStr = PlainBufferBuilder::SerializePrimaryKeyValue(PrimaryKeyValue::toInfMin());
        for(int64_t i = 0, sz = shardSplits.size(); i < sz; ++i) {
            const PrimaryKey& cur = shardSplits[i];
            protocol::PartitionRange *p = pbRequest->add_partitions();
            string curStr = PlainBufferBuilder::SerializePrimaryKeyValue(cur[0].value());
            p->set_begin(lastStr);
            p->set_end(curStr);
            lastStr = curStr;
        }
        string infmaxStr = PlainBufferBuilder::SerializePrimaryKeyValue(PrimaryKeyValue::toInfMin());
        protocol::PartitionRange *p = pbRequest->add_partitions();
        p->set_begin(lastStr);
        p->set_end(infmaxStr);
    }

    return ret;
}

void ProtocolBuilder<CreateTableRequest, CreateTableResponse>::parse(
    const std::string& responseBody)
{
    protocol::CreateTableResponse* pbResponse =
        new protocol::CreateTableResponse();
    mPbResult.reset(pbResponse);
    if (!pbResponse->ParseFromString(responseBody)) {
        throw OTSClientException("Invalid response body.");
    }
}

void ProtocolBuilder<CreateTableRequest, CreateTableResponse>::deserialize(
    CreateTableResponse* resp)
{
}

MessagePtr ProtocolBuilder<ListTableRequest, ListTableResponse>::serialize(
    const ListTableRequest& req)
{
    return MessagePtr(new protocol::ListTableRequest());
}

void ProtocolBuilder<ListTableRequest, ListTableResponse>::parse(
    const std::string& responseBody)
{
    protocol::ListTableResponse* pbResponse = new protocol::ListTableResponse();
    mPbResult.reset(pbResponse);
    if (!pbResponse->ParseFromString(responseBody)) {
        throw OTSClientException("Invalid response body.");
    }
}

void ProtocolBuilder<ListTableRequest, ListTableResponse>::deserialize(
    ListTableResponse* resp)
{
    protocol::ListTableResponse* pbResponse = 
        dynamic_cast<protocol::ListTableResponse*>(mPbResult.get());
    resp->reset();
    for (int i = 0; i < pbResponse->table_names_size(); ++i) {
        resp->mutableTables()->append() = pbResponse->table_names(i);
    }
}


MessagePtr ProtocolBuilder<DeleteTableRequest, DeleteTableResponse>::serialize(
    const DeleteTableRequest& req)
{
    protocol::DeleteTableRequest* pbRequest = new protocol::DeleteTableRequest();
    MessagePtr res(pbRequest);
    pbRequest->set_table_name(req.table());
    return res;
}

void ProtocolBuilder<DeleteTableRequest, DeleteTableResponse>::parse(
    const std::string& responseBody)
{
    protocol::DeleteTableResponse* pbResponse =
        new protocol::DeleteTableResponse();
    mPbResult.reset(pbResponse);
    if (!pbResponse->ParseFromString(responseBody)) {
        throw OTSClientException("Invalid response body.");
    }
}

void ProtocolBuilder<DeleteTableRequest, DeleteTableResponse>::deserialize(
    DeleteTableResponse* resp)
{
    resp->reset();
}

MessagePtr ProtocolBuilder<DescribeTableRequest, DescribeTableResponse>::serialize(
    const DescribeTableRequest& req)
{
    protocol::DescribeTableRequest* pbRequest =
        new protocol::DescribeTableRequest();
    MessagePtr res(pbRequest);
    pbRequest->set_table_name(req.table());
    return res;
}

void ProtocolBuilder<DescribeTableRequest, DescribeTableResponse>::parse(
    const std::string& responseBody)
{
    protocol::DescribeTableResponse* pbResponse =
        new protocol::DescribeTableResponse();
    mPbResult.reset(pbResponse);
    if (!pbResponse->ParseFromString(responseBody)) {
        throw OTSClientException("Invalid response body.");
    }
}

void ProtocolBuilder<DescribeTableRequest, DescribeTableResponse>::deserialize(
    DescribeTableResponse* resp)
{
    protocol::DescribeTableResponse* pbResponse =
        dynamic_cast<protocol::DescribeTableResponse*>(mPbResult.get());

    // 1. meta
    TableMeta tableMeta;
    ToTableMeta(pbResponse->table_meta(), &tableMeta);
    moveAssign(resp->mutableMeta(), util::move(tableMeta));
    
    // 2. Table Options
    TableOptions options;
    ToTableOptions(pbResponse->table_options(), options);
    moveAssign(resp->mutableOptions(), util::move(options));

    // 3. SetReservedThroughputDetails
    {
        CapacityUnit cu;
        ToCapacityUnit(
            pbResponse->reserved_throughput_details().capacity_unit(),
            &cu);
        *resp->mutableOptions()->mutableReservedThroughput() = util::move(cu);
    }

    // 4. TableStatus
    *resp->mutableStatus() = static_cast<TableStatus>(pbResponse->table_status());

    // 5. shard splits
    IVector<PrimaryKey>& splits = *resp->mutableShardSplitPoints();
    splits.reset();
    for(int64_t i = 0, sz = pbResponse->shard_splits_size(); i < sz; ++i) {
        const string& point = pbResponse->shard_splits(i);
        PlainBufferInputStream inputStream(point);
        PlainBufferCodedInputStream codedInputStream(&inputStream);
        Row row;
        codedInputStream.ReadRow(resp->mutableMemHolder(), &row);
        moveAssign(&splits.append(), util::move(*row.mutablePrimaryKey()));
    }
}

MessagePtr ProtocolBuilder<UpdateTableRequest, UpdateTableResponse>::serialize(
    const UpdateTableRequest& req)
{
    protocol::UpdateTableRequest* pbRequest =
        new protocol::UpdateTableRequest();
    MessagePtr res(pbRequest);

    pbRequest->set_table_name(req.table());
    if (req.options().reservedThroughput().present()) {
        ToCapacityUnit(
            *req.options().reservedThroughput(),
            pbRequest->mutable_reserved_throughput());
    }
    ToTableOptions(req.options(), pbRequest->mutable_table_options());

    return res;
}

void ProtocolBuilder<UpdateTableRequest, UpdateTableResponse>::parse(
    const std::string& responseBody)
{
    protocol::UpdateTableResponse* pbResponse =
        new protocol::UpdateTableResponse();
    mPbResult.reset(pbResponse);
    if (!pbResponse->ParseFromString(responseBody)) {
        throw OTSClientException("Invalid response body.");
    }
}

void ProtocolBuilder<UpdateTableRequest, UpdateTableResponse>::deserialize(
    UpdateTableResponse* resp)
{
    resp->reset();
}

MessagePtr ProtocolBuilder<ComputeSplitsBySizeRequest, ComputeSplitsBySizeResponse>::serialize(
    const ComputeSplitsBySizeRequest& req)
{
    protocol::ComputeSplitPointsBySizeRequest* pbRequest = new protocol::ComputeSplitPointsBySizeRequest();
    MessagePtr res(pbRequest);

    pbRequest->set_table_name(req.table());
    pbRequest->set_split_size(req.splitSize());

    return res;
}

void ProtocolBuilder<ComputeSplitsBySizeRequest, ComputeSplitsBySizeResponse>::parse(
    const std::string& responseBody)
{
    protocol::ComputeSplitPointsBySizeResponse* pbResponse = new protocol::ComputeSplitPointsBySizeResponse();
    mPbResult.reset(pbResponse);
    if (!pbResponse->ParseFromString(responseBody)) {
        throw OTSClientException("Invalid response body.");
    }
}

namespace {

void ComplementPrimaryKey(
    PrimaryKey* pkey,
    const Schema& schema,
    const PrimaryKeyValue& val)
{
    int64_t curSz = pkey->size();
    if (curSz < schema.size()) {
        for(int64_t i = curSz, sz = schema.size(); i < sz; ++i) {
            pkey->append() = PrimaryKeyColumn(schema[i].name(), val);
        }
    }
}

} // namespace

void ProtocolBuilder<ComputeSplitsBySizeRequest, ComputeSplitsBySizeResponse>::deserialize(
    ComputeSplitsBySizeResponse* resp)
{
    protocol::ComputeSplitPointsBySizeResponse* pbResponse =
        dynamic_cast<protocol::ComputeSplitPointsBySizeResponse*>(mPbResult.get());
    resp->reset();
    
    ToCapacityUnit(
        pbResponse->consumed().capacity_unit(),
        resp->mutableConsumedCapacity());

    // 1. schema
    for (int64_t i = 0; i < pbResponse->schema_size(); ++i) {
        PrimaryKeyColumnSchema& colSchema = resp->mutableSchema()->append();
        const protocol::PrimaryKeySchema& pbColSchema = pbResponse->schema(i);
        *colSchema.mutableName() = pbColSchema.name();
        *colSchema.mutableType() = ToPrimaryKeyType(pbColSchema.type());
        if (pbColSchema.has_option()) {
            *colSchema.mutableOption() = ToPrimaryKeyOption(pbColSchema.option());
        }
    }

    // 2. splits
    const Schema& schema = resp->schema();
    const PrimaryKeyValue& infMin = PrimaryKeyValue::toInfMin();
    const PrimaryKeyValue& infMax = PrimaryKeyValue::toInfMax();
    shared_ptr<PrimaryKey> lower(new PrimaryKey());
    ComplementPrimaryKey(lower.get(), schema, infMin);
    for(int64_t i = 0, sz = pbResponse->split_points_size(); i < sz; ++i) {
        Split& split = resp->mutableSplits()->append();
        *split.mutableLowerBound() = lower;
        {
            const string& point = pbResponse->split_points(i);
            PlainBufferInputStream inputStream(point);
            PlainBufferCodedInputStream codedInputStream(&inputStream);
            Row tmpRow;
            codedInputStream.ReadRow(resp->mutableMemHolder(), &tmpRow);
            PrimaryKey& upper = *tmpRow.mutablePrimaryKey();
            ComplementPrimaryKey(&upper, schema, infMin);
            split.mutableUpperBound()->reset(new PrimaryKey());
            moveAssign(split.mutableUpperBound()->get(), util::move(upper));
        }
        lower = split.upperBound();
    }
    Split& last = resp->mutableSplits()->append();
    (*last.mutableLowerBound()) = lower;
    last.mutableUpperBound()->reset(new PrimaryKey());
    ComplementPrimaryKey(last.mutableUpperBound()->get(), schema, infMax);

    // 2.1 locations
    int64_t splitIdx = 0;
    for(int64_t i = 0, sz = pbResponse->locations_size(); i < sz; ++i) {
        const protocol::ComputeSplitPointsBySizeResponse_SplitLocation& pbLoc =
            pbResponse->locations(i);
        const string& location = pbLoc.location();
        int64_t repeat = pbLoc.repeat();
        for(; splitIdx < resp->splits().size() && repeat > 0; ++splitIdx, --repeat) {
            *(*resp->mutableSplits())[splitIdx].mutableLocation() = location;
        }
        if (repeat > 0) {
            throw OTSClientException("Locations length is incorrect.");
        }
    }
    if (splitIdx != resp->splits().size()) {
        throw OTSClientException("Locations length is incorrect.");
    }
}

MessagePtr ProtocolBuilder<PutRowRequest, PutRowResponse>::serialize(
    const PutRowRequest& req)
{
    protocol::PutRowRequest* pbRequest = new protocol::PutRowRequest();
    MessagePtr res(pbRequest);

    const RowPutChange& rowChange = req.rowChange();
    pbRequest->set_table_name(rowChange.table());

    pbRequest->set_row(PlainBufferBuilder::SerializeForRow(rowChange));

    protocol::ReturnContent *returnContent = pbRequest->mutable_return_content();
    returnContent->set_return_type(ToReturnType(rowChange.returnType()));
    
    const Condition& condition = rowChange.condition();
    protocol::Condition *pbCondition = pbRequest->mutable_condition();
    pbCondition->set_row_existence(ToRowExistence(condition.rowCondition()));

    const shared_ptr<ColumnCondition>& columnCondition = condition.columnCondition();
    if (columnCondition.get() != NULL) {
        pbCondition->set_column_condition(ToFilter(columnCondition.get()));
    }

    return res;
}

void ProtocolBuilder<PutRowRequest, PutRowResponse>::parse(
    const std::string& responseBody)
{
    protocol::PutRowResponse* pbResponse = new protocol::PutRowResponse();
    mPbResult.reset(pbResponse);
    if (!pbResponse->ParseFromString(responseBody)) {
        throw OTSClientException("Invalid response body.");
    }
}

void ProtocolBuilder<PutRowRequest, PutRowResponse>::deserialize(
    PutRowResponse* resp)
{
    protocol::PutRowResponse* pbResponse =
        dynamic_cast<protocol::PutRowResponse*>(mPbResult.get());
    resp->reset();

    ToCapacityUnit(
        pbResponse->consumed().capacity_unit(),
        resp->mutableConsumedCapacity());

    if (pbResponse->has_row() && !pbResponse->row().empty()) {
        PlainBufferInputStream inputStream(pbResponse->row());
        PlainBufferCodedInputStream codedInputStream(&inputStream);
        Row row;
        codedInputStream.ReadRow(resp->mutableMemHolder(), &row);
        *resp->mutableRow() = util::move(row);
    }
}

MessagePtr ProtocolBuilder<GetRowRequest, GetRowResponse>::serialize(
    const GetRowRequest& req)
{
    protocol::GetRowRequest* pbRequest = new protocol::GetRowRequest();
    MessagePtr res(pbRequest);

    const PointQueryCriterion& queryCriterion = req.queryCriterion();
    pbRequest->set_table_name(queryCriterion.table());
    const PrimaryKey& primaryKey = queryCriterion.primaryKey();
    pbRequest->set_primary_key(
        PlainBufferBuilder::SerializePrimaryKey(primaryKey));

    const IVector<string>& columnsToGet = queryCriterion.columnsToGet();
    for(int64_t i = 0, sz = columnsToGet.size(); i < sz; ++i) {
        pbRequest->add_columns_to_get(columnsToGet[i]);
    }
    if (queryCriterion.timeRange().present()) {
        ToTimeRange(*queryCriterion.timeRange(), pbRequest->mutable_time_range());
    }
    if (queryCriterion.maxVersions().present()) {
        pbRequest->set_max_versions(*queryCriterion.maxVersions());
    }
    if (queryCriterion.cacheBlocks().present()) {
        pbRequest->set_cache_blocks(*queryCriterion.cacheBlocks());
    }
    if (queryCriterion.filter().get() != NULL) {
        pbRequest->set_filter(ToFilter(queryCriterion.filter().get()));
    }

    return res;
}

void ProtocolBuilder<GetRowRequest, GetRowResponse>::parse(
    const std::string& responseBody)
{
    protocol::GetRowResponse* pbResponse = new protocol::GetRowResponse();
    mPbResult.reset(pbResponse);
    if (!pbResponse->ParseFromString(responseBody)) {
        throw OTSClientException("Invalid response body.");
    }
}

void ProtocolBuilder<GetRowRequest, GetRowResponse>::deserialize(
    GetRowResponse* resp)
{
    protocol::GetRowResponse* pbResponse =
        dynamic_cast<protocol::GetRowResponse*>(mPbResult.get());
    resp->reset();
    
    ToCapacityUnit(
        pbResponse->consumed().capacity_unit(),
        resp->mutableConsumedCapacity());

    if (!pbResponse->row().empty()) {
        PlainBufferInputStream inputStream(pbResponse->row());
        PlainBufferCodedInputStream codedInputStream(&inputStream);
        Row row;
        codedInputStream.ReadRow(resp->mutableMemHolder(), &row);
        *resp->mutableRow() = util::move(row);
    }
}

MessagePtr ProtocolBuilder<GetRangeRequest, GetRangeResponse>::serialize(
    const GetRangeRequest& req)
{
    protocol::GetRangeRequest* pbRequest = new protocol::GetRangeRequest();
    MessagePtr res(pbRequest);
    
    const RangeQueryCriterion& queryCriterion = req.queryCriterion();
    pbRequest->set_table_name(queryCriterion.table());
    pbRequest->set_direction(ToDirection(queryCriterion.direction()));

    const IVector<string>& columnsToGet = queryCriterion.columnsToGet();
    for(int64_t i = 0, sz = columnsToGet.size(); i < sz; ++i) {
        pbRequest->add_columns_to_get(columnsToGet[i]);
    }

    if (queryCriterion.timeRange().present()) {
        ToTimeRange(*queryCriterion.timeRange(), pbRequest->mutable_time_range());
    }
    if (queryCriterion.maxVersions().present()) {
        pbRequest->set_max_versions(*queryCriterion.maxVersions());
    }
    if (queryCriterion.limit().present()) {
        pbRequest->set_limit(*queryCriterion.limit());
    }
    if (queryCriterion.cacheBlocks().present()) {
        pbRequest->set_cache_blocks(*queryCriterion.cacheBlocks());
    }
    if (queryCriterion.filter().get() != NULL) {
        pbRequest->set_filter(ToFilter(queryCriterion.filter().get()));
    }

    pbRequest->set_inclusive_start_primary_key(
        PlainBufferBuilder::SerializePrimaryKey(queryCriterion.inclusiveStart()));
    pbRequest->set_exclusive_end_primary_key(
        PlainBufferBuilder::SerializePrimaryKey(queryCriterion.exclusiveEnd()));

    return res;
}

void ProtocolBuilder<GetRangeRequest, GetRangeResponse>::parse(
    const std::string& responseBody)
{
    protocol::GetRangeResponse* pbResponse = new protocol::GetRangeResponse();
    mPbResult.reset(pbResponse);
    if (!pbResponse->ParseFromString(responseBody)) {
        throw OTSClientException("Invalid response body.");
    }
}

void ProtocolBuilder<GetRangeRequest, GetRangeResponse>::deserialize(
    GetRangeResponse* resp)
{
    protocol::GetRangeResponse* pbResponse =
        dynamic_cast<protocol::GetRangeResponse*>(mPbResult.get());
    resp->reset();
    
    ToCapacityUnit(
        pbResponse->consumed().capacity_unit(),
        resp->mutableConsumedCapacity());

    if (!pbResponse->rows().empty()) {
        PlainBufferInputStream inputStream(pbResponse->rows());
        PlainBufferCodedInputStream codedInputStream(&inputStream);
        codedInputStream.ReadRows(resp->mutableMemHolder(), resp->mutableRows());
    }

    if (pbResponse->has_next_start_primary_key()) {
        PlainBufferInputStream inputStream2(pbResponse->next_start_primary_key());
        PlainBufferCodedInputStream codedInputStream2(&inputStream2);
        Row nextStart;
        codedInputStream2.ReadRow(resp->mutableMemHolder(), &nextStart);
        *resp->mutableNextStart() = util::move(*nextStart.mutablePrimaryKey());
    }
}

MessagePtr ProtocolBuilder<UpdateRowRequest, UpdateRowResponse>::serialize(
    const UpdateRowRequest& req)
{
    protocol::UpdateRowRequest* pbRequest = new protocol::UpdateRowRequest();
    MessagePtr res(pbRequest);

    const RowUpdateChange& rowChange = req.rowChange();
    pbRequest->set_table_name(rowChange.table());
    pbRequest->set_row_change(PlainBufferBuilder::SerializeForRow(rowChange));

    protocol::ReturnContent *returnContent = pbRequest->mutable_return_content();
    returnContent->set_return_type(ToReturnType(rowChange.returnType()));

    const Condition& condition = rowChange.condition();
    protocol::Condition *pbCondition = pbRequest->mutable_condition();
    pbCondition->set_row_existence(ToRowExistence(condition.rowCondition()));

    const shared_ptr<ColumnCondition>& columnCondition = condition.columnCondition();
    if (columnCondition != NULL) {
        pbCondition->set_column_condition(ToFilter(columnCondition.get()));
    }

    return res;
}

void ProtocolBuilder<UpdateRowRequest, UpdateRowResponse>::parse(
    const std::string& responseBody)
{
    protocol::UpdateRowResponse* pbResponse = new protocol::UpdateRowResponse();
    mPbResult.reset(pbResponse);
    if (!pbResponse->ParseFromString(responseBody)) {
        throw OTSClientException("Invalid response body.");
    }
}

void ProtocolBuilder<UpdateRowRequest, UpdateRowResponse>::deserialize(
    UpdateRowResponse* resp)
{
    protocol::UpdateRowResponse* pbResponse =
        dynamic_cast<protocol::UpdateRowResponse*>(mPbResult.get());
    resp->reset();

    ToCapacityUnit(
        pbResponse->consumed().capacity_unit(),
        resp->mutableConsumedCapacity());

    if (pbResponse->has_row() && !pbResponse->row().empty()) {
        PlainBufferInputStream inputStream(pbResponse->row());
        PlainBufferCodedInputStream codedInputStream(&inputStream);
        Row row;
        codedInputStream.ReadRow(resp->mutableMemHolder(), &row);
        *resp->mutableRow() = util::move(row);
    }
}

MessagePtr ProtocolBuilder<DeleteRowRequest, DeleteRowResponse>::serialize(
    const DeleteRowRequest& req)
{
    protocol::DeleteRowRequest* pbRequest = new protocol::DeleteRowRequest();
    MessagePtr res(pbRequest);

    const RowDeleteChange& rowChange = req.rowChange();
    pbRequest->set_table_name(rowChange.table());
    pbRequest->set_primary_key(PlainBufferBuilder::SerializeForRow(rowChange));

    protocol::ReturnContent *returnContent = pbRequest->mutable_return_content();
    returnContent->set_return_type(ToReturnType(rowChange.returnType()));

    const Condition& condition = rowChange.condition();
    protocol::Condition *pbCondition = pbRequest->mutable_condition();
    pbCondition->set_row_existence(ToRowExistence(condition.rowCondition()));

    const shared_ptr<ColumnCondition>& columnCondition = condition.columnCondition();
    if (columnCondition != NULL) {
        pbCondition->set_column_condition(ToFilter(columnCondition.get()));
    }

    return res;
}

void ProtocolBuilder<DeleteRowRequest, DeleteRowResponse>::parse(
    const std::string& responseBody)
{
    protocol::DeleteRowResponse* pbResponse = new protocol::DeleteRowResponse();
    mPbResult.reset(pbResponse);
    if (!pbResponse->ParseFromString(responseBody)) {
        throw OTSClientException("Invalid response body.");
    }
}

void ProtocolBuilder<DeleteRowRequest, DeleteRowResponse>::deserialize(
    DeleteRowResponse* resp)
{
    protocol::DeleteRowResponse* pbResponse =
        dynamic_cast<protocol::DeleteRowResponse*>(mPbResult.get());
    resp->reset();

    ToCapacityUnit(pbResponse->consumed().capacity_unit(), resp->mutableConsumedCapacity());

    if (pbResponse->has_row() && !pbResponse->row().empty()) {
        PlainBufferInputStream inputStream(pbResponse->row());
        PlainBufferCodedInputStream codedInputStream(&inputStream);
        Row row;
        codedInputStream.ReadRow(resp->mutableMemHolder(), &row);
        *resp->mutableRow() = util::move(row);
    }
}


MessagePtr ProtocolBuilder<BatchGetRowRequest, BatchGetRowResponse>::serialize(
    const BatchGetRowRequest& req)
{
    protocol::BatchGetRowRequest* pbRequest = new protocol::BatchGetRowRequest();
    MessagePtr res(pbRequest);

    for(int64_t i = 0, sz = req.criteria().size(); i < sz; ++i) {
        const MultiPointQueryCriterion& criterion = req.criteria()[i];
        protocol::TableInBatchGetRowRequest* table = pbRequest->add_tables();
        table->set_table_name(criterion.table());
        for(int64_t j = 0, psz = criterion.rowKeys().size(); j < psz; ++j) {
            table->add_primary_key(
                PlainBufferBuilder::SerializePrimaryKey(criterion.rowKeys()[j].get()));
        }
        if (criterion.columnsToGet().size() > 0) {
            for(int64_t j = 0, sz = criterion.columnsToGet().size(); j < sz; ++j) {
                table->add_columns_to_get(criterion.columnsToGet()[j]);
            }
        }
        if (criterion.timeRange().present()) {
            ToTimeRange(*criterion.timeRange(), table->mutable_time_range());
        }
        if (criterion.maxVersions().present()) {
            table->set_max_versions(*criterion.maxVersions());
        }
        if (criterion.cacheBlocks().present()) {
            table->set_cache_blocks(*criterion.cacheBlocks());
        }
    }

    mRequest = &req;
    return res;
}

void ProtocolBuilder<BatchGetRowRequest, BatchGetRowResponse>::parse(
    const std::string& responseBody)
{
    protocol::BatchGetRowResponse* pbResponse = new protocol::BatchGetRowResponse();
    mPbResult.reset(pbResponse);
    if (!pbResponse->ParseFromString(responseBody)) {
        throw OTSClientException("Invalid response body.");
    }
}

namespace {

void mergeConsumedCapacity(CapacityUnit* result, const CapacityUnit& in)
{
    if (in.read().present()) {
        if (result->read().present()) {
            **result->mutableRead() += *in.read();
        } else {
            *result->mutableRead() = in.read();
        }
    }
    if (in.write().present()) {
        if (result->write().present()) {
            **result->mutableWrite() += *in.write();
        } else {
            *result->mutableWrite() = in.write();
        }
    }
}

} // namespace

void ProtocolBuilder<BatchGetRowRequest, BatchGetRowResponse>::deserialize(
    BatchGetRowResponse* resp)
{
    protocol::BatchGetRowResponse* pbResponse =
        dynamic_cast<protocol::BatchGetRowResponse*>(mPbResult.get());
    resp->reset();
    
    for (int32_t i = 0; i < pbResponse->tables_size(); ++i) {
        const protocol::TableInBatchGetRowResponse& pbTable =
            pbResponse->tables(i);
        for (int32_t j = 0; j < pbTable.rows_size(); ++j) {
            const protocol::RowInBatchGetRowResponse& pbRow = pbTable.rows(j);
            Result<Optional<Row>, Error>& row =
                *resp->mutableResults()->append().mutableGet();
            if (pbRow.is_ok()) {
                CapacityUnit consumedCapacity;
                ToCapacityUnit(
                    pbRow.consumed().capacity_unit(), &consumedCapacity);
                mergeConsumedCapacity(
                    resp->mutableConsumedCapacity(), consumedCapacity);

                if (!pbRow.row().empty()) {
                    PlainBufferInputStream inputStream(pbRow.row());
                    PlainBufferCodedInputStream codedInputStream(&inputStream);
                    Row r;
                    codedInputStream.ReadRow(resp->mutableMemHolder(), &r);
                    *row.mutableOkValue() = util::move(r);
                }
            } else {
                Error error;
                ToError(pbRow.error(), &error);
                *row.mutableErrValue() = util::move(error);
            }
        }
    }

    int64_t i = 0;
    for(int64_t criterionIdx = 0, criterionSz = mRequest->criteria().size();
        criterionIdx < criterionSz;
        ++criterionIdx)
    {
        const MultiPointQueryCriterion& criterion =
            mRequest->criteria()[criterionIdx];
        for(int64_t j = 0, sz = criterion.rowKeys().size(); j < sz; ++j, ++i) {
            *(*resp->mutableResults())[i].mutableUserData() =
                criterion.rowKeys()[j].userData();
        }
    }
}

template<class Op>
void BuildRowChange(
    protocol::BatchWriteRowRequest* pbReq,
    protocol::OperationType pbOp,
    const IVector<Op>& rows,
    const ProtocolBuilder<BatchWriteRowRequest, BatchWriteRowResponse>::Indice& indice)
{
    FOREACH_ITER(it, indice) {
        const string& table = it->first;
        const deque<int64_t>& idxes = it->second;
        FOREACH_ITER(j, idxes) {
            const Op& row = rows[*j];
            
        }
    }
}

// } // namespace

MessagePtr ProtocolBuilder<BatchWriteRowRequest, BatchWriteRowResponse>::serialize(
    const BatchWriteRowRequest& req)
{
    protocol::BatchWriteRowRequest* pbRequest = new protocol::BatchWriteRowRequest();
    MessagePtr res(pbRequest);

    {
        const IVector<BatchWriteRowRequest::Put>& rows = req.puts();
        for(int64_t i = 0, sz = rows.size(); i < sz; ++i) {
            mIndice[rows[i].get().table()].mPuts.push_back(i);
        }
    }
    {
        const IVector<BatchWriteRowRequest::Update>& rows = req.updates();
        for(int64_t i = 0, sz = rows.size(); i < sz; ++i) {
            mIndice[rows[i].get().table()].mUpdates.push_back(i);
        }
    }
    {
        const IVector<BatchWriteRowRequest::Delete>& rows = req.deletes();
        for(int64_t i = 0, sz = rows.size(); i < sz; ++i) {
            mIndice[rows[i].get().table()].mDeletes.push_back(i);
        }
    }

    FOREACH_ITER(i, mIndice) {
        const string& tableName = i->first;
        protocol::TableInBatchWriteRowRequest* pbTable = pbRequest->add_tables();
        pbTable->set_table_name(tableName);

        FOREACH_ITER(j, i->second.mPuts) {
            const RowPutChange& row = req.puts()[*j].get();
            protocol::RowInBatchWriteRowRequest *rowInRequest = pbTable->add_rows();
            rowInRequest->set_type(protocol::PUT);
            rowInRequest->set_row_change(PlainBufferBuilder::SerializeForRow(row));

            protocol::ReturnContent *returnContent = rowInRequest->mutable_return_content();
            returnContent->set_return_type(ToReturnType(row.returnType()));
        
            const Condition& condition = row.condition();
            protocol::Condition* pbCondition = rowInRequest->mutable_condition();
            pbCondition->set_row_existence(ToRowExistence(condition.rowCondition()));

            const shared_ptr<ColumnCondition>& columnCondition = condition.columnCondition();
            if (columnCondition.get() != NULL) {
                pbCondition->set_column_condition(ToFilter(columnCondition.get()));
            }
        }
        FOREACH_ITER(j, i->second.mUpdates) {
            const RowUpdateChange& row = req.updates()[*j].get();
            protocol::RowInBatchWriteRowRequest *rowInRequest = pbTable->add_rows();
            rowInRequest->set_type(protocol::UPDATE);
            rowInRequest->set_row_change(PlainBufferBuilder::SerializeForRow(row));

            protocol::ReturnContent *returnContent = rowInRequest->mutable_return_content();
            returnContent->set_return_type(ToReturnType(row.returnType()));
        
            const Condition& condition = row.condition();
            protocol::Condition* pbCondition = rowInRequest->mutable_condition();
            pbCondition->set_row_existence(ToRowExistence(condition.rowCondition()));

            const shared_ptr<ColumnCondition>& columnCondition = condition.columnCondition();
            if (columnCondition.get() != NULL) {
                pbCondition->set_column_condition(ToFilter(columnCondition.get()));
            }
        }
        FOREACH_ITER(j, i->second.mDeletes) {
            const RowDeleteChange& row = req.deletes()[*j].get();
            protocol::RowInBatchWriteRowRequest *rowInRequest = pbTable->add_rows();
            rowInRequest->set_type(protocol::DELETE);
            rowInRequest->set_row_change(PlainBufferBuilder::SerializeForRow(row));

            protocol::ReturnContent *returnContent = rowInRequest->mutable_return_content();
            returnContent->set_return_type(ToReturnType(row.returnType()));
        
            const Condition& condition = row.condition();
            protocol::Condition* pbCondition = rowInRequest->mutable_condition();
            pbCondition->set_row_existence(ToRowExistence(condition.rowCondition()));

            const shared_ptr<ColumnCondition>& columnCondition = condition.columnCondition();
            if (columnCondition.get() != NULL) {
                pbCondition->set_column_condition(ToFilter(columnCondition.get()));
            }
        }
    }
    
    mRequest = &req;
    return res;
}

void ProtocolBuilder<BatchWriteRowRequest, BatchWriteRowResponse>::parse(
    const std::string& responseBody)
{
    protocol::BatchWriteRowResponse* pbResponse =
        new protocol::BatchWriteRowResponse();
    mPbResult.reset(pbResponse);
    if (!pbResponse->ParseFromString(responseBody)) {
        throw OTSClientException("Invalid response body.");
    }
}

namespace {

void to(
    IVector<string>* memHolder,
    CapacityUnit* consumedCapacity,
    BatchWriteRowResponse::Result* result,
    const protocol::RowInBatchWriteRowResponse& pbResponseRow)
{
    if (pbResponseRow.is_ok()) {
        CapacityUnit cu;
        ToCapacityUnit(pbResponseRow.consumed().capacity_unit(), &cu);
        mergeConsumedCapacity(consumedCapacity, cu);

        if (pbResponseRow.has_row() && !pbResponseRow.row().empty()) {
            PlainBufferInputStream inputStream(pbResponseRow.row());
            PlainBufferCodedInputStream codedInputStream(&inputStream);
            Row row;
            codedInputStream.ReadRow(memHolder, &row);
            *result->mutableGet()->mutableOkValue() = util::move(row);
        }
    } else {
        Error error;
        ToError(pbResponseRow.error(), &error);
        *result->mutableGet()->mutableErrValue() = util::move(error);
    }
}

} // namespace

void ProtocolBuilder<BatchWriteRowRequest, BatchWriteRowResponse>::deserialize(
    BatchWriteRowResponse* resp)
{
    protocol::BatchWriteRowResponse* pbResponse =
        dynamic_cast<protocol::BatchWriteRowResponse*>(mPbResult.get());
    resp->reset();

    if (static_cast<int64_t>(mIndice.size()) != pbResponse->tables_size()) {
        throw OTSClientException("Invalid response");
    }
    for(int64_t i = 0, sz = mRequest->puts().size(); i < sz; ++i) {
        BatchWriteRowResponse::Result& result = resp->mutablePutResults()->append();
        *result.mutableUserData() = mRequest->puts()[i].userData();
    }
    for(int64_t i = 0, sz = mRequest->updates().size(); i < sz; ++i) {
        BatchWriteRowResponse::Result& result = resp->mutableUpdateResults()->append();
        *result.mutableUserData() = mRequest->updates()[i].userData();
    }
    for(int64_t i = 0, sz = mRequest->deletes().size(); i < sz; ++i) {
        BatchWriteRowResponse::Result& result = resp->mutableDeleteResults()->append();
        *result.mutableUserData() = mRequest->deletes()[i].userData();
    }
    for(int64_t i = 0, sz = pbResponse->tables_size(); i < sz; ++i) {
        const protocol::TableInBatchWriteRowResponse& pbResponseTable = pbResponse->tables(i);
        const string& tableName = pbResponseTable.table_name();
        Indice::const_iterator it = mIndice.find(tableName);
        if (it == mIndice.end()) {
            throw OTSClientException("Invalid response");
        }
        const Index& idxes = it->second;
        if (static_cast<int64_t>(idxes.mPuts.size() + idxes.mUpdates.size() + idxes.mDeletes.size())
            != pbResponseTable.rows_size()) {
            throw OTSClientException("Invalid response");
        }
        int64_t pbIdx = 0;
        FOREACH_ITER(it, idxes.mPuts) {
            const protocol::RowInBatchWriteRowResponse& pbResponseRow =
                pbResponseTable.rows(pbIdx);
            to(resp->mutableMemHolder(),
                resp->mutableConsumedCapacity(),
                &(*resp->mutablePutResults())[*it],
                pbResponseRow);
            ++pbIdx;
        }
        FOREACH_ITER(it, idxes.mUpdates) {
            const protocol::RowInBatchWriteRowResponse& pbResponseRow =
                pbResponseTable.rows(pbIdx);
            to(resp->mutableMemHolder(),
                resp->mutableConsumedCapacity(),
                &(*resp->mutableUpdateResults())[*it],
                pbResponseRow);
            ++pbIdx;
        }
        FOREACH_ITER(it, idxes.mDeletes) {
            const protocol::RowInBatchWriteRowResponse& pbResponseRow =
                pbResponseTable.rows(pbIdx);
            to(resp->mutableMemHolder(),
                resp->mutableConsumedCapacity(),
                &(*resp->mutableDeleteResults())[*it],
                pbResponseRow);
            ++pbIdx;
        }
    }
}

} // namespace core
} // namespace tablestore
} // namespace aliyun
