/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#include "ots_protocol_builder.h"
#include "../plainbuffer/plain_buffer_builder.h"
#include "ots_constants.h"
#include "ots/ots_exception.h"
#include "ots_helper.h"

using namespace std;
using namespace std::tr1;
using namespace google::protobuf;
using namespace com::aliyun::tablestore;

namespace aliyun {
namespace tablestore {

void OTSProtocolBuilder::ToError(
    const protocol::Error& pbError,
    Error* error)
{
    error->SetCode(pbError.code());
    if (pbError.has_message()) {
        error->SetMessage(pbError.message());
    }
}

protocol::PrimaryKeyType OTSProtocolBuilder::ToPrimaryKeyType(PrimaryKeyType type)
{
    switch (type) {
        case PKT_INTEGER:
            return protocol::INTEGER;
        case PKT_STRING:
            return protocol::STRING;
        case PKT_BINARY:
            return protocol::BINARY;
        default:
            throw OTSClientException(
                "Unsupported primary key type: " + OTSHelper::Int64ToString(type));
    }
}

protocol::PrimaryKeyOption OTSProtocolBuilder::ToPrimaryKeyOption(PrimaryKeyOption option)
{
    switch (option) {
    case PKO_AUTO_INCREMENT:
        return protocol::AUTO_INCREMENT;
    default:
        throw OTSClientException(
                "Unsupported primary key option: " + OTSHelper::Int64ToString(option));
    }
}

PrimaryKeyType OTSProtocolBuilder::ToPrimaryKeyType(protocol::PrimaryKeyType type)
{
    switch (type) {
        case protocol::INTEGER:
            return PKT_INTEGER;
        case protocol::STRING:
            return PKT_STRING;
        case protocol::BINARY:
            return PKT_BINARY;
        default:
            throw OTSClientException(
                "Unsupported primary key type: " + OTSHelper::Int64ToString(type));
    }
}

PrimaryKeyOption OTSProtocolBuilder::ToPrimaryKeyOption(protocol::PrimaryKeyOption option)
{
    switch (option) {
    case protocol::AUTO_INCREMENT:
        return PKO_AUTO_INCREMENT;
    default:
        throw OTSClientException(
                "Unsupported primary key option: " + OTSHelper::Int64ToString(option));
    }
}

void OTSProtocolBuilder::ToTableMeta(
    const TableMeta& tableMeta,
    protocol::TableMeta* pbTableMeta)
{
    pbTableMeta->set_table_name(tableMeta.GetTableName());

    // Primary key
    const std::list<PrimaryKeySchema>& schemas = tableMeta.GetPrimaryKeySchemas();
    if (schemas.empty()) {
        throw OTSClientException("Primary key schema is not set.");
    }
    for (typeof(schemas.begin()) iter = schemas.begin(); iter != schemas.end(); ++iter) {
        protocol::PrimaryKeySchema* primaryKeySchema = pbTableMeta->add_primary_key();
        primaryKeySchema->set_name(iter->GetName());
        primaryKeySchema->set_type(ToPrimaryKeyType(iter->GetType()));
        if (iter->HasOption()) {
            primaryKeySchema->set_option(ToPrimaryKeyOption(iter->GetOption()));
        }
    }
}

void OTSProtocolBuilder::ToTableMeta(
    const protocol::TableMeta& pbTableMeta,
    TableMeta* tableMeta) 
{
    tableMeta->SetTableName(pbTableMeta.table_name());

    for (int i=0; i<pbTableMeta.primary_key_size(); ++i) {
        const protocol::PrimaryKeySchema& primaryKeySchema = pbTableMeta.primary_key(i);
        if (primaryKeySchema.has_option()) {
            tableMeta->AddPrimaryKeySchema(primaryKeySchema.name(), 
                    ToPrimaryKeyType(primaryKeySchema.type()),
                    ToPrimaryKeyOption(primaryKeySchema.option()));
        } else {
            tableMeta->AddPrimaryKeySchema(primaryKeySchema.name(), 
                    ToPrimaryKeyType(primaryKeySchema.type()));
        }
    }
}

void OTSProtocolBuilder::ToReservedThroughputDetails(
    const protocol::ReservedThroughputDetails &pbDetails,
    ReservedThroughputDetails* details)
{
    CapacityUnit capacityUnit;
    if (pbDetails.capacity_unit().has_read()) {
        capacityUnit.SetReadCapacityUnit(pbDetails.capacity_unit().read());
    }
    if (pbDetails.capacity_unit().has_write()) {
        capacityUnit.SetWriteCapacityUnit(pbDetails.capacity_unit().write());
    }
    details->SetCapacityUnit(capacityUnit);

    details->SetLastIncreaseTime(pbDetails.last_increase_time());
    if (pbDetails.has_last_decrease_time()) {
        details->SetLastDecreaseTime(pbDetails.last_decrease_time());
    }
}

void OTSProtocolBuilder::ToReservedThroughput(
    const ReservedThroughput& throughput,
    protocol::ReservedThroughput* pbThroughput) 
{
    protocol::CapacityUnit* pbCapacityUnit = pbThroughput->mutable_capacity_unit();
    if (throughput.GetCapacityUnit().HasReadCapacityUnit()) {
        pbCapacityUnit->set_read(throughput.GetCapacityUnit().GetReadCapacityUnit());
    }
    if (throughput.GetCapacityUnit().HasWriteCapacityUnit()) {
        pbCapacityUnit->set_write(throughput.GetCapacityUnit().GetWriteCapacityUnit());
    }
}

void OTSProtocolBuilder::ToReservedThroughput(
    const protocol::ReservedThroughput& pbThroughput,
    ReservedThroughput* throughput)
{
    CapacityUnit capacityUnit;
    if (pbThroughput.capacity_unit().has_read()) {
        capacityUnit.SetReadCapacityUnit(pbThroughput.capacity_unit().read());
    }
    if (pbThroughput.capacity_unit().has_write()) {
        capacityUnit.SetWriteCapacityUnit(pbThroughput.capacity_unit().write());
    }
    throughput->SetCapacityUnit(capacityUnit);
}

void OTSProtocolBuilder::ToTableOptions(
    const TableOptions& options,
    protocol::TableOptions* pbOptions)
{
    if (options.HasTimeToLive()) {
        pbOptions->set_time_to_live(options.GetTimeToLive());
    }
    if (options.HasMaxVersions()) {
        pbOptions->set_max_versions(options.GetMaxVersions());
    }
    if (options.HasBloomFilterType()) {
        pbOptions->set_bloom_filter_type((protocol::BloomFilterType)(options.GetBloomFilterType()));
    }
    if (options.HasBlockSize()) {
        pbOptions->set_block_size(options.GetBlockSize());
    }
    if (options.HasDeviationCellVersionInSec()) {
        pbOptions->set_deviation_cell_version_in_sec(options.GetDeviationCellVersionInSec());
    }
}

void OTSProtocolBuilder::ToTableOptions(
    const protocol::TableOptions& pbOptions,
    TableOptions& options) 
{
    if (pbOptions.has_time_to_live()) {
        options.SetTimeToLive(pbOptions.time_to_live());
    }

    if (pbOptions.has_max_versions()) {
        options.SetMaxVersions(pbOptions.max_versions());
    }

    if (pbOptions.has_bloom_filter_type()) {
        options.SetBloomFilterType((BloomFilterType)pbOptions.bloom_filter_type());
    }

    if (pbOptions.has_block_size()) {
        options.SetBlockSize(pbOptions.block_size());
    }

    if (pbOptions.has_deviation_cell_version_in_sec()) {
        options.SetDeviationCellVersionInSec(pbOptions.deviation_cell_version_in_sec());
    }
}

void OTSProtocolBuilder::ToTimeRange(
    const TimeRange& timeRange,
    protocol::TimeRange* pbTimeRange)
{
    int64_t minStamp = timeRange.GetMinStamp();
    int64_t maxStamp = timeRange.GetMaxStamp();
    if (minStamp + 1 == maxStamp) {
        pbTimeRange->set_specific_time(minStamp);
    } else {
        pbTimeRange->set_start_time(minStamp);
        pbTimeRange->set_end_time(maxStamp);
    }
}

void OTSProtocolBuilder::ToConsumedCapacity(
    const protocol::ConsumedCapacity& pbConsumedCapacity,
    ConsumedCapacity* consumedCapacity)
{
    CapacityUnit capacityUnit; 
    const protocol::CapacityUnit& pbCapacityUnit = pbConsumedCapacity.capacity_unit();
    if (pbCapacityUnit.has_read()) {
        capacityUnit.SetReadCapacityUnit(pbCapacityUnit.read());
    }
    if (pbCapacityUnit.has_write()) {
        capacityUnit.SetWriteCapacityUnit(pbCapacityUnit.write());
    }
    consumedCapacity->SetCapacityUnit(capacityUnit);
}

protocol::Direction OTSProtocolBuilder::ToDirection(
    RangeRowQueryCriteria::Direction direction)
{
    switch (direction) {
        case RangeRowQueryCriteria::FORWARD:
            return protocol::FORWARD;
        case RangeRowQueryCriteria::BACKWARD:
            return protocol::BACKWARD;
        default:
            throw OTSClientException(
                "Unsupported direction: " + OTSHelper::Int64ToString(direction));
    }
}

// about filter
protocol::ComparatorType OTSProtocolBuilder::ToComparatorType(
        SingleColumnCondition::CompareOperator compareOperator)
{
    switch (compareOperator) {
        case SingleColumnCondition::EQUAL:
            return protocol::CT_EQUAL;
        case SingleColumnCondition::NOT_EQUAL:
            return protocol::CT_NOT_EQUAL;
        case SingleColumnCondition::GREATER_THAN:
            return protocol::CT_GREATER_THAN;
        case SingleColumnCondition::GREATER_EQUAL:
            return protocol::CT_GREATER_EQUAL;
        case SingleColumnCondition::LESS_THAN:
            return protocol::CT_LESS_THAN;
        case SingleColumnCondition::LESS_EQUAL:
            return protocol::CT_LESS_EQUAL;
        default:
            throw OTSClientException(
                "Unsupported compare operator: " + OTSHelper::Int64ToString(compareOperator));
    }
}

protocol::LogicalOperator OTSProtocolBuilder::ToLogicalOperator(
        CompositeColumnCondition::LogicOperator logicOperator )
{
    switch (logicOperator) {
        case CompositeColumnCondition::NOT:
            return protocol::LO_NOT;
        case CompositeColumnCondition::AND:
            return protocol::LO_AND;
        case CompositeColumnCondition::OR:
            return protocol::LO_OR;
        default:
            throw OTSClientException(
                "Unsupported logic operator: " + OTSHelper::Int64ToString(logicOperator));
    }
}

protocol::FilterType OTSProtocolBuilder::ToFilterType(ColumnConditionType columnConditionType)
{
    switch (columnConditionType) {
        case COMPOSITE_COLUMN_CONDITION:
            return protocol::FT_COMPOSITE_COLUMN_VALUE;
        case SINGLE_COLUMN_CONDITION:
            return protocol::FT_SINGLE_COLUMN_VALUE;
        default:
            throw OTSClientException(
                "Unsupported filter type: " + OTSHelper::Int64ToString(columnConditionType));
    }
}

protocol::RowExistenceExpectation OTSProtocolBuilder::ToRowExistence(
        RowExistenceExpectation expectation)
{
    switch (expectation) {
    case IGNORE:
        return protocol::IGNORE;
    case EXPECT_EXIST:
        return protocol::EXPECT_EXIST;
    case EXPECT_NOT_EXIST:
        return protocol::EXPECT_NOT_EXIST;
    default:
        throw OTSClientException(
                "Unsupported RowExistenceExpectation type: " + 
                OTSHelper::Int64ToString(expectation));
    }
}

protocol::ReturnType OTSProtocolBuilder::ToReturnType(ReturnType returnType)
{
    switch (returnType) {
    case RT_NONE:
        return protocol::RT_NONE;
    case RT_PK:
        return protocol::RT_PK;
    default:
        throw OTSClientException(
                "Unsupported ReturnContent: " +
                OTSHelper::Int64ToString(returnType));
    }
}

string OTSProtocolBuilder::ToSingleCondition(const SingleColumnCondition* singleCondition)
{
    protocol::SingleColumnValueFilter pbRelationFilter;
    pbRelationFilter.set_column_name(singleCondition->GetColumnName());
    pbRelationFilter.set_comparator(ToComparatorType(singleCondition->GetCompareOperator()));
    string valueBuffer = PlainBufferBuilder::SerializeColumnValue(singleCondition->GetColumnValue());
    pbRelationFilter.set_column_value(valueBuffer);
    pbRelationFilter.set_filter_if_missing(singleCondition->GetPassIfMissing());
    pbRelationFilter.set_latest_version_only(singleCondition->GetLatestVersionsOnly());
    string relationFilterStr;
    if (!pbRelationFilter.SerializeToString(&relationFilterStr)) {
        throw OTSClientException("Invalid filter.");
    }
    return relationFilterStr;
}

string OTSProtocolBuilder::ToCompositeCondition(const CompositeColumnCondition* compositeCondition)
{
    protocol::CompositeColumnValueFilter pbCompositeFilter;
    pbCompositeFilter.set_combinator(ToLogicalOperator(compositeCondition->GetLogicOperator()));
    const list<ColumnConditionPtr>& subColumnConditions = compositeCondition->GetSubConditions();
    typeof(subColumnConditions.begin()) iter = subColumnConditions.begin();
    for (; iter != subColumnConditions.end(); ++iter) {
        protocol::Filter* pbSubFilter = pbCompositeFilter.add_sub_filters();
        pbSubFilter->set_type(ToFilterType((*iter)->GetColumnConditionType()));
        pbSubFilter->set_filter((*iter)->Serialize());
    }
    string compositeFilterStr;
    if (!pbCompositeFilter.SerializeToString(&compositeFilterStr)) {
        throw OTSClientException("Invalid filter.");
    }
    return compositeFilterStr;
}

string OTSProtocolBuilder::ToFilter(const ColumnCondition* columnCondition)
{
    protocol::Filter pbFilter;
    pbFilter.set_type(ToFilterType(columnCondition->GetColumnConditionType()));
    pbFilter.set_filter(columnCondition->Serialize());
    string filterStr;
    if (!pbFilter.SerializeToString(&filterStr)) {
        throw OTSClientException("Invalid filter.");
    }
    return filterStr;
}

void OTSProtocolBuilder::BuildProtobufRequest(
    const CreateTableRequestPtr& requestPtr,
    MessagePtr* pbRequestPtr)
{
    pbRequestPtr->reset(new protocol::CreateTableRequest());
    protocol::CreateTableRequest* pbRequest = 
        dynamic_cast<protocol::CreateTableRequest*>(pbRequestPtr->get());

    const TableMeta& tableMeta = requestPtr->GetTableMeta();
    ToTableMeta(tableMeta, pbRequest->mutable_table_meta());
    ToReservedThroughput(requestPtr->GetReservedThroughput(), pbRequest->mutable_reserved_throughput());
    ToTableOptions(requestPtr->GetTableOptions(), pbRequest->mutable_table_options());
    if (requestPtr->HasPartitionRanges()) {
        std::list<PartitionRange> partitions = requestPtr->GetPartitionRanges();
        for (typeof(partitions.begin()) iter = partitions.begin(); iter != partitions.end(); ++iter) {
            protocol::PartitionRange *p = pbRequest->add_partitions();
            p->set_begin(PlainBufferBuilder::SerializePrimaryKeyValue(iter->GetBegin()));
            p->set_end(PlainBufferBuilder::SerializePrimaryKeyValue(iter->GetEnd()));
        }
    }
}

void OTSProtocolBuilder::ParseProtobufResponse(
    const std::string& responseBody,
    CreateTableResultPtr* resultPtr,
    MessagePtr* pbResponsePtr)
{
    pbResponsePtr->reset(new protocol::CreateTableResponse);
    protocol::CreateTableResponse* pbResponse = 
        dynamic_cast<protocol::CreateTableResponse*>(pbResponsePtr->get());
    if (!pbResponse->ParseFromString(responseBody)) {
        throw OTSClientException("Invalid response body.");
    }
}

void OTSProtocolBuilder::ParseProtobufResult(
     const MessagePtr& pbResponsePtr,
     CreateTableResultPtr* resultPtr)
{
    resultPtr->reset(new CreateTableResult);
}

void OTSProtocolBuilder::BuildProtobufRequest(
    const ListTableRequestPtr& requestPtr,
    MessagePtr* pbRequestPtr)
{
    pbRequestPtr->reset(new protocol::ListTableRequest());
}

void OTSProtocolBuilder::ParseProtobufResponse(
    const std::string& responseBody,
    ListTableResultPtr* resultPtr,
    MessagePtr* pbResponsePtr)
{
    pbResponsePtr->reset(new protocol::ListTableResponse);
    protocol::ListTableResponse* pbResponse = 
        dynamic_cast<protocol::ListTableResponse*>(pbResponsePtr->get());
    if (!pbResponse->ParseFromString(responseBody)) {
        throw OTSClientException("Invalid response body.");
    }
}

void OTSProtocolBuilder::ParseProtobufResult(
    const MessagePtr& pbResponsePtr,
    ListTableResultPtr* resultPtr)
{
    if (pbResponsePtr.get() == NULL) return;
    protocol::ListTableResponse* pbResponse = 
        dynamic_cast<protocol::ListTableResponse*>(pbResponsePtr.get());
    resultPtr->reset(new ListTableResult);
    for (int i = 0; i < pbResponse->table_names_size(); ++i) {
        (*resultPtr)->AddTableName(pbResponse->table_names(i));
    }
}

void OTSProtocolBuilder::BuildProtobufRequest(
    const DescribeTableRequestPtr& requestPtr,
    MessagePtr* pbRequestPtr)
{
    pbRequestPtr->reset(new protocol::DescribeTableRequest());
    protocol::DescribeTableRequest* pbRequest =
        dynamic_cast<protocol::DescribeTableRequest*>(pbRequestPtr->get());

    pbRequest->set_table_name(requestPtr->GetTableName());
}

void OTSProtocolBuilder::ParseProtobufResponse(
    const std::string& responseBody,
    DescribeTableResultPtr* resultPtr,
    MessagePtr* pbResponsePtr)
{
    pbResponsePtr->reset(new protocol::DescribeTableResponse);
    protocol::DescribeTableResponse* pbResponse =
        dynamic_cast<protocol::DescribeTableResponse*>(pbResponsePtr->get());
    if (!pbResponse->ParseFromString(responseBody)) {
        throw OTSClientException("Invalid response body.");
    }
}

void OTSProtocolBuilder::ParseProtobufResult(
    const MessagePtr& pbResponsePtr,
    DescribeTableResultPtr* resultPtr)
{
    if (pbResponsePtr.get() == NULL) return;
    protocol::DescribeTableResponse* pbResponse =
        dynamic_cast<protocol::DescribeTableResponse*>(pbResponsePtr.get());
    resultPtr->reset(new DescribeTableResult);

    // 1. meta
    TableMeta tableMeta;
    ToTableMeta(pbResponse->table_meta(), &tableMeta);
    (*resultPtr)->SetTableMeta(tableMeta);

    // 2. SetReservedThroughputDetails
    ReservedThroughputDetails details;
    ToReservedThroughputDetails(pbResponse->reserved_throughput_details(), &details);
    (*resultPtr)->SetReservedThroughputDetails(details);

    // 3. Table Options
    TableOptions options;
    ToTableOptions(pbResponse->table_options(), options);
    (*resultPtr)->SetTableOptions(options);

    // 4. TableStatus
    (*resultPtr)->SetTableStatus((TableStatus)(pbResponse->table_status()));

    // 5. shard splits
    deque<PrimaryKey> splits;
    for(int64_t i = 0, sz = pbResponse->shard_splits_size(); i < sz; ++i) {
        const string& point = pbResponse->shard_splits(i);
        PlainBufferInputStream inputStream(point);
        PlainBufferCodedInputStream codedInputStream(&inputStream);
        RowPtr row = codedInputStream.ReadRow();
        splits.push_back(row->GetPrimaryKey());
    }
    (*resultPtr)->SetShardSplits(splits);
}

void OTSProtocolBuilder::BuildProtobufRequest(
    const ComputeSplitsBySizeRequestPtr& requestPtr,
    MessagePtr* pbRequestPtr)
{
    protocol::ComputeSplitPointsBySizeRequest* pbRequest = new protocol::ComputeSplitPointsBySizeRequest();
    pbRequestPtr->reset(pbRequest);

    pbRequest->set_table_name(requestPtr->GetTableName());
    pbRequest->set_split_size(requestPtr->GetSplitSize());
}

void OTSProtocolBuilder::ParseProtobufResponse(
    const std::string& responseBody,
    ComputeSplitsBySizeResultPtr* resultPtr,
    MessagePtr* pbResponsePtr)
{
    protocol::ComputeSplitPointsBySizeResponse* pbResponse = new protocol::ComputeSplitPointsBySizeResponse();
    pbResponsePtr->reset(pbResponse);

    if (!pbResponse->ParseFromString(responseBody)) {
        throw OTSClientException("Invalid response body.");
    }
}

namespace {

void ComplementPrimaryKey(
    PrimaryKey* pkey,
    const IVector<PrimaryKeySchema>& schema,
    const PrimaryKeyValue& val)
{
    int64_t curSz = pkey->GetPrimaryKeyColumns().size();
    if (curSz < schema.Size()) {
        for(int64_t i = curSz, sz = schema.Size(); i < sz; ++i) {
            pkey->AddPrimaryKeyColumn(schema.Get(i).GetName(), val);
        }
    }
}

void ToCapacityUnit(CapacityUnit* out, const protocol::CapacityUnit& in)
{
    if (in.has_read()) {
        out->SetReadCapacityUnit(in.read());
    }
    if (in.has_write()) {
        out->SetWriteCapacityUnit(in.write());
    }
}

} // namespace

void OTSProtocolBuilder::ParseProtobufResult(
    const MessagePtr& pbResponsePtr,
    ComputeSplitsBySizeResultPtr* resultPtr)
{
    if (pbResponsePtr.get() == NULL) {
        return;
    }
    protocol::ComputeSplitPointsBySizeResponse* pbResponse =
        dynamic_cast<protocol::ComputeSplitPointsBySizeResponse*>(pbResponsePtr.get());
    resultPtr->reset(new ComputeSplitsBySizeResult);

    ToCapacityUnit(
        (*resultPtr)->MutableConsumedCapacity(),
        pbResponse->consumed().capacity_unit());

    // 1. schema
    for (int64_t i = 0; i < pbResponse->schema_size(); ++i) {
        PrimaryKeySchema* schema = (*resultPtr)->MutableSchema()->Append();
        const protocol::PrimaryKeySchema& primaryKeySchema = pbResponse->schema(i);
        schema->SetName(primaryKeySchema.name());
        schema->SetType(ToPrimaryKeyType(primaryKeySchema.type()));
        if (primaryKeySchema.has_option()) {
            schema->SetOption(ToPrimaryKeyOption(primaryKeySchema.option()));
        }
    }

    // 2. splits
    const IVector<PrimaryKeySchema>& schema = (*resultPtr)->GetSchema();
    const PrimaryKeyValue infMin = PrimaryKeyValue(InfMin());
    const PrimaryKeyValue infMax = PrimaryKeyValue(InfMax());
    shared_ptr<PrimaryKey> lower(new PrimaryKey());
    ComplementPrimaryKey(lower.get(), schema, infMin);
    for(int64_t i = 0, sz = pbResponse->split_points_size(); i < sz; ++i) {
        Split* split = (*resultPtr)->MutableSplits()->Append();
        const string& point = pbResponse->split_points(i);
        PlainBufferInputStream inputStream(point);
        PlainBufferCodedInputStream codedInputStream(&inputStream);
        RowPtr row = codedInputStream.ReadRow();
        const PrimaryKey& upper = row->GetPrimaryKey();
        split->mLowerBound = lower;
        split->mUpperBound.reset(new PrimaryKey(upper));
        ComplementPrimaryKey(split->mUpperBound.get(), schema, infMin);
        lower = split->mUpperBound;
    }
    Split* last = (*resultPtr)->MutableSplits()->Append();
    last->mLowerBound = lower;
    last->mUpperBound.reset(new PrimaryKey());
    ComplementPrimaryKey(last->mUpperBound.get(), schema, infMax);

    // 2.1 locations
    int64_t splitIdx = 0;
    for(int64_t i = 0, sz = pbResponse->locations_size(); i < sz; ++i) {
        const protocol::ComputeSplitPointsBySizeResponse_SplitLocation& pbLoc =
            pbResponse->locations(i);
        const string& location = pbLoc.location();
        int64_t repeat = pbLoc.repeat();
        for(; splitIdx < (*resultPtr)->GetSplits().Size() && repeat > 0; ++splitIdx, --repeat) {
            (*resultPtr)->MutableSplits()->MutableGet(splitIdx)->mLocation = location;
        }
        if (repeat > 0) {
            throw OTSClientException("Locations length is incorrect.");
        }
    }
    if (splitIdx != (*resultPtr)->GetSplits().Size()) {
        throw OTSClientException("Locations length is incorrect.");
    }
}

void OTSProtocolBuilder::BuildProtobufRequest(
    const DeleteTableRequestPtr& requestPtr,
    MessagePtr* pbRequestPtr)
{
    pbRequestPtr->reset(new protocol::DeleteTableRequest());
    protocol::DeleteTableRequest* pbRequest =
        dynamic_cast<protocol::DeleteTableRequest*>(pbRequestPtr->get());

    pbRequest->set_table_name(requestPtr->GetTableName());
}

void OTSProtocolBuilder::ParseProtobufResponse(
    const std::string& responseBody,
    DeleteTableResultPtr* resultPtr,
    MessagePtr* pbResponsePtr)
{
    pbResponsePtr->reset(new protocol::DeleteTableResponse);
    protocol::DeleteTableResponse* pbResponse =
        dynamic_cast<protocol::DeleteTableResponse*>(pbResponsePtr->get());
    if (!pbResponse->ParseFromString(responseBody)) {
        throw OTSClientException("Invalid response body.");
    }
}

void OTSProtocolBuilder::ParseProtobufResult(
    const MessagePtr& pbResponsePtr,
    DeleteTableResultPtr* resultPtr)
{
    resultPtr->reset(new DeleteTableResult);
}

void OTSProtocolBuilder::BuildProtobufRequest(
    const UpdateTableRequestPtr& requestPtr,
    MessagePtr* pbRequestPtr)
{
    pbRequestPtr->reset(new protocol::UpdateTableRequest());
    protocol::UpdateTableRequest* pbRequest =
        dynamic_cast<protocol::UpdateTableRequest*>(pbRequestPtr->get());

    pbRequest->set_table_name(requestPtr->GetTableName());
    if (requestPtr->HasReservedThroughputForUpdate()) {
        ToReservedThroughput(
            requestPtr->GetReservedThroughputForUpdate(),
            pbRequest->mutable_reserved_throughput());
    }
    if (requestPtr->HasTableOptionsForUpdate()) {
        ToTableOptions(
            requestPtr->GetTableOptionsForUpdate(),
            pbRequest->mutable_table_options());
    }
}

void OTSProtocolBuilder::ParseProtobufResponse(
    const std::string& responseBody,
    UpdateTableResultPtr* resultPtr,
    MessagePtr* pbResponsePtr)
{
    pbResponsePtr->reset(new protocol::UpdateTableResponse);
    protocol::UpdateTableResponse* pbResponse =
        dynamic_cast<protocol::UpdateTableResponse*>(pbResponsePtr->get());
    if (!pbResponse->ParseFromString(responseBody)) {
        throw OTSClientException("Invalid response body.");
    }
}

void OTSProtocolBuilder::ParseProtobufResult(
    const MessagePtr& pbResponsePtr,
    UpdateTableResultPtr* resultPtr)
{
    resultPtr->reset(new UpdateTableResult);
}

void OTSProtocolBuilder::BuildProtobufRequest(
    const GetRowRequestPtr& requestPtr,
    MessagePtr* pbRequestPtr)
{
    pbRequestPtr->reset(new protocol::GetRowRequest());
    protocol::GetRowRequest* pbRequest =
        dynamic_cast<protocol::GetRowRequest*>(pbRequestPtr->get());

    const SingleRowQueryCriteria& queryCriteria = requestPtr->GetRowQueryCriteria();
    pbRequest->set_table_name(queryCriteria.GetTableName());
    const PrimaryKey& primaryKey = queryCriteria.GetPrimaryKey();
    if (primaryKey.GetPrimaryKeyColumnsSize() <= 0) {
        throw OTSClientException("Primary key is not set.");
    }
    CheckPrimaryKeyInfValue(primaryKey);

    pbRequest->set_primary_key(
        PlainBufferBuilder::SerializePrimaryKey(queryCriteria.GetPrimaryKey()));

    const list<string>& columnsToGet = queryCriteria.GetColumnsToGet();
    typeof(columnsToGet.begin()) iter = columnsToGet.begin(); 
    for (; iter != columnsToGet.end(); ++iter) {
        pbRequest->add_columns_to_get(*iter);
    }
    if (queryCriteria.HasTimeRange()) {
        ToTimeRange(queryCriteria.GetTimeRange(), pbRequest->mutable_time_range());
    }
    if (queryCriteria.HasMaxVersions()) {
        pbRequest->set_max_versions(queryCriteria.GetMaxVersions());
    }
    if (queryCriteria.HasCacheBlocks()) {
        pbRequest->set_cache_blocks(queryCriteria.GetCacheBlocks());
    }
    if (queryCriteria.HasFilter()) {
        pbRequest->set_filter(ToFilter(queryCriteria.GetFilter().get()));
    }
}

void OTSProtocolBuilder::ParseProtobufResponse(
    const std::string& responseBody,
    GetRowResultPtr* resultPtr,
    MessagePtr* pbResponsePtr)
{
    pbResponsePtr->reset(new protocol::GetRowResponse);
    protocol::GetRowResponse* pbResponse =
        dynamic_cast<protocol::GetRowResponse*>(pbResponsePtr->get());
    if (!pbResponse->ParseFromString(responseBody)) {
        throw OTSClientException("Invalid response body.");
    }
}

void OTSProtocolBuilder::ParseProtobufResult(
    const MessagePtr& pbResponsePtr,
    GetRowResultPtr* resultPtr)
{
    if (pbResponsePtr.get() == NULL) return;
    protocol::GetRowResponse* pbResponse =
        dynamic_cast<protocol::GetRowResponse*>(pbResponsePtr.get());
    resultPtr->reset(new GetRowResult);

    ConsumedCapacity consumedCapacity;
    ToConsumedCapacity(pbResponse->consumed(), &consumedCapacity);
    (*resultPtr)->SetConsumedCapacity(consumedCapacity);

    if (!pbResponse->row().empty()) {
        PlainBufferInputStream inputStream(pbResponse->row());
        PlainBufferCodedInputStream codedInputStream(&inputStream);
        RowPtr row = codedInputStream.ReadRow();
        (*resultPtr)->SetRow(row);
    }
}

void OTSProtocolBuilder::BuildProtobufRequest(
    const PutRowRequestPtr& requestPtr,
    MessagePtr* pbRequestPtr)
{
    pbRequestPtr->reset(new protocol::PutRowRequest());
    protocol::PutRowRequest* pbRequest =
        dynamic_cast<protocol::PutRowRequest*>(pbRequestPtr->get());

    const RowPutChange& rowChange = requestPtr->GetRowChange();
    pbRequest->set_table_name(rowChange.GetTableName());
    const PrimaryKey& primaryKey = rowChange.GetPrimaryKey();
    if (primaryKey.GetPrimaryKeyColumnsSize() <= 0) {
        throw OTSClientException("Primary key is not set."); 
    }
    CheckPrimaryKeyInfValue(primaryKey);

    if (rowChange.GetColumnsSize() <= 0) {
        throw OTSClientException("Column is not set.");
    }
    pbRequest->set_row(PlainBufferBuilder::SerializeForRow(rowChange));

    protocol::ReturnContent *returnContent = pbRequest->mutable_return_content();
    returnContent->set_return_type(ToReturnType(rowChange.GetReturnType()));
    
    const Condition& condition = rowChange.GetCondition();
    protocol::Condition *pbCondition = pbRequest->mutable_condition();
    pbCondition->set_row_existence(ToRowExistence(condition.GetRowCondition()));

    const ColumnConditionPtr& columnCondition = condition.GetColumnCondition();
    if (columnCondition != NULL) {
        pbCondition->set_column_condition(ToFilter(columnCondition.get()));
    }
}

void OTSProtocolBuilder::ParseProtobufResponse(
    const std::string& responseBody,
    PutRowResultPtr* resultPtr,
    MessagePtr* pbResponsePtr)
{
    pbResponsePtr->reset(new protocol::PutRowResponse);
    protocol::PutRowResponse* pbResponse =
        dynamic_cast<protocol::PutRowResponse*>(pbResponsePtr->get());
    if (!pbResponse->ParseFromString(responseBody)) {
        throw OTSClientException("Invalid response body.");
    }
}

void OTSProtocolBuilder::ParseProtobufResult(
    const MessagePtr& pbResponsePtr,
    PutRowResultPtr* resultPtr)
{
    if (pbResponsePtr.get() == NULL) return;
    protocol::PutRowResponse* pbResponse =
        dynamic_cast<protocol::PutRowResponse*>(pbResponsePtr.get());
    resultPtr->reset(new PutRowResult);

    ConsumedCapacity consumedCapacity;
    ToConsumedCapacity(pbResponse->consumed(), &consumedCapacity);
    (*resultPtr)->SetConsumedCapacity(consumedCapacity);


    if (pbResponse->has_row() && !pbResponse->row().empty()) {
        PlainBufferInputStream inputStream(pbResponse->row());
        PlainBufferCodedInputStream codedInputStream(&inputStream);
        RowPtr row = codedInputStream.ReadRow();
        (*resultPtr)->SetRow(row);
    }
}

void OTSProtocolBuilder::BuildProtobufRequest(
    const UpdateRowRequestPtr& requestPtr,
    MessagePtr* pbRequestPtr)
{
    pbRequestPtr->reset(new protocol::UpdateRowRequest());
    protocol::UpdateRowRequest* pbRequest =
        dynamic_cast<protocol::UpdateRowRequest*>(pbRequestPtr->get());

    const RowUpdateChange& rowChange = requestPtr->GetRowChange();
    pbRequest->set_table_name(rowChange.GetTableName());
    const PrimaryKey& primaryKey = rowChange.GetPrimaryKey();
    if (primaryKey.GetPrimaryKeyColumnsSize() <= 0) {
        throw OTSClientException("Primary key is not set."); 
    }
    CheckPrimaryKeyInfValue(primaryKey);
    
    if (rowChange.GetColumnsSize() <= 0) {
        throw OTSClientException("Column is not set.");
    }
    pbRequest->set_row_change(PlainBufferBuilder::SerializeForRow(rowChange));

    protocol::ReturnContent *returnContent = pbRequest->mutable_return_content();
    returnContent->set_return_type(ToReturnType(rowChange.GetReturnType()));

    const Condition& condition = rowChange.GetCondition();
    protocol::Condition *pbCondition = pbRequest->mutable_condition();
    pbCondition->set_row_existence(ToRowExistence(condition.GetRowCondition()));

    const ColumnConditionPtr& columnCondition = condition.GetColumnCondition();
    if (columnCondition != NULL) {
        pbCondition->set_column_condition(ToFilter(columnCondition.get()));
    }
}

void OTSProtocolBuilder::ParseProtobufResponse(
    const std::string& responseBody,
    UpdateRowResultPtr* resultPtr,
    MessagePtr* pbResponsePtr)
{
    pbResponsePtr->reset(new protocol::UpdateRowResponse);
    protocol::UpdateRowResponse* pbResponse =
        dynamic_cast<protocol::UpdateRowResponse*>(pbResponsePtr->get());
    if (!pbResponse->ParseFromString(responseBody)) {
        throw OTSClientException("Invalid response body.");
    }
}

void OTSProtocolBuilder::ParseProtobufResult(
    const MessagePtr& pbResponsePtr,
    UpdateRowResultPtr* resultPtr)
{
    if (pbResponsePtr.get() == NULL) return;
    protocol::UpdateRowResponse* pbResponse =
        dynamic_cast<protocol::UpdateRowResponse*>(pbResponsePtr.get());
    resultPtr->reset(new UpdateRowResult);

    ConsumedCapacity consumedCapacity;
    ToConsumedCapacity(pbResponse->consumed(), &consumedCapacity);
    (*resultPtr)->SetConsumedCapacity(consumedCapacity);

    if (pbResponse->has_row() && !pbResponse->row().empty()) {
        PlainBufferInputStream inputStream(pbResponse->row());
        PlainBufferCodedInputStream codedInputStream(&inputStream);
        RowPtr row = codedInputStream.ReadRow();
        (*resultPtr)->SetRow(row);
    }
}

void OTSProtocolBuilder::BuildProtobufRequest(
    const DeleteRowRequestPtr& requestPtr,
    MessagePtr* pbRequestPtr)
{
    pbRequestPtr->reset(new protocol::DeleteRowRequest());
    protocol::DeleteRowRequest* pbRequest =
        dynamic_cast<protocol::DeleteRowRequest*>(pbRequestPtr->get());

    const RowDeleteChange& rowChange = requestPtr->GetRowChange();
    pbRequest->set_table_name(rowChange.GetTableName());
    const PrimaryKey& primaryKey = rowChange.GetPrimaryKey();
    if (primaryKey.GetPrimaryKeyColumnsSize() <= 0) {
        throw OTSClientException("Primary key is not set."); 
    }
    CheckPrimaryKeyInfValue(primaryKey);
    
    pbRequest->set_primary_key(PlainBufferBuilder::SerializeForRow(rowChange));

    protocol::ReturnContent *returnContent = pbRequest->mutable_return_content();
    returnContent->set_return_type(ToReturnType(rowChange.GetReturnType()));

    const Condition& condition = rowChange.GetCondition();
    protocol::Condition *pbCondition = pbRequest->mutable_condition();
    pbCondition->set_row_existence(ToRowExistence(condition.GetRowCondition()));

    const ColumnConditionPtr& columnCondition = condition.GetColumnCondition();
    if (columnCondition != NULL) {
        pbCondition->set_column_condition(ToFilter(columnCondition.get()));
    }
}

void OTSProtocolBuilder::ParseProtobufResponse(
    const std::string& responseBody,
    DeleteRowResultPtr* resultPtr,
    MessagePtr* pbResponsePtr)
{
    pbResponsePtr->reset(new protocol::DeleteRowResponse);
    protocol::DeleteRowResponse* pbResponse =
        dynamic_cast<protocol::DeleteRowResponse*>(pbResponsePtr->get());
    if (!pbResponse->ParseFromString(responseBody)) {
        throw OTSClientException("Invalid response body.");
    }
}

void OTSProtocolBuilder::ParseProtobufResult(
    const MessagePtr& pbResponsePtr,
    DeleteRowResultPtr* resultPtr)
{
    if (pbResponsePtr.get() == NULL) return;
    protocol::DeleteRowResponse* pbResponse =
        dynamic_cast<protocol::DeleteRowResponse*>(pbResponsePtr.get());
    resultPtr->reset(new DeleteRowResult);

    ConsumedCapacity consumedCapacity;
    ToConsumedCapacity(pbResponse->consumed(), &consumedCapacity);
    (*resultPtr)->SetConsumedCapacity(consumedCapacity);

    if (pbResponse->has_row() && !pbResponse->row().empty()) {
        PlainBufferInputStream inputStream(pbResponse->row());
        PlainBufferCodedInputStream codedInputStream(&inputStream);
        RowPtr row = codedInputStream.ReadRow();
        (*resultPtr)->SetRow(row);
    }
}

void OTSProtocolBuilder::BuildProtobufRequest(
    const BatchGetRowRequestPtr& requestPtr,
    MessagePtr* pbRequestPtr)
{
    pbRequestPtr->reset(new protocol::BatchGetRowRequest());
    protocol::BatchGetRowRequest* pbRequest =
        dynamic_cast<protocol::BatchGetRowRequest*>(pbRequestPtr->get());

    const map<string, MultiRowQueryCriteria>& criterias = requestPtr->GetCriterias();
    for (typeof(criterias.begin()) iter = criterias.begin();
         iter != criterias.end();
         ++iter)
    {
        protocol::TableInBatchGetRowRequest* table = pbRequest->add_tables();
        const MultiRowQueryCriteria& queryCriteria = iter->second;
        table->set_table_name(queryCriteria.GetTableName());

        const list<PrimaryKey>& primaryKeys = queryCriteria.GetPrimaryKeys();
        if (primaryKeys.empty()) {
            throw OTSClientException("Primary key is not set.");
        }
        typeof(primaryKeys.begin()) pkIter = primaryKeys.begin();
        for (; pkIter != primaryKeys.end(); ++pkIter) {
            if (pkIter->GetPrimaryKeyColumnsSize() <= 0) {
                throw OTSClientException("Primary key is not set.");
            }
            CheckPrimaryKeyInfValue(*pkIter);
            
            table->add_primary_key(PlainBufferBuilder::SerializePrimaryKey(*pkIter));
        }

        const list<string>& columnsToGet = queryCriteria.GetColumnsToGet();
        for (typeof(columnsToGet.begin()) iter = columnsToGet.begin();
             iter != columnsToGet.end();
             ++iter)
        {
            table->add_columns_to_get(*iter);
        }
        if (queryCriteria.HasTimeRange()) {
            ToTimeRange(queryCriteria.GetTimeRange(), table->mutable_time_range());
        }
        if (queryCriteria.HasMaxVersions()) {
            table->set_max_versions(queryCriteria.GetMaxVersions());
        }
        if (queryCriteria.HasCacheBlocks()) {
            table->set_cache_blocks(queryCriteria.GetCacheBlocks());
        }
    }
}

void OTSProtocolBuilder::ParseProtobufResponse(
    const std::string& responseBody,
    BatchGetRowResultPtr* resultPtr,
    MessagePtr* pbResponsePtr)
{
    pbResponsePtr->reset(new protocol::BatchGetRowResponse);
    protocol::BatchGetRowResponse* pbResponse =
        dynamic_cast<protocol::BatchGetRowResponse*>(pbResponsePtr->get());
    if (!pbResponse->ParseFromString(responseBody)) {
        throw OTSClientException("Invalid response body.");
    }
}

void OTSProtocolBuilder::ParseProtobufResult(
    const MessagePtr& pbResponsePtr,
    BatchGetRowResultPtr* resultPtr)
{
    if (pbResponsePtr.get() == NULL) return;
    protocol::BatchGetRowResponse* pbResponse =
        dynamic_cast<protocol::BatchGetRowResponse*>(pbResponsePtr.get());
    resultPtr->reset(new BatchGetRowResult);

    for (int32_t i = 0; i < pbResponse->tables_size(); ++i) {
        const protocol::TableInBatchGetRowResponse& pbTable = pbResponse->tables(i);
        for (int32_t j = 0; j < pbTable.rows_size(); ++j) {
            const protocol::RowInBatchGetRowResponse& pbRow = pbTable.rows(j);
            BatchGetRowResult::RowResult rowResult(pbTable.table_name());
            if (pbRow.is_ok()) {
                rowResult.SetIsSuccessful(true);

                ConsumedCapacity consumedCapacity;
                ToConsumedCapacity(pbRow.consumed(), &consumedCapacity);
                rowResult.SetConsumedCapacity(consumedCapacity);

                if (!pbRow.row().empty()) {
                    PlainBufferInputStream inputStream(pbRow.row());
                    PlainBufferCodedInputStream codedInputStream(&inputStream);
                    RowPtr row = codedInputStream.ReadRow();
                    rowResult.SetRow(row);
                }
            } else {
                rowResult.SetIsSuccessful(false);
                Error error;
                ToError(pbRow.error(), &error);
                rowResult.SetError(error);
            }
            (*resultPtr)->AddRowResult(rowResult);
        }
    }
}

void OTSProtocolBuilder::MergeBatchGetRowResponse(
    MessagePtr& initPBRequestPtr,
    MessagePtr& lastPBResponsePtr,
    MessagePtr& pbRequestPtr,
    MessagePtr& pbResponsePtr,
    vector<Error>* requestErrors)
{
    requestErrors->clear();
    // merge last response and this response 
    if (initPBRequestPtr.get() == NULL) {
        initPBRequestPtr = pbRequestPtr;
    }
    if (lastPBResponsePtr.get() == NULL) {
        lastPBResponsePtr = pbResponsePtr;
    } else {
        protocol::BatchGetRowResponse* pbResponse =
            dynamic_cast<protocol::BatchGetRowResponse*>(pbResponsePtr.get());
        protocol::BatchGetRowResponse* lastPBResponse =
            dynamic_cast<protocol::BatchGetRowResponse*>(lastPBResponsePtr.get());
        int respTableIndex = 0;
        for (int i = 0; i < lastPBResponse->tables_size(); ++i) {
            protocol::TableInBatchGetRowResponse* lastTable = lastPBResponse->mutable_tables(i);
            protocol::TableInBatchGetRowResponse* pbTable = pbResponse->mutable_tables(respTableIndex);
            if (lastTable->table_name() == pbTable->table_name()) {
                int respRowIndex = 0; 
                for (int j = 0; j < lastTable->rows_size(); ++j) {
                    protocol::RowInBatchGetRowResponse* lastRow = lastTable->mutable_rows(j);  
                    if (!lastRow->is_ok()) {
                        lastRow->Swap(pbTable->mutable_rows(respRowIndex));
                        ++respRowIndex;
                    }
                }
                if (++respTableIndex >= pbResponse->tables_size()) {
                    break;
                }
            }
        }
        pbResponsePtr = lastPBResponsePtr;
    }

    // 1. pickup failed rows and construct protobuf request
    // 2. get errors of all failed rows
    pbRequestPtr.reset(new protocol::BatchGetRowRequest());
    protocol::BatchGetRowRequest* pbRequest =
        dynamic_cast<protocol::BatchGetRowRequest*>(pbRequestPtr.get());
    protocol::BatchGetRowRequest* initPBRequest =
        dynamic_cast<protocol::BatchGetRowRequest*>(initPBRequestPtr.get());
    protocol::BatchGetRowResponse* lastPBResponse =
        dynamic_cast<protocol::BatchGetRowResponse*>(lastPBResponsePtr.get());
    for (int i = 0; i < lastPBResponse->tables_size(); ++i) {
        bool firstFailedRowInTable = true;
        protocol::TableInBatchGetRowRequest* pbTable = NULL;
        const protocol::TableInBatchGetRowRequest& tableRequest = initPBRequest->tables(i);
        const protocol::TableInBatchGetRowResponse& tableResponse = lastPBResponse->tables(i);
        for (int j = 0; j < tableResponse.rows_size(); ++j) {
            const protocol::RowInBatchGetRowResponse& rowResponse = tableResponse.rows(j);
            if (!rowResponse.is_ok()) {
                // add row in new request.
                if (firstFailedRowInTable) {
                    pbTable = pbRequest->add_tables();
                    pbTable->set_table_name(tableRequest.table_name());
                    if (tableRequest.columns_to_get_size() > 0) {
                        RepeatedPtrField<string>* columnsToGet = pbTable->mutable_columns_to_get();
                        *columnsToGet = tableRequest.columns_to_get();
                    }
                    if (tableRequest.has_time_range()) {
                        protocol::TimeRange* timeRange = pbTable->mutable_time_range();
                        *timeRange = tableRequest.time_range();
                    }
                    if (tableRequest.has_max_versions()) {
                        pbTable->set_max_versions(tableRequest.max_versions());
                    }
                    if (tableRequest.has_cache_blocks()) {
                        pbTable->set_cache_blocks(tableRequest.cache_blocks());
                    }
                    if (tableRequest.has_filter()) {
                        pbTable->set_filter(tableRequest.filter());
                    }
                    firstFailedRowInTable = false;
                }
                pbTable->add_primary_key(tableRequest.primary_key(j));
                // get error of the failed row
                const protocol::Error& pbError = rowResponse.error();
                Error error;
                error.SetCode(pbError.code());
                error.SetMessage(pbError.message());
                requestErrors->push_back(error);
            }
        }
    }
}
    
void OTSProtocolBuilder::BuildProtobufRequest(
    const BatchWriteRowRequestPtr& requestPtr,
    MessagePtr* pbRequestPtr)
{
    pbRequestPtr->reset(new protocol::BatchWriteRowRequest());
    protocol::BatchWriteRowRequest* pbRequest =
        dynamic_cast<protocol::BatchWriteRowRequest*>(pbRequestPtr->get());

    const map<std::string, std::list<RowPutChange> >& putChanges = requestPtr->GetRowPutChanges();
    typeof(putChanges.begin()) putIter = putChanges.begin();
    for (; putIter != putChanges.end(); ++putIter) {
        BuildRowChange<RowPutChange>(protocol::PUT, putIter->first, 
                putIter->second, pbRequest);
    }

    const map<std::string, std::list<RowUpdateChange> >& updateChanges = requestPtr->GetRowUpdateChanges();
    typeof(updateChanges.begin()) updateIter = updateChanges.begin();
    for (; updateIter != updateChanges.end(); ++updateIter) {
        BuildRowChange<RowUpdateChange>(protocol::UPDATE, 
                updateIter->first, updateIter->second, pbRequest);
    }

    const map<std::string, std::list<RowDeleteChange> >& deleteChanges = requestPtr->GetRowDeleteChanges();
    typeof(deleteChanges.begin()) deleteIter = deleteChanges.begin(); 
    for (; deleteIter != deleteChanges.end(); ++deleteIter) {
        BuildRowChange<RowDeleteChange>(protocol::DELETE, 
                deleteIter->first, deleteIter->second, pbRequest);
    }
}

template <typename ROW_TYPE>
void OTSProtocolBuilder::BuildRowChange(protocol::OperationType type, 
                                        const string& tableName,
                                        const list<ROW_TYPE>& rowChanges, 
                                        protocol::BatchWriteRowRequest* pbRequest)
{
    protocol::TableInBatchWriteRowRequest* pbTable = NULL;
    for (int i = 0; i < pbRequest->tables_size(); i++) {
        protocol::TableInBatchWriteRowRequest* table = pbRequest->mutable_tables(i);
        if (tableName == table->table_name()) {
            pbTable = table;
            break;
        }
    }

    if (pbTable == NULL) {
        pbTable = pbRequest->add_tables();
        pbTable->set_table_name(tableName);
    }

    if (rowChanges.empty()) {
        throw OTSClientException("RowChange is not set.");
    }

    typeof(rowChanges.begin()) rowIter = rowChanges.begin();
    for (; rowIter != rowChanges.end(); ++rowIter) {
        const PrimaryKey& primaryKey = rowIter->GetPrimaryKey();
        if (primaryKey.GetPrimaryKeyColumnsSize() <= 0) {
            throw OTSClientException("Primary key is not set.");
        }

        CheckPrimaryKeyInfValue(primaryKey);

        protocol::RowInBatchWriteRowRequest *rowInRequest = pbTable->add_rows();
        rowInRequest->set_type(type);
        rowInRequest->set_row_change(PlainBufferBuilder::SerializeForRow(*rowIter));

        protocol::ReturnContent *returnContent = rowInRequest->mutable_return_content();
        returnContent->set_return_type(ToReturnType(rowIter->GetReturnType()));
        
        const Condition& condition = rowIter->GetCondition();
        protocol::Condition *pbCondition = rowInRequest->mutable_condition();
        pbCondition->set_row_existence(ToRowExistence(condition.GetRowCondition()));

        const ColumnConditionPtr& columnCondition = condition.GetColumnCondition();
        if (columnCondition != NULL) {
            pbCondition->set_column_condition(ToFilter(columnCondition.get()));
        }
    }
}

void OTSProtocolBuilder::ParseProtobufResponse(
    const std::string& responseBody,
    BatchWriteRowResultPtr* resultPtr,
    MessagePtr* pbResponsePtr)
{
    pbResponsePtr->reset(new protocol::BatchWriteRowResponse);
    protocol::BatchWriteRowResponse* pbResponse =
        dynamic_cast<protocol::BatchWriteRowResponse*>(pbResponsePtr->get());
    if (!pbResponse->ParseFromString(responseBody)) {
        throw OTSClientException("Invalid response body.");
    }
}

void OTSProtocolBuilder::ParseProtobufResult(
        const MessagePtr& pbRequestPtr,
        const MessagePtr& pbResponsePtr,
        BatchWriteRowResultPtr* resultPtr)
{
    if (pbResponsePtr.get() == NULL) return;

    protocol::BatchWriteRowRequest* pbRequest = 
        dynamic_cast<protocol::BatchWriteRowRequest*>(pbRequestPtr.get());
    
    protocol::BatchWriteRowResponse* pbResponse =
        dynamic_cast<protocol::BatchWriteRowResponse*>(pbResponsePtr.get());

    resultPtr->reset(new BatchWriteRowResult);

    for (int32_t i = 0; i < pbResponse->tables_size(); ++i) {
        const protocol::TableInBatchWriteRowRequest& pbRequestTable = pbRequest->tables(i);
        const protocol::TableInBatchWriteRowResponse& pbResponseTable = pbResponse->tables(i);

        for (int32_t j = 0; j < pbResponseTable.rows_size(); ++j) {
            const protocol::RowInBatchWriteRowRequest& pbRequestRow = pbRequestTable.rows(j);
            const protocol::RowInBatchWriteRowResponse& pbResponseRow = pbResponseTable.rows(j);

            BatchWriteRowResult::RowResult rowResult(pbResponseTable.table_name());
            if (pbResponseRow.is_ok()) {
                rowResult.SetIsSuccessful(true);
                ConsumedCapacity consumedCapacity;
                ToConsumedCapacity(pbResponseRow.consumed(), &consumedCapacity);
                rowResult.SetConsumedCapacity(consumedCapacity);

                if (pbResponseRow.has_row() && !pbResponseRow.row().empty()) {
                    PlainBufferInputStream inputStream(pbResponseRow.row());
                    PlainBufferCodedInputStream codedInputStream(&inputStream);
                    RowPtr row = codedInputStream.ReadRow();
                    rowResult.SetRow(row);
                }
            } else {
                rowResult.SetIsSuccessful(false);
                Error error;
                ToError(pbResponseRow.error(), &error);
                rowResult.SetError(error);
            }
            
            if (pbRequestRow.type() == protocol::PUT) {
                (*resultPtr)->AddPutRowResult(rowResult);
            } else if (pbRequestRow.type() == protocol::UPDATE) {
                (*resultPtr)->AddUpdateRowResult(rowResult);
            } else if (pbRequestRow.type() == protocol::DELETE) {
                (*resultPtr)->AddDeleteRowResult(rowResult);
            }
        }
    }
}

void OTSProtocolBuilder::MergeBatchWriteRowResponse(
    MessagePtr& initPBRequestPtr,
    MessagePtr& lastPBResponsePtr,
    MessagePtr& pbRequestPtr,
    MessagePtr& pbResponsePtr,
    std::vector<Error>* requestErrors)
{
    requestErrors->clear();
    // merge last response and this response 
    if (initPBRequestPtr.get() == NULL) {
        initPBRequestPtr = pbRequestPtr;
    }
    if (lastPBResponsePtr.get() == NULL) {
        lastPBResponsePtr = pbResponsePtr;
    } else {
        protocol::BatchWriteRowResponse* pbResponse =
            dynamic_cast<protocol::BatchWriteRowResponse*>(pbResponsePtr.get());
        protocol::BatchWriteRowResponse* lastPBResponse =
            dynamic_cast<protocol::BatchWriteRowResponse*>(lastPBResponsePtr.get());
        int respTableIndex = 0;
        for (int i = 0; i < lastPBResponse->tables_size(); ++i) {
            protocol::TableInBatchWriteRowResponse* lastTable = lastPBResponse->mutable_tables(i);
            protocol::TableInBatchWriteRowResponse* pbTable = pbResponse->mutable_tables(respTableIndex);
            if (lastTable->table_name() == pbTable->table_name()) {
                int respRowIndex = 0; 
                for (int j = 0; j < lastTable->rows_size(); ++j) {
                    protocol::RowInBatchWriteRowResponse* lastRow = lastTable->mutable_rows(j);  
                    if (!lastRow->is_ok()) {
                        lastRow->Swap(pbTable->mutable_rows(respRowIndex));
                        ++respRowIndex;
                    }
                }

                if (++respTableIndex >= pbResponse->tables_size()) {
                    break;
                }
            }
        }
        pbResponsePtr = lastPBResponsePtr;
    }

    // 1. pickup failed rows and construct protobuf request
    // 2. get errors of all failed rows
    pbRequestPtr.reset(new protocol::BatchWriteRowRequest());
    protocol::BatchWriteRowRequest* pbRequest =
        dynamic_cast<protocol::BatchWriteRowRequest*>(pbRequestPtr.get());
    protocol::BatchWriteRowRequest* initPBRequest =
        dynamic_cast<protocol::BatchWriteRowRequest*>(initPBRequestPtr.get());
    protocol::BatchWriteRowResponse* lastPBResponse =
        dynamic_cast<protocol::BatchWriteRowResponse*>(lastPBResponsePtr.get());
    for (int i = 0; i < lastPBResponse->tables_size(); ++i) {
        protocol::TableInBatchWriteRowRequest* pbTable = NULL;
        const protocol::TableInBatchWriteRowRequest& tableRequest = initPBRequest->tables(i);
        const protocol::TableInBatchWriteRowResponse& tableResponse = lastPBResponse->tables(i);
        for (int j = 0; j < tableResponse.rows_size(); ++j) {
            const protocol::RowInBatchWriteRowResponse& rowResponse = tableResponse.rows(j);
            if (!rowResponse.is_ok()) {
                // add row in new request.
                if (pbTable == NULL) {
                    pbTable = pbRequest->add_tables();
                    pbTable->set_table_name(tableRequest.table_name());
                }
                pbTable->add_rows()->CopyFrom(tableRequest.rows(j));
                // get error of the failed row
                const protocol::Error& pbError = rowResponse.error();
                Error error;
                error.SetCode(pbError.code());
                error.SetMessage(pbError.message());
                requestErrors->push_back(error);
            }
        }
    }
}
    
void OTSProtocolBuilder::MergeBatchResponse(
    const std::string& requestType,
    MessagePtr& initPBRequestPtr,
    MessagePtr& lastPBResponsePtr,
    MessagePtr& pbRequestPtr,
    MessagePtr& pbResponsePtr,
    vector<Error>* requestErrors)
{
    if (requestType == kAPIBatchGetRow) {
        MergeBatchGetRowResponse(
            initPBRequestPtr,
            lastPBResponsePtr,
            pbRequestPtr,
            pbResponsePtr,
            requestErrors);
    } else if (requestType == kAPIBatchWriteRow) {
        MergeBatchWriteRowResponse(
            initPBRequestPtr,
            lastPBResponsePtr,
            pbRequestPtr,
            pbResponsePtr,
            requestErrors);
    }
}
    
void OTSProtocolBuilder::BuildProtobufRequest(
    const GetRangeRequestPtr& requestPtr,
    MessagePtr* pbRequestPtr)
{
    pbRequestPtr->reset(new protocol::GetRangeRequest());
    protocol::GetRangeRequest* pbRequest =
        dynamic_cast<protocol::GetRangeRequest*>(pbRequestPtr->get());

    const RangeRowQueryCriteria& queryCriteria = requestPtr->GetRowQueryCriteria();
    pbRequest->set_table_name(queryCriteria.GetTableName());
    if (!queryCriteria.HasDirection()) {
        throw OTSClientException("Direction is not set.");
    }
    pbRequest->set_direction(ToDirection(queryCriteria.GetDirection()));

    const list<string>& columnsToGet = queryCriteria.GetColumnsToGet();
    typeof(columnsToGet.begin()) iter = columnsToGet.begin(); 
    for (; iter != columnsToGet.end(); ++iter) {
        pbRequest->add_columns_to_get(*iter);
    }
    if (queryCriteria.HasTimeRange()) {
        ToTimeRange(queryCriteria.GetTimeRange(), pbRequest->mutable_time_range());
    }
    if (queryCriteria.HasMaxVersions()) {
        pbRequest->set_max_versions(queryCriteria.GetMaxVersions());
    }
    if (queryCriteria.HasLimit()) {
        pbRequest->set_limit(queryCriteria.GetLimit());
    }
    if (queryCriteria.HasCacheBlocks()) {
        pbRequest->set_cache_blocks(queryCriteria.GetCacheBlocks());
    }
    if (queryCriteria.HasFilter()) {
        pbRequest->set_filter(ToFilter(queryCriteria.GetFilter().get()));
    }

    const PrimaryKey& startPrimaryKey = queryCriteria.GetInclusiveStartPrimaryKey();
    if (startPrimaryKey.GetPrimaryKeyColumnsSize() <= 0) {
        throw OTSClientException("Inclusive start primary key is not set.");
    }
    const PrimaryKey& endPrimaryKey = queryCriteria.GetExclusiveEndPrimaryKey();
    if (endPrimaryKey.GetPrimaryKeyColumnsSize() <= 0) {
        throw OTSClientException("Exclusive end primary key is not set.");
    }
    pbRequest->set_inclusive_start_primary_key(
        PlainBufferBuilder::SerializePrimaryKey(queryCriteria.GetInclusiveStartPrimaryKey()));
    pbRequest->set_exclusive_end_primary_key(
        PlainBufferBuilder::SerializePrimaryKey(queryCriteria.GetExclusiveEndPrimaryKey()));
}

void OTSProtocolBuilder::ParseProtobufResponse(
    const std::string& responseBody,
    GetRangeResultPtr* resultPtr,
    MessagePtr* pbResponsePtr)
{
    pbResponsePtr->reset(new protocol::GetRangeResponse);
    protocol::GetRangeResponse* pbResponse =
        dynamic_cast<protocol::GetRangeResponse*>(pbResponsePtr->get());
    if (!pbResponse->ParseFromString(responseBody)) {
        throw OTSClientException("Invalid response body.");
    }
}

void OTSProtocolBuilder::ParseProtobufResult(
    const MessagePtr& pbResponsePtr,
    GetRangeResultPtr* resultPtr)
{
    protocol::GetRangeResponse* pbResponse =
        dynamic_cast<protocol::GetRangeResponse*>(pbResponsePtr.get());
    resultPtr->reset(new GetRangeResult);

    ConsumedCapacity consumedCapacity;
    ToConsumedCapacity(pbResponse->consumed(), &consumedCapacity);
    (*resultPtr)->SetConsumedCapacity(consumedCapacity);

    if (!pbResponse->rows().empty()) {
        PlainBufferInputStream inputStream(pbResponse->rows());
        PlainBufferCodedInputStream codedInputStream(&inputStream);
        list<RowPtr> rows = codedInputStream.ReadRows();
        (*resultPtr)->SetRows(rows);
    }

    if (pbResponse->has_next_start_primary_key()) {
        PlainBufferInputStream inputStream2(pbResponse->next_start_primary_key());
        PlainBufferCodedInputStream codedInputStream2(&inputStream2);
        RowPtr nextStartRow = codedInputStream2.ReadRow();
        (*resultPtr)->SetNextStartPrimaryKey(nextStartRow->GetPrimaryKey());
    }
}

void OTSProtocolBuilder::ParseErrorResponse(
    const std::string& responseBody,
    Error* error,
    MessagePtr* pbErrorPtr)
{
    pbErrorPtr->reset(new protocol::Error);
    protocol::Error* pbError = 
        dynamic_cast<protocol::Error*>(pbErrorPtr->get());
    if (!pbError->ParseFromString(responseBody)) {
        throw OTSClientException("Invalid response body.");
    }
    error->SetCode(pbError->code());
    error->SetMessage(pbError->message());
}

void OTSProtocolBuilder::CheckPrimaryKeyInfValue(const PrimaryKey& primaryKey)
{
    for(size_t i = 0, sz = primaryKey.GetPrimaryKeyColumnsSize(); i < sz; ++i) {
        const PrimaryKeyValue& pkValue = primaryKey.GetColumn(i).GetValue();
        if (pkValue.IsInfMin() || pkValue.IsInfMax()) {
            throw OTSClientException("InfMin or InfMax is allowed in GetRange only.");
        }
    }
}

} // end of tablestore
} // end of aliyun
