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
#include "serde.hpp"
#include "../types.hpp"
#include "../protocol/table_store.pb.h"
#include "../protocol/table_store_filter.pb.h"
#include "../plainbuffer/plain_buffer_builder.hpp"
#include "tablestore/util/mempiece.hpp"
#include "tablestore/util/foreach.hpp"
#include "tablestore/util/try.hpp"
#include <tr1/memory>
#include <cstdio>

using namespace std;
using namespace std::tr1;
using namespace aliyun::tablestore::util;

#define PB com::aliyun::tablestore::protocol

namespace aliyun {
namespace tablestore {
namespace core {
namespace impl {

namespace {

void concat(string& out, const deque<MemPiece>& pieces)
{
    int64_t size = 0;
    FOREACH_ITER(i, pieces) {
        size += i->length();
    }
    if (out.capacity() < out.size() + size + 1) {
        // +1 for the tailing '\0'
        out.reserve(out.size() + size + 1);
    }
    FOREACH_ITER(i, pieces) {
        out.append((char*)i->data(), i->length());
    }
}

void toError(Error& api, const PB::Error& pb)
{
    api.mutableErrorCode() = pb.code();
    if (pb.has_message()) {
        api.mutableMessage() = pb.message();
    }
}

PrimaryKeyType toPrimaryKeyType(PB::PrimaryKeyType pb)
{
    switch (pb) {
    case PB::INTEGER: return kPKT_Integer;
    case PB::STRING: return kPKT_String;
    case PB::BINARY: return kPKT_Binary;
    }
    OTS_ASSERT(false)((int) pb);
    return kPKT_Integer;
}

PB::PrimaryKeyType toPrimaryKeyType(PrimaryKeyType api)
{
    switch (api) {
    case kPKT_Integer: return PB::INTEGER;
    case kPKT_String: return PB::STRING;
    case kPKT_Binary: return PB::BINARY;
    }
    OTS_ASSERT(false)(api);
    return PB::INTEGER;
}

PB::PrimaryKeyOption toPrimaryKeyOption(PrimaryKeyColumnSchema::Option api)
{
    switch (api) {
    case PrimaryKeyColumnSchema::AutoIncrement: return PB::AUTO_INCREMENT;
    }
    OTS_ASSERT(false)(api);
    return PB::AUTO_INCREMENT;
}

PrimaryKeyColumnSchema::Option toPrimaryKeyOption(PB::PrimaryKeyOption pb)
{
    switch (pb) {
    case PB::AUTO_INCREMENT: return PrimaryKeyColumnSchema::AutoIncrement;
    }
    OTS_ASSERT(false);
    return PrimaryKeyColumnSchema::AutoIncrement;
}

void toTableMeta(PB::TableMeta& pb, const TableMeta& api)
{
    pb.set_table_name(api.tableName());

    // Primary key
    const Schema& schema = api.schema();
    for(int64_t i = 0, sz = schema.size(); i < sz; ++i) {
        PB::PrimaryKeySchema& pkeySchema = *pb.add_primary_key();
        pkeySchema.set_name(schema[i].name());
        pkeySchema.set_type(toPrimaryKeyType(schema[i].type()));
        if (schema[i].option().present()) {
            pkeySchema.set_option(toPrimaryKeyOption(*schema[i].option()));
        }
    }
}

void toTableMeta(TableMeta& api, const PB::TableMeta& pb)
{
    api.mutableTableName() = pb.table_name();

    for(int64_t i = 0, sz = pb.primary_key_size(); i < sz; ++i) {
        const PB::PrimaryKeySchema& primaryKeySchema = pb.primary_key(i);
        Schema& schema = api.mutableSchema();
        schema.append().mutableName() = primaryKeySchema.name();
        schema.back().mutableType() = toPrimaryKeyType(primaryKeySchema.type());
        if (primaryKeySchema.has_option()) {
            *schema.back().mutableOption() =
                toPrimaryKeyOption(primaryKeySchema.option());
        }
    }
}

void toCapacityUnit(PB::ReservedThroughput& pb, const CapacityUnit& api)
{
    PB::CapacityUnit& pbCu = *pb.mutable_capacity_unit();
    if (api.read().present()) {
        pbCu.set_read(*api.read());
    }
    if (api.write().present()) {
        pbCu.set_write(*api.write());
    }
}

void toCapacityUnit(CapacityUnit& api, const PB::CapacityUnit& pb)
{
    if (pb.has_read()) {
        api.mutableRead().reset(pb.read());
    }
    if (pb.has_write()) {
        api.mutableWrite().reset(pb.write());
    }
}

void toTableOptions(PB::TableOptions& pb, const TableOptions& api)
{
    if (api.timeToLive().present()) {
        pb.set_time_to_live(api.timeToLive()->toSec());
    } else {
        pb.set_time_to_live(-1);
    }
    if (api.maxVersions().present()) {
        pb.set_max_versions(*api.maxVersions());
    }
    if (api.bloomFilterType().present()) {
        pb.set_bloom_filter_type((PB::BloomFilterType) *api.bloomFilterType());
    }
    if (api.blockSize().present()) {
        pb.set_block_size(*api.blockSize());
    }
    if (api.maxTimeDeviation().present()) {
        pb.set_deviation_cell_version_in_sec(api.maxTimeDeviation()->toSec());
    }
}

void toTableOptions(TableOptions& api, const PB::TableOptions& pb)
{
    if (pb.has_time_to_live()) {
        api.mutableTimeToLive().reset(Duration::fromSec(pb.time_to_live()));
    }
    if (pb.has_max_versions()) {
        api.mutableMaxVersions().reset(pb.max_versions());
    }
    if (pb.has_bloom_filter_type()) {
        api.mutableBloomFilterType().reset(
            static_cast<BloomFilterType>(pb.bloom_filter_type()));
    }
    if (pb.has_block_size()) {
        api.mutableBlockSize().reset(pb.block_size());
    }
    if (pb.has_deviation_cell_version_in_sec()) {
        api.mutableMaxTimeDeviation().reset(
            Duration::fromSec(pb.deviation_cell_version_in_sec()));
    }
}

void toTimeRange(PB::TimeRange& pb, const TimeRange& api)
{
    int64_t tsStart = api.start().toMsec();
    int64_t tsEnd = api.end().toMsec();
    if (tsStart + 1 == tsEnd) {
        pb.set_specific_time(tsStart);
    } else {
        pb.set_start_time(tsStart);
        pb.set_end_time(tsEnd);
    }
}

PB::Direction toDirection(RangeQueryCriterion::Direction dir)
{
    switch (dir) {
    case RangeQueryCriterion::FORWARD: return PB::FORWARD;
    case RangeQueryCriterion::BACKWARD: return PB::BACKWARD;
    }
    OTS_ASSERT(false)((int) dir);
    return PB::FORWARD;
}

PB::RowExistenceExpectation toRowExistence(
    Condition::RowExistenceExpectation exp)
{
    switch (exp) {
    case Condition::kIgnore: return PB::IGNORE;
    case Condition::kExpectExist: return PB::EXPECT_EXIST;
    case Condition::kExpectNotExist: return PB::EXPECT_NOT_EXIST;
    }
    OTS_ASSERT(false)((int) exp);
    return PB::IGNORE;
}

PB::ReturnType toReturnType(RowChange::ReturnType rt)
{
    switch (rt) {
    case RowChange::kRT_None: return PB::RT_NONE;
    case RowChange::kRT_PrimaryKey: return PB::RT_PK;
    }
    OTS_ASSERT(false)((int) rt);
    return PB::RT_NONE;
}

PB::FilterType toFilterType(ColumnCondition::Type cct)
{
    switch (cct) {
    case ColumnCondition::kComposite:
        return PB::FT_COMPOSITE_COLUMN_VALUE;
    case ColumnCondition::kSingle:
        return PB::FT_SINGLE_COLUMN_VALUE;
    }
    OTS_ASSERT(false)((int) cct);
    return PB::FT_SINGLE_COLUMN_VALUE;
}

PB::ComparatorType toComparatorType(
    SingleColumnCondition::Relation rel)
{
    switch (rel) {
    case SingleColumnCondition::kEqual: return PB::CT_EQUAL;
    case SingleColumnCondition::kNotEqual: return PB::CT_NOT_EQUAL;
    case SingleColumnCondition::kLarger: return PB::CT_GREATER_THAN;
    case SingleColumnCondition::kLargerEqual: return PB::CT_GREATER_EQUAL;
    case SingleColumnCondition::kSmaller: return PB::CT_LESS_THAN;
    case SingleColumnCondition::kSmallerEqual: return PB::CT_LESS_EQUAL;
    }
    OTS_ASSERT(false)((int) rel);
    return PB::CT_EQUAL;
}

string toSingleCondition(const SingleColumnCondition& cc)
{
    PB::SingleColumnValueFilter pb;
    pb.set_column_name(cc.columnName());
    pb.set_comparator(toComparatorType(cc.relation()));
    pb.set_column_value(
        PlainBufferBuilder::SerializeColumnValue(cc.columnValue()));
    pb.set_filter_if_missing(!cc.passIfMissing());
    pb.set_latest_version_only(cc.latestVersionOnly());
    string res;
    bool ret = pb.SerializeToString(&res);
    OTS_ASSERT(ret)(cc);
    return res;
}

PB::LogicalOperator toLogicalOperator(CompositeColumnCondition::Operator op)
{
    switch (op) {
    case CompositeColumnCondition::kNot: return PB::LO_NOT;
    case CompositeColumnCondition::kAnd: return PB::LO_AND;
    case CompositeColumnCondition::kOr: return PB::LO_OR;
    }
    OTS_ASSERT(false)((int) op);
    return PB::LO_AND;
}

string toCompositeCondition(const CompositeColumnCondition& cc)
{
    PB::CompositeColumnValueFilter pb;
    pb.set_combinator(toLogicalOperator(cc.op()));
    for(int64_t i = 0, sz = cc.children().size(); i < sz; ++i) {
        const ColumnCondition& subcc = *(cc.children()[i]);
        PB::Filter& pbSubFilter = *pb.add_sub_filters();
        pbSubFilter.set_type(toFilterType(subcc.type()));
        switch(subcc.type()) {
        case ColumnCondition::kSingle:
            pbSubFilter.set_filter(toSingleCondition(
                    dynamic_cast<const SingleColumnCondition&>(subcc)));
            break;
        case ColumnCondition::kComposite:
            pbSubFilter.set_filter(toCompositeCondition(
                    dynamic_cast<const CompositeColumnCondition&>(subcc)));
            break;
        }
    }
    string res;
    bool ret = pb.SerializeToString(&res);
    OTS_ASSERT(ret)(cc);
    return res;
}

string toFilter(const ColumnCondition& cc)
{
    PB::Filter pbFilter;
    pbFilter.set_type(toFilterType(cc.type()));
    switch(cc.type()) {
    case ColumnCondition::kSingle:
        pbFilter.set_filter(toSingleCondition(
                dynamic_cast<const SingleColumnCondition&>(cc)));
        break;
    case ColumnCondition::kComposite:
        pbFilter.set_filter(toCompositeCondition(
                dynamic_cast<const CompositeColumnCondition&>(cc)));
        break;
    }
    string filterStr;
    bool ret = pbFilter.SerializeToString(&filterStr);
    OTS_ASSERT(ret)(cc);
    return filterStr;
}

template<typename T>
Optional<Error> parseBody(T& out, deque<MemPiece>& body)
{
    MemPoolZeroCopyInputStream is(body);
    bool ok = out.ParseFromZeroCopyStream(&is);
    if (!ok) {
        Error e(Error::kPredefined_CorruptedResponse);
        e.mutableMessage() = "Fail to parse protobuf in response.";
        return Optional<Error>(util::move(e));
    }
    return Optional<Error>();
}

} // namespace

Optional<Error> deserialize(
    Error& out,
    deque<MemPiece>& body)
{
    typedef com::aliyun::tablestore::protocol::Error PbError;

    MemPoolZeroCopyInputStream is(body);
    PbError pb;
    bool ret = pb.ParseFromZeroCopyStream(&is);
    if (!ret) {
        Error e(Error::kPredefined_CorruptedResponse);
        e.mutableMessage().clear();
        concat(e.mutableMessage(), body);
        return Optional<Error>(util::move(e));
    }
    toError(out, pb);
    return Optional<Error>();
}

Serde<kApi_ListTable>::Serde(MemPool& mpool)
  : mOStream(&mpool)
{}

Optional<Error> Serde<kApi_ListTable>::serialize(
    deque<MemPiece>& body,
    const ApiRequest& api)
{
    PbRequest req;
    req.SerializeToZeroCopyStream(&mOStream);
    moveAssign(body, mOStream.pieces());

    return Optional<Error>();
}

Optional<Error> Serde<kApi_ListTable>::deserialize(
    ApiResponse& api,
    deque<MemPiece>& body)
{
    PbResponse pb;
    TRY(parseBody(pb, body));

    IVector<string>& tables = api.mutableTables();
    for(int64_t i = 0; i < pb.table_names_size(); ++i) {
        tables.append() = pb.table_names(i);
    }
    return Optional<Error>();
}


Serde<kApi_CreateTable>::Serde(MemPool& mpool)
  : mOStream(&mpool)
{}

Optional<Error> Serde<kApi_CreateTable>::serialize(
    deque<MemPiece>& body,
    const ApiRequest& api)
{
    PbRequest pb;
    try {
        toTableMeta(*pb.mutable_table_meta(), api.meta());
        if (api.options().reservedThroughput().present()) {
            toCapacityUnit(*pb.mutable_reserved_throughput(), *api.options().reservedThroughput());
        }
        toTableOptions(*pb.mutable_table_options(), api.options());
        if (api.shardSplitPoints().size() > 0) {
            const IVector<PrimaryKey>& shardSplits = api.shardSplitPoints();
            string lastStr = PlainBufferBuilder::SerializePrimaryKeyValue(PrimaryKeyValue::toInfMin());
            for(int64_t i = 0, sz = shardSplits.size(); i < sz; ++i) {
                const PrimaryKey& cur = shardSplits[i];
                PB::PartitionRange& p = *pb.add_partitions();
                string curStr = PlainBufferBuilder::SerializePrimaryKeyValue(cur[0].value());
                p.set_begin(lastStr);
                p.set_end(curStr);
                lastStr = curStr;
            }
            string infmaxStr = PlainBufferBuilder::SerializePrimaryKeyValue(PrimaryKeyValue::toInfMin());
            PB::PartitionRange& p = *pb.add_partitions();
            p.set_begin(lastStr);
            p.set_end(infmaxStr);
        }
    }
    catch(const OTSClientException& ex) {
        Error e(Error::kPredefined_OTSParameterInvalid);
        e.mutableMessage() = ex.GetMessage();
        return Optional<Error>(util::move(e));
    }
    pb.SerializeToZeroCopyStream(&mOStream);
    moveAssign(body, mOStream.pieces());
    return Optional<Error>();
}

Optional<Error> Serde<kApi_CreateTable>::deserialize(
    ApiResponse& api,
    deque<MemPiece>& body)
{
    PbResponse pb;
    TRY(parseBody(pb, body));
    return Optional<Error>();
}


Serde<kApi_DeleteTable>::Serde(MemPool& mpool)
  : mOStream(&mpool)
{}

Optional<Error> Serde<kApi_DeleteTable>::serialize(
    deque<MemPiece>& body,
    const ApiRequest& api)
{
    PbRequest pb;
    pb.set_table_name(api.table());

    pb.SerializeToZeroCopyStream(&mOStream);
    moveAssign(body, mOStream.pieces());
    return Optional<Error>();
}

Optional<Error> Serde<kApi_DeleteTable>::deserialize(
    ApiResponse& api,
    deque<MemPiece>& body)
{
    MemPoolZeroCopyInputStream is(body);
    PbResponse pb;
    bool ok = pb.ParseFromZeroCopyStream(&is);
    if (!ok) {
        Error e(Error::kPredefined_CorruptedResponse);
        e.mutableMessage() = "Fail to parse protobuf in response.";
        return Optional<Error>(util::move(e));
    }

    return Optional<Error>();
}


Serde<kApi_DescribeTable>::Serde(MemPool& mpool)
  : mOStream(&mpool)
{}

Optional<Error> Serde<kApi_DescribeTable>::serialize(
    deque<MemPiece>& body,
    const ApiRequest& api)
{
    PbRequest pb;
    pb.set_table_name(api.table());

    pb.SerializeToZeroCopyStream(&mOStream);
    moveAssign(body, mOStream.pieces());
    return Optional<Error>();
}

Optional<Error> Serde<kApi_DescribeTable>::deserialize(
    ApiResponse& api,
    deque<MemPiece>& body)
{
    PbResponse pb;
    TRY(parseBody(pb, body));

    try {
        // 1. meta
        toTableMeta(api.mutableMeta(), pb.table_meta());
    
        // 2. Table Options
        toTableOptions(api.mutableOptions(), pb.table_options());

        // 3. SetReservedThroughputDetails
        CapacityUnit cu;
        toCapacityUnit(
            cu,
            pb.reserved_throughput_details().capacity_unit());
        api.mutableOptions().mutableReservedThroughput().reset(util::move(cu));

        // 4. TableStatus
        api.mutableStatus() = static_cast<TableStatus>(pb.table_status());

        // 5. shard splits
        IVector<PrimaryKey>& splits = api.mutableShardSplitPoints();
        for(int64_t i = 0, sz = pb.shard_splits_size(); i < sz; ++i) {
            const string& point = pb.shard_splits(i);
            PlainBufferInputStream is(point);
            PlainBufferCodedInputStream codedInputStream(&is);
            Row row;
            codedInputStream.ReadRow(&row);
            moveAssign(splits.append(), util::move(row.mutablePrimaryKey()));
        }
    }
    catch(const OTSClientException& ex) {
        Error e(Error::kPredefined_CorruptedResponse);
        e.mutableMessage() = ex.GetMessage();
        return Optional<Error>(util::move(e));
    }
    return Optional<Error>();
}


Serde<kApi_UpdateTable>::Serde(MemPool& mpool)
  : mOStream(&mpool)
{}

Optional<Error> Serde<kApi_UpdateTable>::serialize(
    deque<MemPiece>& body,
    const ApiRequest& api)
{
    PbRequest pb;

    pb.set_table_name(api.table());

    if (api.options().reservedThroughput().present()) {
        toCapacityUnit(
            *pb.mutable_reserved_throughput(),
            *api.options().reservedThroughput());
    }

    toTableOptions(*pb.mutable_table_options(), api.options());

    pb.SerializeToZeroCopyStream(&mOStream);
    moveAssign(body, mOStream.pieces());
    return Optional<Error>();
}

Optional<Error> Serde<kApi_UpdateTable>::deserialize(
    ApiResponse& api,
    deque<MemPiece>& body)
{
    PbResponse pb;
    TRY(parseBody(pb, body));

    return Optional<Error>();
}


Serde<kApi_ComputeSplitsBySize>::Serde(MemPool& mpool)
  : mOStream(&mpool)
{}

Optional<Error> Serde<kApi_ComputeSplitsBySize>::serialize(
    deque<MemPiece>& body,
    const ApiRequest& api)
{
    PbRequest pb;

    pb.set_table_name(api.table());
    pb.set_split_size(api.splitSize());

    pb.SerializeToZeroCopyStream(&mOStream);
    moveAssign(body, mOStream.pieces());
    return Optional<Error>();
}

namespace {

void ComplementPrimaryKey(
    PrimaryKey& pkey,
    const Schema& schema,
    const PrimaryKeyValue& val)
{
    int64_t curSz = pkey.size();
    if (curSz < schema.size()) {
        for(int64_t i = curSz, sz = schema.size(); i < sz; ++i) {
            pkey.append() = PrimaryKeyColumn(schema[i].name(), val);
        }
    }
}

} // namespace

Optional<Error> Serde<kApi_ComputeSplitsBySize>::deserialize(
    ApiResponse& api,
    deque<MemPiece>& body)
{
    PbResponse pb;
    TRY(parseBody(pb, body));

    toCapacityUnit(
        api.mutableConsumedCapacity(),
        pb.consumed().capacity_unit());

    try {
        // 1. schema
        for (int64_t i = 0; i < pb.schema_size(); ++i) {
            PrimaryKeyColumnSchema& colSchema = api.mutableSchema().append();
            const PB::PrimaryKeySchema& pbColSchema = pb.schema(i);
            colSchema.mutableName() = pbColSchema.name();
            colSchema.mutableType() = toPrimaryKeyType(pbColSchema.type());
            if (pbColSchema.has_option()) {
                *colSchema.mutableOption() = toPrimaryKeyOption(pbColSchema.option());
            }
        }

        // 2. splits
        const Schema& schema = api.schema();
        const PrimaryKeyValue& infMin = PrimaryKeyValue::toInfMin();
        const PrimaryKeyValue& infMax = PrimaryKeyValue::toInfMax();
        shared_ptr<PrimaryKey> lower(new PrimaryKey());
        ComplementPrimaryKey(*lower, schema, infMin);
        for(int64_t i = 0, sz = pb.split_points_size(); i < sz; ++i) {
            Split& split = api.mutableSplits().append();
            split.mutableLowerBound() = lower;
            {
                const string& point = pb.split_points(i);
                PlainBufferInputStream inputStream(point);
                PlainBufferCodedInputStream codedInputStream(&inputStream);
                Row tmpRow;
                codedInputStream.ReadRow(&tmpRow);
                PrimaryKey& upper = tmpRow.mutablePrimaryKey();
                ComplementPrimaryKey(upper, schema, infMin);
                split.mutableUpperBound().reset(new PrimaryKey());
                moveAssign(*split.mutableUpperBound(), util::move(upper));
            }
            lower = split.upperBound();
        }
        Split& last = api.mutableSplits().append();
        last.mutableLowerBound() = lower;
        last.mutableUpperBound().reset(new PrimaryKey());
        ComplementPrimaryKey(*last.mutableUpperBound(), schema, infMax);

        // 2.1 locations
        int64_t splitIdx = 0;
        for(int64_t i = 0, sz = pb.locations_size(); i < sz; ++i) {
            const PB::ComputeSplitPointsBySizeResponse_SplitLocation& pbLoc =
                pb.locations(i);
            const string& location = pbLoc.location();
            int64_t repeat = pbLoc.repeat();
            for(; splitIdx < api.splits().size() && repeat > 0; ++splitIdx, --repeat) {
                api.mutableSplits()[splitIdx].mutableLocation() = location;
            }
            if (repeat > 0) {
                Error e(Error::kPredefined_CorruptedResponse);
                e.mutableMessage() = "Locations length is incorrect.";
                return Optional<Error>(util::move(e));
            }
        }
        if (splitIdx != api.splits().size()) {
            Error e(Error::kPredefined_CorruptedResponse);
            e.mutableMessage() = "Locations length is incorrect.";
            return Optional<Error>(util::move(e));
        }
    }
    catch(const OTSClientException& ex) {
        Error e(Error::kPredefined_CorruptedResponse);
        e.mutableMessage() = ex.GetMessage();
        return Optional<Error>(util::move(e));
    }

    return Optional<Error>();
}


Serde<kApi_PutRow>::Serde(MemPool& mpool)
  : mOStream(&mpool)
{}

Optional<Error> Serde<kApi_PutRow>::serialize(
    deque<MemPiece>& body,
    const ApiRequest& api)
{
    PbRequest pb;

    try {
        const RowPutChange& rc = api.rowChange();
        pb.set_table_name(rc.table());
        pb.set_row(PlainBufferBuilder::SerializeForRow(rc));

        PB::ReturnContent& returnContent = *pb.mutable_return_content();
        returnContent.set_return_type(toReturnType(rc.returnType()));
    
        const Condition& condition = rc.condition();
        PB::Condition& pbCondition = *pb.mutable_condition();
        pbCondition.set_row_existence(toRowExistence(condition.rowCondition()));

        const shared_ptr<ColumnCondition>& columnCondition = condition.columnCondition();
        if (columnCondition.get() != NULL) {
            pbCondition.set_column_condition(toFilter(*columnCondition));
        }
    }
    catch(const OTSClientException& ex) {
        Error e(Error::kPredefined_OTSParameterInvalid);
        e.mutableMessage() = ex.GetMessage();
        return Optional<Error>(util::move(e));
    }

    pb.SerializeToZeroCopyStream(&mOStream);
    moveAssign(body, mOStream.pieces());
    return Optional<Error>();
}

Optional<Error> Serde<kApi_PutRow>::deserialize(
    ApiResponse& api,
    deque<MemPiece>& body)
{
    PbResponse pb;
    TRY(parseBody(pb, body));

    return Optional<Error>();
}


Serde<kApi_GetRow>::Serde(MemPool& mpool)
  : mOStream(&mpool)
{}

Optional<Error> Serde<kApi_GetRow>::serialize(
    deque<MemPiece>& body,
    const ApiRequest& api)
{
    PbRequest pb;

    try {
        const PointQueryCriterion& queryCriterion = api.queryCriterion();
        pb.set_table_name(queryCriterion.table());
        const PrimaryKey& primaryKey = queryCriterion.primaryKey();
        pb.set_primary_key(
            PlainBufferBuilder::SerializePrimaryKey(primaryKey));

        const IVector<string>& columnsToGet = queryCriterion.columnsToGet();
        for(int64_t i = 0, sz = columnsToGet.size(); i < sz; ++i) {
            pb.add_columns_to_get(columnsToGet[i]);
        }
        if (queryCriterion.timeRange().present()) {
            toTimeRange(*pb.mutable_time_range(), *queryCriterion.timeRange());
        }
        if (queryCriterion.maxVersions().present()) {
            pb.set_max_versions(*queryCriterion.maxVersions());
        }
        if (queryCriterion.cacheBlocks().present()) {
            pb.set_cache_blocks(*queryCriterion.cacheBlocks());
        }
        if (queryCriterion.filter().get() != NULL) {
            pb.set_filter(toFilter(*queryCriterion.filter()));
        }
    }
    catch(const OTSClientException& ex) {
        Error e(Error::kPredefined_OTSParameterInvalid);
        e.mutableMessage() = ex.GetMessage();
        return Optional<Error>(util::move(e));
    }

    pb.SerializeToZeroCopyStream(&mOStream);
    moveAssign(body, mOStream.pieces());
    return Optional<Error>();
}

Optional<Error> Serde<kApi_GetRow>::deserialize(
    ApiResponse& api,
    deque<MemPiece>& body)
{
    PbResponse pb;
    TRY(parseBody(pb, body));

    toCapacityUnit(
        api.mutableConsumedCapacity(),
        pb.consumed().capacity_unit());

    try {
        if (!pb.row().empty()) {
            PlainBufferInputStream inputStream(pb.row());
            PlainBufferCodedInputStream codedInputStream(&inputStream);
            Row row;
            codedInputStream.ReadRow(&row);
            api.mutableRow().reset(util::move(row));
        }
    }
    catch(const OTSClientException& ex) {
        Error e(Error::kPredefined_CorruptedResponse);
        e.mutableMessage() = ex.GetMessage();
        return Optional<Error>(util::move(e));
    }

    return Optional<Error>();
}


Serde<kApi_GetRange>::Serde(MemPool& mpool)
  : mOStream(&mpool)
{}

Optional<Error> Serde<kApi_GetRange>::serialize(
    deque<MemPiece>& body,
    const ApiRequest& api)
{
    PbRequest pb;

    try {
        const RangeQueryCriterion& cri = api.queryCriterion();
        pb.set_table_name(cri.table());
        pb.set_direction(toDirection(cri.direction()));

        const IVector<string>& columnsToGet = cri.columnsToGet();
        for(int64_t i = 0, sz = columnsToGet.size(); i < sz; ++i) {
            pb.add_columns_to_get(columnsToGet[i]);
        }

        if (cri.timeRange().present()) {
            toTimeRange(*pb.mutable_time_range(), *cri.timeRange());
        }
        if (cri.maxVersions().present()) {
            pb.set_max_versions(*cri.maxVersions());
        }
        if (cri.limit().present()) {
            pb.set_limit(*cri.limit());
        }
        if (cri.cacheBlocks().present()) {
            pb.set_cache_blocks(*cri.cacheBlocks());
        }
        if (cri.filter().get() != NULL) {
            pb.set_filter(toFilter(*cri.filter()));
        }

        pb.set_inclusive_start_primary_key(
            PlainBufferBuilder::SerializePrimaryKey(cri.inclusiveStart()));
        pb.set_exclusive_end_primary_key(
            PlainBufferBuilder::SerializePrimaryKey(cri.exclusiveEnd()));
    }
    catch(const OTSClientException& ex) {
        Error e(Error::kPredefined_OTSParameterInvalid);
        e.mutableMessage() = ex.GetMessage();
        return Optional<Error>(util::move(e));
    }

    pb.SerializeToZeroCopyStream(&mOStream);
    moveAssign(body, mOStream.pieces());
    return Optional<Error>();
}

Optional<Error> Serde<kApi_GetRange>::deserialize(
    ApiResponse& api,
    deque<MemPiece>& body)
{
    PbResponse pb;
    TRY(parseBody(pb, body));

    toCapacityUnit(
        api.mutableConsumedCapacity(),
        pb.consumed().capacity_unit());

    try {
        if (!pb.rows().empty()) {
            PlainBufferInputStream inputStream(pb.rows());
            PlainBufferCodedInputStream codedInputStream(&inputStream);
            codedInputStream.ReadRows(&api.mutableRows());
        }

        if (pb.has_next_start_primary_key()) {
            PlainBufferInputStream inputStream(pb.next_start_primary_key());
            PlainBufferCodedInputStream codedInputStream(&inputStream);
            Row nextStart;
            codedInputStream.ReadRow(&nextStart);
            api.mutableNextStart().reset(util::move(nextStart.mutablePrimaryKey()));
        }
    }
    catch(const OTSClientException& ex) {
        Error e(Error::kPredefined_CorruptedResponse);
        e.mutableMessage() = ex.GetMessage();
        return Optional<Error>(util::move(e));
    }

    return Optional<Error>();
}


Serde<kApi_UpdateRow>::Serde(MemPool& mpool)
  : mOStream(&mpool)
{}

Optional<Error> Serde<kApi_UpdateRow>::serialize(
    deque<MemPiece>& body,
    const ApiRequest& api)
{
    PbRequest pb;

    try {
        const RowUpdateChange& rc = api.rowChange();
        pb.set_table_name(rc.table());
        pb.set_row_change(PlainBufferBuilder::SerializeForRow(rc));

        PB::ReturnContent& returnContent = *pb.mutable_return_content();
        returnContent.set_return_type(toReturnType(rc.returnType()));

        const Condition& condition = rc.condition();
        PB::Condition& pbCondition = *pb.mutable_condition();
        pbCondition.set_row_existence(toRowExistence(condition.rowCondition()));

        const shared_ptr<ColumnCondition>& columnCondition = condition.columnCondition();
        if (columnCondition.get() != NULL) {
            pbCondition.set_column_condition(toFilter(*columnCondition));
        }
    }
    catch(const OTSClientException& ex) {
        Error e(Error::kPredefined_OTSParameterInvalid);
        e.mutableMessage() = ex.GetMessage();
        return Optional<Error>(util::move(e));
    }

    pb.SerializeToZeroCopyStream(&mOStream);
    moveAssign(body, mOStream.pieces());
    return Optional<Error>();
}

Optional<Error> Serde<kApi_UpdateRow>::deserialize(
    ApiResponse& api,
    deque<MemPiece>& body)
{
    PbResponse pb;
    TRY(parseBody(pb, body));

    toCapacityUnit(
        api.mutableConsumedCapacity(),
        pb.consumed().capacity_unit());

    try {
        if (pb.has_row() && !pb.row().empty()) {
            PlainBufferInputStream inputStream(pb.row());
            PlainBufferCodedInputStream codedInputStream(&inputStream);
            Row row;
            codedInputStream.ReadRow(&row);
            api.mutableRow().reset(util::move(row));
        }
    }
    catch(const OTSClientException& ex) {
        Error e(Error::kPredefined_CorruptedResponse);
        e.mutableMessage() = ex.GetMessage();
        return Optional<Error>(util::move(e));
    }

    return Optional<Error>();
}


Serde<kApi_DeleteRow>::Serde(MemPool& mpool)
  : mOStream(&mpool)
{}

Optional<Error> Serde<kApi_DeleteRow>::serialize(
    deque<MemPiece>& body,
    const ApiRequest& api)
{
    PbRequest pb;

    try {
        const RowDeleteChange& rowChange = api.rowChange();
        pb.set_table_name(rowChange.table());
        pb.set_primary_key(PlainBufferBuilder::SerializeForRow(rowChange));

        PB::ReturnContent& returnContent = *pb.mutable_return_content();
        returnContent.set_return_type(toReturnType(rowChange.returnType()));

        const Condition& condition = rowChange.condition();
        PB::Condition& pbCondition = *pb.mutable_condition();
        pbCondition.set_row_existence(toRowExistence(condition.rowCondition()));

        const shared_ptr<ColumnCondition>& columnCondition = condition.columnCondition();
        if (columnCondition.get() != NULL) {
            pbCondition.set_column_condition(toFilter(*columnCondition));
        }
    }
    catch(const OTSClientException& ex) {
        Error e(Error::kPredefined_OTSParameterInvalid);
        e.mutableMessage() = ex.GetMessage();
        return Optional<Error>(util::move(e));
    }

    pb.SerializeToZeroCopyStream(&mOStream);
    moveAssign(body, mOStream.pieces());
    return Optional<Error>();
}

Optional<Error> Serde<kApi_DeleteRow>::deserialize(
    ApiResponse& api,
    deque<MemPiece>& body)
{
    PbResponse pb;
    TRY(parseBody(pb, body));

    toCapacityUnit(
        api.mutableConsumedCapacity(),
        pb.consumed().capacity_unit());

    try {
        if (pb.has_row() && !pb.row().empty()) {
            PlainBufferInputStream inputStream(pb.row());
            PlainBufferCodedInputStream codedInputStream(&inputStream);
            Row row;
            codedInputStream.ReadRow(&row);
            api.mutableRow().reset(util::move(row));
        }
    }
    catch(const OTSClientException& ex) {
        Error e(Error::kPredefined_CorruptedResponse);
        e.mutableMessage() = ex.GetMessage();
        return Optional<Error>(util::move(e));
    }

    return Optional<Error>();
}


Serde<kApi_BatchGetRow>::Serde(MemPool& mpool)
  : mOStream(&mpool),
    mRequest(NULL)
{}

Optional<Error> Serde<kApi_BatchGetRow>::serialize(
    deque<MemPiece>& body,
    const ApiRequest& api)
{
    mRequest = &api;
    PbRequest pb;

    try {
        for(int64_t i = 0, sz = api.criteria().size(); i < sz; ++i) {
            const MultiPointQueryCriterion& criterion = api.criteria()[i];
            PB::TableInBatchGetRowRequest& table = *pb.add_tables();
            table.set_table_name(criterion.table());
            for(int64_t j = 0, psz = criterion.rowKeys().size(); j < psz; ++j) {
                table.add_primary_key(
                    PlainBufferBuilder::SerializePrimaryKey(
                        criterion.rowKeys()[j].get()));
            }
            if (criterion.columnsToGet().size() > 0) {
                for(int64_t j = 0, sz = criterion.columnsToGet().size(); j < sz; ++j) {
                    table.add_columns_to_get(criterion.columnsToGet()[j]);
                }
            }
            if (criterion.timeRange().present()) {
                toTimeRange(*table.mutable_time_range(), *criterion.timeRange());
            }
            if (criterion.maxVersions().present()) {
                table.set_max_versions(*criterion.maxVersions());
            }
            if (criterion.cacheBlocks().present()) {
                table.set_cache_blocks(*criterion.cacheBlocks());
            }
        }
    }
    catch(const OTSClientException& ex) {
        Error e(Error::kPredefined_OTSParameterInvalid);
        e.mutableMessage() = ex.GetMessage();
        return Optional<Error>(util::move(e));
    }

    pb.SerializeToZeroCopyStream(&mOStream);
    moveAssign(body, mOStream.pieces());
    return Optional<Error>();
}

namespace {

void mergeConsumedCapacity(CapacityUnit& result, const CapacityUnit& in)
{
    if (in.read().present()) {
        if (result.read().present()) {
            *result.mutableRead() += *in.read();
        } else {
            result.mutableRead() = in.read();
        }
    }
    if (in.write().present()) {
        if (result.write().present()) {
            *result.mutableWrite() += *in.write();
        } else {
            result.mutableWrite() = in.write();
        }
    }
}

} // namespace

Optional<Error> Serde<kApi_BatchGetRow>::deserialize(
    ApiResponse& api,
    deque<MemPiece>& body)
{
    PbResponse pb;
    TRY(parseBody(pb, body));

    try {
        for (int32_t i = 0; i < pb.tables_size(); ++i) {
            const PB::TableInBatchGetRowResponse& pbTable = pb.tables(i);
            for (int32_t j = 0; j < pbTable.rows_size(); ++j) {
                const PB::RowInBatchGetRowResponse& pbRow = pbTable.rows(j);
                Result<Optional<Row>, Error>& row =
                    api.mutableResults().append().mutableGet();
                if (pbRow.is_ok()) {
                    CapacityUnit consumedCapacity;
                    toCapacityUnit(
                        consumedCapacity,
                        pbRow.consumed().capacity_unit());
                    mergeConsumedCapacity(
                        api.mutableConsumedCapacity(), consumedCapacity);

                    if (!pbRow.row().empty()) {
                        PlainBufferInputStream inputStream(pbRow.row());
                        PlainBufferCodedInputStream codedInputStream(&inputStream);
                        Row r;
                        codedInputStream.ReadRow(&r);
                        row.mutableOkValue().reset(util::move(r));
                    }
                } else {
                    Error error;
                    toError(error, pbRow.error());
                    row.mutableErrValue() = util::move(error);
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
                api.mutableResults()[i].mutableUserData() =
                    criterion.rowKeys()[j].userData();
            }
        }
    }
    catch(const OTSClientException& ex) {
        Error e(Error::kPredefined_CorruptedResponse);
        e.mutableMessage() = ex.GetMessage();
        return Optional<Error>(util::move(e));
    }

    return Optional<Error>();
}


Serde<kApi_BatchWriteRow>::Serde(MemPool& mpool)
  : mOStream(&mpool),
    mRequest(NULL)
{}

Optional<Error> Serde<kApi_BatchWriteRow>::serialize(
    deque<MemPiece>& body,
    const ApiRequest& api)
{
    mRequest = &api;
    PbRequest pb;

    try {
        {
            const IVector<BatchWriteRowRequest::Put>& rows = api.puts();
            for(int64_t i = 0, sz = rows.size(); i < sz; ++i) {
                mIndices[rows[i].get().table()].mPuts.push_back(i);
            }
        }
        {
            const IVector<BatchWriteRowRequest::Update>& rows = api.updates();
            for(int64_t i = 0, sz = rows.size(); i < sz; ++i) {
                mIndices[rows[i].get().table()].mUpdates.push_back(i);
            }
        }
        {
            const IVector<BatchWriteRowRequest::Delete>& rows = api.deletes();
            for(int64_t i = 0, sz = rows.size(); i < sz; ++i) {
                mIndices[rows[i].get().table()].mDeletes.push_back(i);
            }
        }

        FOREACH_ITER(i, mIndices) {
            const string& tableName = i->first;
            PB::TableInBatchWriteRowRequest& pbTable = *pb.add_tables();
            pbTable.set_table_name(tableName);

            FOREACH_ITER(j, i->second.mPuts) {
                const RowPutChange& row = api.puts()[*j].get();
                PB::RowInBatchWriteRowRequest& rowInRequest = *pbTable.add_rows();
                rowInRequest.set_type(PB::PUT);
                rowInRequest.set_row_change(PlainBufferBuilder::SerializeForRow(row));

                PB::ReturnContent& returnContent = *rowInRequest.mutable_return_content();
                returnContent.set_return_type(toReturnType(row.returnType()));
        
                const Condition& condition = row.condition();
                PB::Condition& pbCondition = *rowInRequest.mutable_condition();
                pbCondition.set_row_existence(toRowExistence(condition.rowCondition()));

                const shared_ptr<ColumnCondition>& columnCondition = condition.columnCondition();
                if (columnCondition.get() != NULL) {
                    pbCondition.set_column_condition(toFilter(*columnCondition));
                }
            }
            FOREACH_ITER(j, i->second.mUpdates) {
                const RowUpdateChange& row = api.updates()[*j].get();
                PB::RowInBatchWriteRowRequest& rowInRequest = *pbTable.add_rows();
                rowInRequest.set_type(PB::UPDATE);
                rowInRequest.set_row_change(PlainBufferBuilder::SerializeForRow(row));

                PB::ReturnContent& returnContent = *rowInRequest.mutable_return_content();
                returnContent.set_return_type(toReturnType(row.returnType()));
        
                const Condition& condition = row.condition();
                PB::Condition& pbCondition = *rowInRequest.mutable_condition();
                pbCondition.set_row_existence(toRowExistence(condition.rowCondition()));

                const shared_ptr<ColumnCondition>& columnCondition = condition.columnCondition();
                if (columnCondition.get() != NULL) {
                    pbCondition.set_column_condition(toFilter(*columnCondition));
                }
            }
            FOREACH_ITER(j, i->second.mDeletes) {
                const RowDeleteChange& row = api.deletes()[*j].get();
                PB::RowInBatchWriteRowRequest& rowInRequest = *pbTable.add_rows();
                rowInRequest.set_type(PB::DELETE);
                rowInRequest.set_row_change(PlainBufferBuilder::SerializeForRow(row));

                PB::ReturnContent& returnContent = *rowInRequest.mutable_return_content();
                returnContent.set_return_type(toReturnType(row.returnType()));
        
                const Condition& condition = row.condition();
                PB::Condition& pbCondition = *rowInRequest.mutable_condition();
                pbCondition.set_row_existence(toRowExistence(condition.rowCondition()));

                const shared_ptr<ColumnCondition>& columnCondition = condition.columnCondition();
                if (columnCondition.get() != NULL) {
                    pbCondition.set_column_condition(toFilter(*columnCondition));
                }
            }
        }
    }
    catch(const OTSClientException& ex) {
        Error e(Error::kPredefined_OTSParameterInvalid);
        e.mutableMessage() = ex.GetMessage();
        return Optional<Error>(util::move(e));
    }

    pb.SerializeToZeroCopyStream(&mOStream);
    moveAssign(body, mOStream.pieces());
    return Optional<Error>();
}

namespace {

void to(
    CapacityUnit& consumedCapacity,
    BatchWriteRowResponse::Result& result,
    const PB::RowInBatchWriteRowResponse& pbResponseRow)
{
    if (pbResponseRow.is_ok()) {
        CapacityUnit cu;
        toCapacityUnit(cu, pbResponseRow.consumed().capacity_unit());
        mergeConsumedCapacity(consumedCapacity, cu);

        if (pbResponseRow.has_row() && !pbResponseRow.row().empty()) {
            PlainBufferInputStream inputStream(pbResponseRow.row());
            PlainBufferCodedInputStream codedInputStream(&inputStream);
            Row row;
            codedInputStream.ReadRow(&row);
            result.mutableGet().mutableOkValue().reset(util::move(row));
        }
    } else {
        Error error;
        toError(error, pbResponseRow.error());
        result.mutableGet().mutableErrValue() = util::move(error);
    }
}

} // namespace

Optional<Error> Serde<kApi_BatchWriteRow>::deserialize(
    ApiResponse& api,
    deque<MemPiece>& body)
{
    PbResponse pb;
    TRY(parseBody(pb, body));

    try {
        if (static_cast<int64_t>(mIndices.size()) != pb.tables_size()) {
            Error err(Error::kPredefined_CorruptedResponse);
            err.mutableMessage() = "Invalid response";
            return Optional<Error>(util::move(err));
        }

        for(int64_t i = 0, sz = mRequest->puts().size(); i < sz; ++i) {
            BatchWriteRowResponse::Result& result = api.mutablePutResults().append();
            result.mutableUserData() = mRequest->puts()[i].userData();
        }
        for(int64_t i = 0, sz = mRequest->updates().size(); i < sz; ++i) {
            BatchWriteRowResponse::Result& result = api.mutableUpdateResults().append();
            result.mutableUserData() = mRequest->updates()[i].userData();
        }
        for(int64_t i = 0, sz = mRequest->deletes().size(); i < sz; ++i) {
            BatchWriteRowResponse::Result& result = api.mutableDeleteResults().append();
            result.mutableUserData() = mRequest->deletes()[i].userData();
        }
        for(int64_t i = 0, sz = pb.tables_size(); i < sz; ++i) {
            const PB::TableInBatchWriteRowResponse& pbResponseTable = pb.tables(i);
            const string& tableName = pbResponseTable.table_name();
            Indices::const_iterator it = mIndices.find(tableName);
            if (it == mIndices.end()) {
                Error err(Error::kPredefined_CorruptedResponse);
                err.mutableMessage() = "Invalid response";
                return Optional<Error>(util::move(err));
            }
            const Index& idxes = it->second;
            if (idxes.mPuts.size() + idxes.mUpdates.size() + idxes.mDeletes.size()
                != static_cast<size_t>(pbResponseTable.rows_size()))
            {
                Error err(Error::kPredefined_CorruptedResponse);
                err.mutableMessage() = "Invalid response";
                return Optional<Error>(util::move(err));
            }
            int64_t pbIdx = 0;
            FOREACH_ITER(it, idxes.mPuts) {
                const PB::RowInBatchWriteRowResponse& pbResponseRow =
                    pbResponseTable.rows(pbIdx);
                to(api.mutableConsumedCapacity(),
                    api.mutablePutResults()[*it],
                    pbResponseRow);
                ++pbIdx;
            }
            FOREACH_ITER(it, idxes.mUpdates) {
                const PB::RowInBatchWriteRowResponse& pbResponseRow =
                    pbResponseTable.rows(pbIdx);
                to(api.mutableConsumedCapacity(),
                    api.mutableUpdateResults()[*it],
                    pbResponseRow);
                ++pbIdx;
            }
            FOREACH_ITER(it, idxes.mDeletes) {
                const PB::RowInBatchWriteRowResponse& pbResponseRow =
                    pbResponseTable.rows(pbIdx);
                to(api.mutableConsumedCapacity(),
                    api.mutableDeleteResults()[*it],
                    pbResponseRow);
                ++pbIdx;
            }
        }
    }
    catch(const OTSClientException& ex) {
        Error e(Error::kPredefined_CorruptedResponse);
        e.mutableMessage() = ex.GetMessage();
        return Optional<Error>(util::move(e));
    }

    return Optional<Error>();
}

} // namespace impl
} // namespace core
} // namespace tablestore
} // namespace aliyun
