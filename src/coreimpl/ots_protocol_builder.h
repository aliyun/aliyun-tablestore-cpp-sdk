/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#ifndef OTS_PROTOCOL_BUILDER_H
#define OTS_PROTOCOL_BUILDER_H

#include "../protocol/table_store_filter.pb.h"
#include "../protocol/table_store.pb.h"
#include "ots/ots_condition.h"
#include "ots/ots_request.h"
#include <string>
#include <tr1/memory>
#include <vector>

using namespace com::aliyun::tablestore;

namespace aliyun {
namespace tablestore {

typedef std::tr1::shared_ptr<google::protobuf::Message> MessagePtr;

class OTSProtocolBuilder
{
public:

    static void ToError(
        const protocol::Error& pbError,
        Error* error);

    static protocol::PrimaryKeyType ToPrimaryKeyType(PrimaryKeyType type);

    static PrimaryKeyType ToPrimaryKeyType(protocol::PrimaryKeyType type);

    static protocol::PrimaryKeyOption ToPrimaryKeyOption(PrimaryKeyOption option);

    static PrimaryKeyOption ToPrimaryKeyOption(protocol::PrimaryKeyOption option);
    
    // TableMeta 
    static void ToTableMeta(
        const TableMeta& tableMeta,
        protocol::TableMeta* pbTableMeta); 

    static void ToTableMeta(
        const protocol::TableMeta& pbTableMeta,
        TableMeta* tableMeta);

    // ReservedThroughputDetails
    static void ToReservedThroughputDetails(
        const protocol::ReservedThroughputDetails& pbDetails,
        ReservedThroughputDetails* details);

     // ReservedThroughput
    static void ToReservedThroughput(
        const ReservedThroughput& throughput,
        protocol::ReservedThroughput* pbThroughput ); 

    static void ToReservedThroughput(
        const protocol::ReservedThroughput& pbThroughtput,
        ReservedThroughput* throughput);

    // table options
    static void ToTableOptions(
        const TableOptions& options,
        protocol::TableOptions* pbOptions); 

    static void ToTableOptions(
        const protocol::TableOptions& pbOptions,
        TableOptions& options);

    static void ToTimeRange(
        const TimeRange& timeRange,
        protocol::TimeRange* pbTimeRange);

    static void ToConsumedCapacity(
        const protocol::ConsumedCapacity& pbConsumedCapacity,
        ConsumedCapacity* consumedCapacity);

    static protocol::Direction ToDirection(RangeRowQueryCriteria::Direction direction);
    
    // filter
    static protocol::ComparatorType ToComparatorType(
            SingleColumnCondition::CompareOperator compareOperator);

    static protocol::LogicalOperator ToLogicalOperator(
            CompositeColumnCondition::LogicOperator logicOperator);

    static protocol::FilterType ToFilterType(ColumnConditionType columnConditionType);

    static protocol::RowExistenceExpectation ToRowExistence(RowExistenceExpectation expectation);

    static protocol::ReturnType ToReturnType(ReturnType returnType);

    static std::string ToSingleCondition(const SingleColumnCondition* singleCondition);

    static std::string ToCompositeCondition(const CompositeColumnCondition* compositeCondition);

    static std::string ToFilter(const ColumnCondition* columnCondition);

    // for table operations
    static void BuildProtobufRequest(
        const CreateTableRequestPtr& requestPtr,
        MessagePtr* pbRequestPtr);
    
    static void ParseProtobufResponse(
        const std::string& responseBody,
        CreateTableResultPtr* resultPtr,
        MessagePtr* pbResponsePtr);

    static void ParseProtobufResult(
        const MessagePtr& pbResponsePtr,
        CreateTableResultPtr* resultPtr);

    static void BuildProtobufRequest(
        const ListTableRequestPtr& requestPtr,
        MessagePtr* pbRequestPtr);

    static void ParseProtobufResponse(
        const std::string& responseBody,
        ListTableResultPtr* resultPtr,
        MessagePtr* pbResponsePtr);
    
    static void ParseProtobufResult(
        const MessagePtr& pbResponsePtr,
        ListTableResultPtr* resultPtr);

    static void BuildProtobufRequest(
        const DescribeTableRequestPtr& requestPtr,
        MessagePtr* pbRequestPtr);

    static void ParseProtobufResponse(
        const std::string& responseBody,
        DescribeTableResultPtr* resultPtr,
        MessagePtr* pbResponsePtr);

    static void ParseProtobufResult(
        const MessagePtr& pbResponsePtr,
        DescribeTableResultPtr* resultPtr);

    static void BuildProtobufRequest(
        const ComputeSplitsBySizeRequestPtr& requestPtr,
        MessagePtr* pbRequestPtr);

    static void ParseProtobufResponse(
        const std::string& responseBody,
        ComputeSplitsBySizeResultPtr* resultPtr,
        MessagePtr* pbResponsePtr);
    
    static void ParseProtobufResult(
        const MessagePtr& pbResponsePtr,
        ComputeSplitsBySizeResultPtr* resultPtr);

    static void BuildProtobufRequest(
        const DeleteTableRequestPtr& requestPtr,
        MessagePtr* pbRequestPtr);

    static void ParseProtobufResponse(
        const std::string& responseBody,
        DeleteTableResultPtr* resultPtr,
        MessagePtr* pbResponsePtr);
    
    static void ParseProtobufResult(
        const MessagePtr& pbResponsePtr,
        DeleteTableResultPtr* resultPtr);

    static void BuildProtobufRequest(
        const UpdateTableRequestPtr& requestPtr,
        MessagePtr* pbRequestPtr);

    static void ParseProtobufResponse(
        const std::string& responseBody,
        UpdateTableResultPtr* resultPtr,
        MessagePtr* pbResponsePtr);
    
    static void ParseProtobufResult(
        const MessagePtr& pbResponsePtr,
        UpdateTableResultPtr* resultPtr);

    // for data operations
    static void BuildProtobufRequest(
        const GetRowRequestPtr& requestPtr,
        MessagePtr* pbRequestPtr);

    static void ParseProtobufResponse(
        const std::string& responseBody,
        GetRowResultPtr* resultPtr,
        MessagePtr* pbResponsePtr);
    
    static void ParseProtobufResult(
        const MessagePtr& pbResponsePtr,
        GetRowResultPtr* resultPtr);

    static void BuildProtobufRequest(
        const PutRowRequestPtr& requestPtr,
        MessagePtr* pbRequestPtr);

    static void ParseProtobufResponse(
        const std::string& responseBody,
        PutRowResultPtr* resultPtr,
        MessagePtr* pbResponsePtr);

    static void ParseProtobufResult(
        const MessagePtr& pbResponsePtr,
        PutRowResultPtr* resultPtr);

    static void BuildProtobufRequest(
        const UpdateRowRequestPtr& requestPtr,
        MessagePtr* pbRequestPtr);

    static void ParseProtobufResponse(
        const std::string& responseBody,
        UpdateRowResultPtr* resultPtr,
        MessagePtr* pbResponsePtr);
    
    static void ParseProtobufResult(
        const MessagePtr& pbResponsePtr,
        UpdateRowResultPtr* resultPtr);

    static void BuildProtobufRequest(
        const DeleteRowRequestPtr& requestPtr,
        MessagePtr* pbRequestPtr);

    static void ParseProtobufResponse(
        const std::string& responseBody,
        DeleteRowResultPtr* resultPtr,
        MessagePtr* pbResponsePtr);
    
    static void ParseProtobufResult(
        const MessagePtr& pbResponsePtr,
        DeleteRowResultPtr* resultPtr);

    static void BuildProtobufRequest(
        const BatchGetRowRequestPtr& requestPtr,
        MessagePtr* pbRequestPtr);

    static void ParseProtobufResponse(
        const std::string& responseBody,
        BatchGetRowResultPtr* resultPtr,
        MessagePtr* pbResponsePtr);

    static void ParseProtobufResult(
        const MessagePtr& pbResponsePtr,
        BatchGetRowResultPtr* resultPtr);
    
    static void MergeBatchGetRowResponse(
        MessagePtr& initPBRequestPtr,
        MessagePtr& lastPBResponsePtr,
        MessagePtr& pbRequestPtr,
        MessagePtr& pbResponsePtr,
        std::vector<Error>* requestErrors);
    
    static void BuildProtobufRequest(
        const BatchWriteRowRequestPtr& requestPtr,
        MessagePtr* pbRequestPtr);

    static void ParseProtobufResponse(
        const std::string& responseBody,
        BatchWriteRowResultPtr* resultPtr,
        MessagePtr* pbResponsePtr);
    
    static void ParseProtobufResult(
        const MessagePtr& pbRequestPtr,
        const MessagePtr& pbResponsePtr,
        BatchWriteRowResultPtr* resultPtr);

    static void MergeBatchWriteRowResponse(
        MessagePtr& initPBRequestPtr,
        MessagePtr& lastPBResponsePtr,
        MessagePtr& pbRequestPtr,
        MessagePtr& pbResponsePtr,
        std::vector<Error>* requestErrors);
    
    static void MergeBatchResponse(
        const std::string& requestType,
        MessagePtr& initPBRequestPtr,
        MessagePtr& lastPBResponsePtr,
        MessagePtr& pbRequestPtr,
        MessagePtr& pbResponsePtr,
        std::vector<Error>* requestErrors);
    
    static void BuildProtobufRequest(
        const GetRangeRequestPtr& requestPtr,
        MessagePtr* pbRequestPtr);

    static void ParseProtobufResponse(
        const std::string& responseBody,
        GetRangeResultPtr* resultPtr,
        MessagePtr* pbResponsePtr);

    static void ParseProtobufResult(
        const MessagePtr& pbResponsePtr,
        GetRangeResultPtr* resultPtr);

    // Error
    static void ParseErrorResponse(
        const std::string& responseBody,
        Error* error,
        MessagePtr* pbErrorPtr);

    // Check parameters
    static void CheckPrimaryKeyInfValue(const PrimaryKey& primaryKey);

private:
    template <typename ROW_TYPE>
    static void BuildRowChange(protocol::OperationType type, 
                               const std::string& tableName,
                               const std::list<ROW_TYPE>& rowChanges, 
                               protocol::BatchWriteRowRequest* pbRequest);
};

} // end of tablestore
} // end of aliyun

#endif
