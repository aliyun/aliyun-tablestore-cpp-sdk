#ifndef OTS_FACTORY_IMPL_H
#define OTS_FACTORY_IMPL_H

#include <string>
#include <tr1/functional>
#include <stdint.h>

#include "include/ots_types.h"
#include "ots_protocol_2.pb.h"

namespace aliyun {
namespace openservices {
namespace ots {

class OTSClientImpl;

class OTSFactoryImpl
{
public:
    // Request Object
    static CapacityUnit* NewCapacityUnit(int32_t read, int32_t write);

    static ReservedThroughput* NewReservedThroughput();
   
    static OTSValue* NewInfMinValue();

    static OTSValue* NewInfMaxValue();

    static OTSValue* NewStringValue(const std::string& value);

    static OTSValue* NewIntValue(int64_t value);

    static OTSValue* NewBoolValue(bool value);

    static OTSValue* NewDoubleValue(double value);

    static OTSValue* NewBinaryValue(const char* data, int32_t size);

    static ColumnSchema* NewColumnSchema(const std::string& name, ColumnType type);

    static Column* NewColumn(const std::string& name, OTSValue* value);

    static ColumnUpdate* NewColumnUpdate(OperationType type, const std::string& name, OTSValue* value);

    static Condition* NewCondition(RowExistenceExpectation expectation);

    static TableMeta* NewTableMeta();

    static CreateTableRequest* NewCreateTableRequest();

    static UpdateTableRequest* NewUpdateTableRequest();

    static GetRowRequest* NewGetRowRequest();

    static PutRowRequest* NewPutRowRequest();

    static UpdateRowRequest* NewUpdateRowRequest();

    static DeleteRowRequest* NewDeleteRowRequest();

    static BatchGetRequestRowItem* NewBatchGetRequestRowItem();

    static BatchGetRequestTableItem* NewBatchGetRequestTableItem();

    static BatchGetRowRequest* NewBatchGetRowRequest();

    static BatchWriteRequestPutRowItem* NewBatchWriteRequestPutRowItem();

    static BatchWriteRequestUpdateRowItem* NewBatchWriteRequestUpdateRowItem();

    static BatchWriteRequestDeleteRowItem* NewBatchWriteRequestDeleteRowItem();

    static BatchWriteRequestTableItem* NewBatchWriteRequestTableItem();

    static BatchWriteRowRequest* NewBatchWriteRowRequest();

    static GetRangeRequest* NewGetRangeRequest();

    //Response Object
    static Error* NewError(com::aliyun::cloudservice::ots2::Error* pbMessage); 

    static CapacityUnit* NewCapacityUnit(com::aliyun::cloudservice::ots2::CapacityUnit* pbMessage);
    
    static ReservedThroughputDetails* NewReservedThroughputDetails(com::aliyun::cloudservice::ots2::ReservedThroughputDetails* pbMessage);

    static OTSValue* NewInfMinValue(com::aliyun::cloudservice::ots2::ColumnValue* pbMessage);

    static OTSValue* NewInfMaxValue(com::aliyun::cloudservice::ots2::ColumnValue* pbMessage);

    static OTSValue* NewStringValue(com::aliyun::cloudservice::ots2::ColumnValue* pbMessage);

    static OTSValue* NewIntValue(com::aliyun::cloudservice::ots2::ColumnValue* pbMessage);

    static OTSValue* NewBoolValue(com::aliyun::cloudservice::ots2::ColumnValue* pbMessage);

    static OTSValue* NewDoubleValue(com::aliyun::cloudservice::ots2::ColumnValue* pbMessage);

    static OTSValue* NewBinaryValue(com::aliyun::cloudservice::ots2::ColumnValue* pbMessage);

    static ColumnSchema* NewColumnSchema(com::aliyun::cloudservice::ots2::ColumnSchema* pbMessage);

    static Column* NewColumn(com::aliyun::cloudservice::ots2::Column* pbMessage);

    static Row* NewRow(com::aliyun::cloudservice::ots2::Row* pbMessage);

    static TableMeta* NewTableMeta(com::aliyun::cloudservice::ots2::TableMeta* pbMessage);

    static UpdateTableResponse* NewUpdateTableResponse();

    static DescribeTableResponse* NewDescribeTableResponse();

    static ListTableResponse* NewListTableResponse();

    static GetRowResponse* NewGetRowResponse();

    static PutRowResponse* NewPutRowResponse();

    static UpdateRowResponse* NewUpdateRowResponse();

    static DeleteRowResponse* NewDeleteRowResponse();

    static BatchGetResponseRowItem* NewBatchGetResponseRowItem(
                com::aliyun::cloudservice::ots2::RowInBatchGetRowResponse* pbMessage);

    static BatchGetResponseTableItem* NewBatchGetResponseTableItem(
                com::aliyun::cloudservice::ots2::TableInBatchGetRowResponse* pbMessage);

    static BatchGetRowResponse* NewBatchGetRowResponse();

    static BatchWriteResponseRowItem* NewBatchWriteResponseRowItem(
                com::aliyun::cloudservice::ots2::RowInBatchWriteRowResponse* pbMessage);

    static BatchWriteResponseTableItem* NewBatchWriteResponseTableItem(
                com::aliyun::cloudservice::ots2::TableInBatchWriteRowResponse* pbMessage);

    static BatchWriteRowResponse* NewBatchWriteRowResponse();

    static GetRangeResponse* NewGetRangeResponse();

    static RowIterator* NewRowIterator();

    // Protobuf message
    static com::aliyun::cloudservice::ots2::ColumnSchema* NewColumnSchemaPB();

    static com::aliyun::cloudservice::ots2::ColumnValue* NewColumnValuePB();

    static com::aliyun::cloudservice::ots2::Column* NewColumnPB();

    static com::aliyun::cloudservice::ots2::Row* NewRowPB();

    static com::aliyun::cloudservice::ots2::TableMeta* NewTableMetaPB();

    static com::aliyun::cloudservice::ots2::Condition* NewConditionPB();

    static com::aliyun::cloudservice::ots2::CapacityUnit* NewCapacityUnitPB();

    static com::aliyun::cloudservice::ots2::ReservedThroughput* NewReservedThroughputPB();
    
    static com::aliyun::cloudservice::ots2::ReservedThroughputDetails* NewReservedThroughputDetailsPB();

    static com::aliyun::cloudservice::ots2::ConsumedCapacity* NewConsumedCapacityPB();

    static com::aliyun::cloudservice::ots2::CreateTableRequest* NewCreateTableRequestPB();

    static com::aliyun::cloudservice::ots2::CreateTableResponse* NewCreateTableResponsePB();

    static com::aliyun::cloudservice::ots2::UpdateTableRequest* NewUpdateTableRequestPB();

    static com::aliyun::cloudservice::ots2::UpdateTableResponse* NewUpdateTableResponsePB();

    static com::aliyun::cloudservice::ots2::DescribeTableRequest* NewDescribeTableRequestPB();

    static com::aliyun::cloudservice::ots2::DescribeTableResponse* NewDescribeTableResponsePB();

    static com::aliyun::cloudservice::ots2::ListTableRequest* NewListTableRequestPB();

    static com::aliyun::cloudservice::ots2::ListTableResponse* NewListTableResponsePB();

    static com::aliyun::cloudservice::ots2::DeleteTableRequest* NewDeleteTableRequestPB();

    static com::aliyun::cloudservice::ots2::DeleteTableResponse* NewDeleteTableResponsePB();

    static com::aliyun::cloudservice::ots2::GetRowRequest* NewGetRowRequestPB();

    static com::aliyun::cloudservice::ots2::GetRowResponse* NewGetRowResponsePB();

    static com::aliyun::cloudservice::ots2::ColumnUpdate* NewColumnUpdatePB();

    static com::aliyun::cloudservice::ots2::UpdateRowRequest* NewUpdateRowRequestPB();

    static com::aliyun::cloudservice::ots2::UpdateRowResponse* NewUpdateRowResponsePB();

    static com::aliyun::cloudservice::ots2::PutRowRequest* NewPutRowRequestPB();

    static com::aliyun::cloudservice::ots2::PutRowResponse* NewPutRowResponsePB();

    static com::aliyun::cloudservice::ots2::DeleteRowRequest* NewDeleteRowRequestPB();

    static com::aliyun::cloudservice::ots2::DeleteRowResponse* NewDeleteRowResponsePB();

    static com::aliyun::cloudservice::ots2::RowInBatchGetRowRequest* NewBatchGetRowRequestRowPB();

    static com::aliyun::cloudservice::ots2::TableInBatchGetRowRequest* NewBatchGetRowRequestTablePB();

    static com::aliyun::cloudservice::ots2::BatchGetRowRequest* NewBatchGetRowRequestPB();

    static com::aliyun::cloudservice::ots2::BatchGetRowResponse* NewBatchGetRowResponsePB();

    static com::aliyun::cloudservice::ots2::PutRowInBatchWriteRowRequest* NewBatchWriteRowRequestPutRowPB();

    static com::aliyun::cloudservice::ots2::UpdateRowInBatchWriteRowRequest* NewBatchWriteRowRequestUpdateRowPB();

    static com::aliyun::cloudservice::ots2::DeleteRowInBatchWriteRowRequest* NewBatchWriteRowRequestDeleteRowPB();

    static com::aliyun::cloudservice::ots2::TableInBatchWriteRowRequest* NewBatchWriteRowRequestTablePB();

    static com::aliyun::cloudservice::ots2::BatchWriteRowRequest* NewBatchWriteRowRequestPB();

    static com::aliyun::cloudservice::ots2::BatchWriteRowResponse* NewBatchWriteRowResponsePB();

    static com::aliyun::cloudservice::ots2::GetRangeRequest* NewGetRangeRequestPB();

    static com::aliyun::cloudservice::ots2::GetRangeResponse* NewGetRangeResponsePB();

    // Delete PB message
    static void DeletePBMessage(google::protobuf::Message* message);
};

} //end of ots
} //end of openservices
} //end of aliyun

#endif
