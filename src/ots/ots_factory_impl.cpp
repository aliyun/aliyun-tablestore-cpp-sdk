#include "ots_factory_impl.h"

#include <stdlib.h>
#include "ots_exception_impl.h"
#include "ots_types_impl.h"
#include "ots_protocol_2.pb.h"

using namespace com::aliyun;
using namespace aliyun::openservices::ots;
using namespace google::protobuf;

extern const int32_t aliyun::openservices::ots::kCapacityNone = -1;

CapacityUnit* OTSFactoryImpl::NewCapacityUnit(int32_t read, int32_t write)
{
    cloudservice::ots2::CapacityUnit* pbCapacity = NewCapacityUnitPB();
    CapacityUnitImpl* capacityImpl = new CapacityUnitImpl(pbCapacity, OWN);
    if (read != kCapacityNone) {
        capacityImpl->SetRead(read);
    }
    if (write != kCapacityNone) {
        capacityImpl->SetWrite(write);
    }
    return capacityImpl;
}

ReservedThroughput* OTSFactoryImpl::NewReservedThroughput()
{
    cloudservice::ots2::ReservedThroughput* pbThroughput = NewReservedThroughputPB();
    return new ReservedThroughputImpl(pbThroughput, OWN);
}

OTSValue* OTSFactoryImpl::NewInfMinValue()
{
    cloudservice::ots2::ColumnValue* pbValue = NewColumnValuePB();
    return new OTSInfMinValueImpl(pbValue, OWN);
}

OTSValue* OTSFactoryImpl::NewInfMaxValue()
{
    cloudservice::ots2::ColumnValue* pbValue = NewColumnValuePB();
    return new OTSInfMaxValueImpl(pbValue, OWN);
}

OTSValue* OTSFactoryImpl::NewStringValue(const std::string& value)
{
    cloudservice::ots2::ColumnValue* pbValue = NewColumnValuePB();
    OTSStringValueImpl* valueImpl = new OTSStringValueImpl(pbValue, OWN);
    valueImpl->SetValue(value);
    return valueImpl;
}

OTSValue* OTSFactoryImpl::NewIntValue(int64_t value)
{
    cloudservice::ots2::ColumnValue* pbValue = NewColumnValuePB();
    OTSIntValueImpl* valueImpl = new OTSIntValueImpl(pbValue, OWN);
    valueImpl->SetValue(value);
    return valueImpl;
}

OTSValue* OTSFactoryImpl::NewBoolValue(bool value)
{
    cloudservice::ots2::ColumnValue* pbValue = NewColumnValuePB();
    OTSBoolValueImpl* valueImpl = new OTSBoolValueImpl(pbValue, OWN);
    valueImpl->SetValue(value);
    return valueImpl;
}

OTSValue* OTSFactoryImpl::NewDoubleValue(double value)
{
    cloudservice::ots2::ColumnValue* pbValue = NewColumnValuePB();
    OTSDoubleValueImpl* valueImpl = new OTSDoubleValueImpl(pbValue, OWN);
    valueImpl->SetValue(value);
    return valueImpl;
}

OTSValue* OTSFactoryImpl::NewBinaryValue(const char* data, int32_t size)
{
    cloudservice::ots2::ColumnValue* pbValue = NewColumnValuePB();
    OTSBinaryValueImpl* valueImpl = new OTSBinaryValueImpl(pbValue, OWN);
    valueImpl->SetValue(data, size);
    return valueImpl;
}

ColumnSchema* OTSFactoryImpl::NewColumnSchema(const std::string& name, ColumnType type)
{
    cloudservice::ots2::ColumnSchema* pbScheam = NewColumnSchemaPB();
    ColumnSchemaImpl* schemaImpl = new ColumnSchemaImpl(pbScheam, OWN);
    schemaImpl->SetColumnName(name);
    schemaImpl->SetColumnType(type);
    return schemaImpl;
}

Column* OTSFactoryImpl::NewColumn(const std::string& name, OTSValue* value)
{
    if (value == NULL) {
        OTS_THROW("OTSClient", "ClientParameterError", "OTSValue is null");
    }
    cloudservice::ots2::Column* pbColumn = NewColumnPB();
    ColumnImpl* columnImpl = new ColumnImpl(pbColumn, OWN);
    columnImpl->SetColumnName(name);
    columnImpl->SetColumnValue(value);
    return columnImpl;
}

ColumnUpdate* OTSFactoryImpl::NewColumnUpdate(OperationType type, const std::string& name, OTSValue* value)
{
    cloudservice::ots2::ColumnUpdate* pbColumn = NewColumnUpdatePB();
    ColumnUpdateImpl* columnImpl = new ColumnUpdateImpl(pbColumn, OWN);
    columnImpl->SetOperationType(type);
    columnImpl->SetColumnName(name);
    if (value != NULL) {
        columnImpl->SetColumnValue(value);
    }
    return columnImpl;
}

Condition* OTSFactoryImpl::NewCondition(RowExistenceExpectation expectation)
{
    cloudservice::ots2::Condition* pbCondition = NewConditionPB();
    ConditionImpl* conditionImpl = new ConditionImpl(pbCondition, OWN);
    conditionImpl->SetRowExistenceExpectation(expectation);
    return conditionImpl;
}

TableMeta* OTSFactoryImpl::NewTableMeta()
{
    cloudservice::ots2::TableMeta* pbTableMeta = NewTableMetaPB();
    return new TableMetaImpl(pbTableMeta, OWN);
}

CreateTableRequest* OTSFactoryImpl::NewCreateTableRequest()
{
    cloudservice::ots2::CreateTableRequest* pbRequest = NewCreateTableRequestPB();
    return new CreateTableRequestImpl(pbRequest, OWN);
}

UpdateTableRequest* OTSFactoryImpl::NewUpdateTableRequest()
{
    cloudservice::ots2::UpdateTableRequest* pbRequest = NewUpdateTableRequestPB();
    return new UpdateTableRequestImpl(pbRequest, OWN);
}

GetRowRequest* OTSFactoryImpl::NewGetRowRequest()
{
    cloudservice::ots2::GetRowRequest* pbRequest = NewGetRowRequestPB();
    return new GetRowRequestImpl(pbRequest, OWN);
}

PutRowRequest* OTSFactoryImpl::NewPutRowRequest()
{
    cloudservice::ots2::PutRowRequest* pbRequest = NewPutRowRequestPB();
    return new PutRowRequestImpl(pbRequest, OWN);
}

UpdateRowRequest* OTSFactoryImpl::NewUpdateRowRequest()
{
    cloudservice::ots2::UpdateRowRequest* pbRequest = NewUpdateRowRequestPB();
    return new UpdateRowRequestImpl(pbRequest, OWN);
}

DeleteRowRequest* OTSFactoryImpl::NewDeleteRowRequest()
{
    cloudservice::ots2::DeleteRowRequest* pbRequest = NewDeleteRowRequestPB();
    return new DeleteRowRequestImpl(pbRequest, OWN);
}

BatchGetRequestRowItem* OTSFactoryImpl::NewBatchGetRequestRowItem()
{
    cloudservice::ots2::RowInBatchGetRowRequest* pbRequest = NewBatchGetRowRequestRowPB();
    return new BatchGetRequestRowItemImpl(pbRequest, OWN);
}

BatchGetRequestTableItem* OTSFactoryImpl::NewBatchGetRequestTableItem()
{
    cloudservice::ots2::TableInBatchGetRowRequest* pbRequest = NewBatchGetRowRequestTablePB();
    return new BatchGetRequestTableItemImpl(pbRequest, OWN);
}

BatchGetRowRequest* OTSFactoryImpl::NewBatchGetRowRequest()
{
    cloudservice::ots2::BatchGetRowRequest* pbRequest = NewBatchGetRowRequestPB();
    return new BatchGetRowRequestImpl(pbRequest, OWN);
}

BatchWriteRequestPutRowItem* OTSFactoryImpl::NewBatchWriteRequestPutRowItem()
{
    cloudservice::ots2::PutRowInBatchWriteRowRequest* pbRequest = NewBatchWriteRowRequestPutRowPB();
    return new BatchWriteRequestPutRowItemImpl(pbRequest, OWN);
}

BatchWriteRequestUpdateRowItem* OTSFactoryImpl::NewBatchWriteRequestUpdateRowItem()
{
    cloudservice::ots2::UpdateRowInBatchWriteRowRequest* pbRequest = NewBatchWriteRowRequestUpdateRowPB();
    return new BatchWriteRequestUpdateRowItemImpl(pbRequest, OWN);
}

BatchWriteRequestDeleteRowItem* OTSFactoryImpl::NewBatchWriteRequestDeleteRowItem()
{
    cloudservice::ots2::DeleteRowInBatchWriteRowRequest* pbRequest = NewBatchWriteRowRequestDeleteRowPB();
    return new BatchWriteRequestDeleteRowItemImpl(pbRequest, OWN);
}

BatchWriteRequestTableItem* OTSFactoryImpl::NewBatchWriteRequestTableItem()
{
    cloudservice::ots2::TableInBatchWriteRowRequest* pbRequest = NewBatchWriteRowRequestTablePB();
    return new BatchWriteRequestTableItemImpl(pbRequest, OWN);
}

BatchWriteRowRequest* OTSFactoryImpl::NewBatchWriteRowRequest()
{
    cloudservice::ots2::BatchWriteRowRequest* pbRequest = NewBatchWriteRowRequestPB();
    return new BatchWriteRowRequestImpl(pbRequest, OWN);
}

GetRangeRequest* OTSFactoryImpl::NewGetRangeRequest()
{
    cloudservice::ots2::GetRangeRequest* pbRequest = NewGetRangeRequestPB();
    return new GetRangeRequestImpl(pbRequest, OWN);
}

// response
Error* OTSFactoryImpl::NewError(cloudservice::ots2::Error* pbMessage)
{
    return new ErrorImpl(pbMessage, NOT_OWN);
}

CapacityUnit* OTSFactoryImpl::NewCapacityUnit(cloudservice::ots2::CapacityUnit* pbMessage)
{
    return new CapacityUnitImpl(pbMessage, NOT_OWN);
}

ReservedThroughputDetails* OTSFactoryImpl::NewReservedThroughputDetails(cloudservice::ots2::ReservedThroughputDetails* pbMessage)
{
    return new ReservedThroughputDetailsImpl(pbMessage, NOT_OWN);
}

OTSValue* OTSFactoryImpl::NewInfMinValue(cloudservice::ots2::ColumnValue* pbMessage)
{
    return new OTSInfMinValueImpl(pbMessage, NOT_OWN);
}

OTSValue* OTSFactoryImpl::NewInfMaxValue(cloudservice::ots2::ColumnValue* pbMessage)
{
    return new OTSInfMaxValueImpl(pbMessage, NOT_OWN);
}

OTSValue* OTSFactoryImpl::NewStringValue(cloudservice::ots2::ColumnValue* pbMessage)
{
    return new OTSStringValueImpl(pbMessage, NOT_OWN);
}

OTSValue* OTSFactoryImpl::NewIntValue(cloudservice::ots2::ColumnValue* pbMessage)
{
    return new OTSIntValueImpl(pbMessage, NOT_OWN);
}

OTSValue* OTSFactoryImpl::NewBoolValue(cloudservice::ots2::ColumnValue* pbMessage)
{
    return new OTSBoolValueImpl(pbMessage, NOT_OWN);
}

OTSValue* OTSFactoryImpl::NewDoubleValue(cloudservice::ots2::ColumnValue* pbMessage)
{
    return new OTSDoubleValueImpl(pbMessage, NOT_OWN);
}

OTSValue* OTSFactoryImpl::NewBinaryValue(cloudservice::ots2::ColumnValue* pbMessage)
{
    return new OTSBinaryValueImpl(pbMessage, NOT_OWN);
}

ColumnSchema* OTSFactoryImpl::NewColumnSchema(cloudservice::ots2::ColumnSchema* pbMessage)
{
    return new ColumnSchemaImpl(pbMessage, NOT_OWN);
}     

Column* OTSFactoryImpl::NewColumn(cloudservice::ots2::Column* pbMessage)
{
    return new ColumnImpl(pbMessage, NOT_OWN);
}

Row* OTSFactoryImpl::NewRow(cloudservice::ots2::Row* pbMessage)
{
    return new RowImpl(pbMessage, NOT_OWN);
}

TableMeta* OTSFactoryImpl::NewTableMeta(cloudservice::ots2::TableMeta* pbMessage)
{
    return new TableMetaImpl(pbMessage, NOT_OWN);
}

UpdateTableResponse* OTSFactoryImpl::NewUpdateTableResponse()
{
    cloudservice::ots2::UpdateTableResponse* pbMessage = 
        new cloudservice::ots2::UpdateTableResponse;
    return new UpdateTableResponseImpl(pbMessage, OWN);
}

DescribeTableResponse* OTSFactoryImpl::NewDescribeTableResponse()
{
    cloudservice::ots2::DescribeTableResponse* pbMessage =
        new cloudservice::ots2::DescribeTableResponse;
    return new DescribeTableResponseImpl(pbMessage, OWN);
} 

ListTableResponse* OTSFactoryImpl::NewListTableResponse()
{
    cloudservice::ots2::ListTableResponse* pbMessage = 
        new cloudservice::ots2::ListTableResponse;
    return new ListTableResponseImpl(pbMessage, OWN);
}

GetRowResponse* OTSFactoryImpl::NewGetRowResponse()
{
    cloudservice::ots2::GetRowResponse* pbMessage = 
        new cloudservice::ots2::GetRowResponse;
    return new GetRowResponseImpl(pbMessage, OWN);
}

PutRowResponse* OTSFactoryImpl::NewPutRowResponse()
{
    cloudservice::ots2::PutRowResponse* pbMessage =
        new cloudservice::ots2::PutRowResponse;
    return new PutRowResponseImpl(pbMessage, OWN);
}

UpdateRowResponse* OTSFactoryImpl::NewUpdateRowResponse()
{
    cloudservice::ots2::UpdateRowResponse* pbMessage = 
        new cloudservice::ots2::UpdateRowResponse;
    return new UpdateRowResponseImpl(pbMessage, OWN);
}

DeleteRowResponse* OTSFactoryImpl::NewDeleteRowResponse()
{
    cloudservice::ots2::DeleteRowResponse* pbMessage = 
        new cloudservice::ots2::DeleteRowResponse;
    return new DeleteRowResponseImpl(pbMessage, OWN);
}

BatchGetResponseRowItem* OTSFactoryImpl::NewBatchGetResponseRowItem(
            cloudservice::ots2::RowInBatchGetRowResponse* pbMessage)
{
    return new BatchGetResponseRowItemImpl(pbMessage, NOT_OWN);
}

BatchGetResponseTableItem* OTSFactoryImpl::NewBatchGetResponseTableItem(
            cloudservice::ots2::TableInBatchGetRowResponse* pbMessage)
{
    return new BatchGetResponseTableItemImpl(pbMessage, NOT_OWN);
}

BatchGetRowResponse* OTSFactoryImpl::NewBatchGetRowResponse()
{
    cloudservice::ots2::BatchGetRowResponse* pbMessage = 
        new cloudservice::ots2::BatchGetRowResponse;
    return new BatchGetRowResponseImpl(pbMessage, OWN);
}

BatchWriteResponseRowItem* OTSFactoryImpl::NewBatchWriteResponseRowItem(
            cloudservice::ots2::RowInBatchWriteRowResponse* pbMessage)
{
    return new BatchWriteResponseRowItemImpl(pbMessage, NOT_OWN);
}

BatchWriteResponseTableItem* OTSFactoryImpl::NewBatchWriteResponseTableItem(
            cloudservice::ots2::TableInBatchWriteRowResponse* pbMessage)
{
    return new BatchWriteResponseTableItemImpl(pbMessage, NOT_OWN);
}

BatchWriteRowResponse* OTSFactoryImpl::NewBatchWriteRowResponse()
{
    cloudservice::ots2::BatchWriteRowResponse* pbMessage = 
        new cloudservice::ots2::BatchWriteRowResponse;
    return new BatchWriteRowResponseImpl(pbMessage, OWN);
}

GetRangeResponse* OTSFactoryImpl::NewGetRangeResponse()
{
    cloudservice::ots2::GetRangeResponse* pbMessage = 
        new cloudservice::ots2::GetRangeResponse;
    return new GetRangeResponseImpl(pbMessage, OWN);
}

RowIterator* OTSFactoryImpl::NewRowIterator()
{
    cloudservice::ots2::GetRangeRequest* pbRequest = 
        new cloudservice::ots2::GetRangeRequest;
    cloudservice::ots2::GetRangeResponse* pbResponse = 
        new cloudservice::ots2::GetRangeResponse;
    return new RowIteratorImpl(pbRequest, pbResponse, OWN);
}

cloudservice::ots2::ColumnSchema* OTSFactoryImpl::NewColumnSchemaPB()
{
    return new cloudservice::ots2::ColumnSchema();
}
    
cloudservice::ots2::ColumnValue* OTSFactoryImpl::NewColumnValuePB()
{
    return new cloudservice::ots2::ColumnValue();
}

cloudservice::ots2::Column* OTSFactoryImpl::NewColumnPB()
{
    return new cloudservice::ots2::Column();
}

cloudservice::ots2::Row* OTSFactoryImpl::NewRowPB()
{
    return new cloudservice::ots2::Row();
}

cloudservice::ots2::TableMeta* OTSFactoryImpl::NewTableMetaPB()
{
    return new cloudservice::ots2::TableMeta();
}

cloudservice::ots2::Condition* OTSFactoryImpl::NewConditionPB()
{
    return new cloudservice::ots2::Condition();
}

cloudservice::ots2::CapacityUnit* OTSFactoryImpl::NewCapacityUnitPB()
{
    return new cloudservice::ots2::CapacityUnit();
}

cloudservice::ots2::ReservedThroughput* OTSFactoryImpl::NewReservedThroughputPB()
{
    return new cloudservice::ots2::ReservedThroughput();
}

cloudservice::ots2::ReservedThroughputDetails* OTSFactoryImpl::NewReservedThroughputDetailsPB()
{
    return new cloudservice::ots2::ReservedThroughputDetails();
}

cloudservice::ots2::ConsumedCapacity* OTSFactoryImpl::NewConsumedCapacityPB()
{
    return new cloudservice::ots2::ConsumedCapacity();
}

cloudservice::ots2::CreateTableRequest* OTSFactoryImpl::NewCreateTableRequestPB()
{
    return new cloudservice::ots2::CreateTableRequest();
}
    
cloudservice::ots2::CreateTableResponse* OTSFactoryImpl::NewCreateTableResponsePB()
{
    return new cloudservice::ots2::CreateTableResponse();
}

cloudservice::ots2::UpdateTableRequest* OTSFactoryImpl::NewUpdateTableRequestPB()
{
    return new cloudservice::ots2::UpdateTableRequest();
}

cloudservice::ots2::UpdateTableResponse* OTSFactoryImpl::NewUpdateTableResponsePB()
{
    return new cloudservice::ots2::UpdateTableResponse();
}

cloudservice::ots2::DescribeTableRequest* OTSFactoryImpl::NewDescribeTableRequestPB()
{
    return new cloudservice::ots2::DescribeTableRequest();
}

cloudservice::ots2::DescribeTableResponse* OTSFactoryImpl::NewDescribeTableResponsePB()
{
    return new cloudservice::ots2::DescribeTableResponse();
}

cloudservice::ots2::ListTableRequest* OTSFactoryImpl::NewListTableRequestPB()
{
    return new cloudservice::ots2::ListTableRequest();
}

cloudservice::ots2::ListTableResponse* OTSFactoryImpl::NewListTableResponsePB()
{
    return new cloudservice::ots2::ListTableResponse();
}

cloudservice::ots2::DeleteTableRequest* OTSFactoryImpl::NewDeleteTableRequestPB()
{
    return new cloudservice::ots2::DeleteTableRequest();
}

cloudservice::ots2::DeleteTableResponse* OTSFactoryImpl::NewDeleteTableResponsePB()
{
    return new cloudservice::ots2::DeleteTableResponse();
}

cloudservice::ots2::GetRowRequest* OTSFactoryImpl::NewGetRowRequestPB()
{
    return new cloudservice::ots2::GetRowRequest();
}

cloudservice::ots2::GetRowResponse* OTSFactoryImpl::NewGetRowResponsePB()
{
    return new cloudservice::ots2::GetRowResponse();
}

cloudservice::ots2::ColumnUpdate* OTSFactoryImpl::NewColumnUpdatePB()
{
    return new cloudservice::ots2::ColumnUpdate();
}

cloudservice::ots2::UpdateRowRequest* OTSFactoryImpl::NewUpdateRowRequestPB()
{
    return new cloudservice::ots2::UpdateRowRequest();
}

cloudservice::ots2::UpdateRowResponse* OTSFactoryImpl::NewUpdateRowResponsePB()
{
    return new cloudservice::ots2::UpdateRowResponse();
}

cloudservice::ots2::PutRowRequest* OTSFactoryImpl::NewPutRowRequestPB()
{
    return new cloudservice::ots2::PutRowRequest();
}
    
cloudservice::ots2::PutRowResponse* OTSFactoryImpl::NewPutRowResponsePB()
{
    return new cloudservice::ots2::PutRowResponse();
}

cloudservice::ots2::DeleteRowRequest* OTSFactoryImpl::NewDeleteRowRequestPB()
{
    return new cloudservice::ots2::DeleteRowRequest();
}

cloudservice::ots2::DeleteRowResponse* OTSFactoryImpl::NewDeleteRowResponsePB()
{
    return new cloudservice::ots2::DeleteRowResponse();
}

cloudservice::ots2::RowInBatchGetRowRequest* OTSFactoryImpl::NewBatchGetRowRequestRowPB()
{
    return new cloudservice::ots2::RowInBatchGetRowRequest();
}
    
cloudservice::ots2::TableInBatchGetRowRequest* OTSFactoryImpl::NewBatchGetRowRequestTablePB()
{
    return new cloudservice::ots2::TableInBatchGetRowRequest();
}

cloudservice::ots2::BatchGetRowRequest* OTSFactoryImpl::NewBatchGetRowRequestPB()
{
    return new cloudservice::ots2::BatchGetRowRequest();
}

cloudservice::ots2::BatchGetRowResponse* OTSFactoryImpl::NewBatchGetRowResponsePB()
{
    return new cloudservice::ots2::BatchGetRowResponse();
}

cloudservice::ots2::PutRowInBatchWriteRowRequest* OTSFactoryImpl::NewBatchWriteRowRequestPutRowPB()
{
    return new cloudservice::ots2::PutRowInBatchWriteRowRequest();
}

cloudservice::ots2::UpdateRowInBatchWriteRowRequest* OTSFactoryImpl::NewBatchWriteRowRequestUpdateRowPB()
{
    return new cloudservice::ots2::UpdateRowInBatchWriteRowRequest();
}

cloudservice::ots2::DeleteRowInBatchWriteRowRequest* OTSFactoryImpl::NewBatchWriteRowRequestDeleteRowPB()
{
    return new cloudservice::ots2::DeleteRowInBatchWriteRowRequest();
}

cloudservice::ots2::TableInBatchWriteRowRequest* OTSFactoryImpl::NewBatchWriteRowRequestTablePB()
{
    return new cloudservice::ots2::TableInBatchWriteRowRequest();
}

cloudservice::ots2::BatchWriteRowRequest* OTSFactoryImpl::NewBatchWriteRowRequestPB()
{
    return new cloudservice::ots2::BatchWriteRowRequest();
}

cloudservice::ots2::BatchWriteRowResponse* OTSFactoryImpl::NewBatchWriteRowResponsePB()
{
    return new cloudservice::ots2::BatchWriteRowResponse();
}

cloudservice::ots2::GetRangeRequest* OTSFactoryImpl::NewGetRangeRequestPB()
{
    return new cloudservice::ots2::GetRangeRequest();
}

cloudservice::ots2::GetRangeResponse* OTSFactoryImpl::NewGetRangeResponsePB()
{
    return new cloudservice::ots2::GetRangeResponse();
}
