#include "include/ots_factory.h"
#include "ots_factory_impl.h"

using namespace aliyun::openservices::ots;

CapacityUnit* OTSFactory::NewCapacityUnit(int32_t read, int32_t write)
{
    return OTSFactoryImpl::NewCapacityUnit(read, write);
}

ReservedThroughput* OTSFactory::NewReservedThroughput()
{
    return OTSFactoryImpl::NewReservedThroughput();
}

OTSValue* OTSFactory::NewInfMinValue()
{
    return OTSFactoryImpl::NewInfMinValue();
}

OTSValue* OTSFactory::NewInfMaxValue()
{
    return OTSFactoryImpl::NewInfMaxValue();
}

OTSValue* OTSFactory::NewStringValue(const std::string& value)
{
    return OTSFactoryImpl::NewStringValue(value);
}

OTSValue* OTSFactory::NewIntValue(int64_t value)
{
    return OTSFactoryImpl::NewIntValue(value);
}

OTSValue* OTSFactory::NewBoolValue(bool value)
{
    return OTSFactoryImpl::NewBoolValue(value);
}

OTSValue* OTSFactory::NewDoubleValue(double value)
{
    return OTSFactoryImpl::NewDoubleValue(value);
}

OTSValue* OTSFactory::NewBinaryValue(const char* data, int32_t size)
{
    return OTSFactoryImpl::NewBinaryValue(data, size);
}

ColumnSchema* OTSFactory::NewColumnSchema(const std::string& name, ColumnType type)
{
    return OTSFactoryImpl::NewColumnSchema(name, type);
}

Column* OTSFactory::NewColumn(const std::string& name, OTSValue* value)
{
    return OTSFactoryImpl::NewColumn(name, value);
}

ColumnUpdate* OTSFactory::NewColumnUpdate(OperationType type, const std::string& name, OTSValue* value)
{
    return OTSFactoryImpl::NewColumnUpdate(type, name, value);
}

Condition* OTSFactory::NewCondition(RowExistenceExpectation expectation)
{
    return OTSFactoryImpl::NewCondition(expectation);
}

TableMeta* OTSFactory::NewTableMeta()
{
    return OTSFactoryImpl::NewTableMeta();
}

CreateTableRequest* OTSFactory::NewCreateTableRequest()
{
    return OTSFactoryImpl::NewCreateTableRequest();
}

UpdateTableRequest* OTSFactory::NewUpdateTableRequest()
{
    return OTSFactoryImpl::NewUpdateTableRequest();
}

GetRowRequest* OTSFactory::NewGetRowRequest()
{
    return OTSFactoryImpl::NewGetRowRequest();
}

PutRowRequest* OTSFactory::NewPutRowRequest()
{
    return OTSFactoryImpl::NewPutRowRequest();
}

UpdateRowRequest* OTSFactory::NewUpdateRowRequest()
{
    return OTSFactoryImpl::NewUpdateRowRequest();
}

DeleteRowRequest* OTSFactory::NewDeleteRowRequest()
{
    return OTSFactoryImpl::NewDeleteRowRequest();
}

BatchGetRequestRowItem* OTSFactory::NewBatchGetRequestRowItem()
{
    return OTSFactoryImpl::NewBatchGetRequestRowItem();
}

BatchGetRequestTableItem* OTSFactory::NewBatchGetRequestTableItem()
{
    return OTSFactoryImpl::NewBatchGetRequestTableItem();
}

BatchGetRowRequest* OTSFactory::NewBatchGetRowRequest()
{
    return OTSFactoryImpl::NewBatchGetRowRequest();
}

BatchWriteRequestPutRowItem* OTSFactory::NewBatchWriteRequestPutRowItem()
{
    return OTSFactoryImpl::NewBatchWriteRequestPutRowItem();
}

BatchWriteRequestUpdateRowItem* OTSFactory::NewBatchWriteRequestUpdateRowItem()
{
    return OTSFactoryImpl::NewBatchWriteRequestUpdateRowItem();
}

BatchWriteRequestDeleteRowItem* OTSFactory::NewBatchWriteRequestDeleteRowItem()
{
    return OTSFactoryImpl::NewBatchWriteRequestDeleteRowItem();
}

BatchWriteRequestTableItem* OTSFactory::NewBatchWriteRequestTableItem()
{
    return OTSFactoryImpl::NewBatchWriteRequestTableItem();
}

BatchWriteRowRequest* OTSFactory::NewBatchWriteRowRequest()
{
    return OTSFactoryImpl::NewBatchWriteRowRequest();
}

GetRangeRequest* OTSFactory::NewGetRangeRequest()
{
    return OTSFactoryImpl::NewGetRangeRequest();
}

UpdateTableResponse* OTSFactory::NewUpdateTableResponse()
{
    return OTSFactoryImpl::NewUpdateTableResponse();
}

DescribeTableResponse* OTSFactory::NewDescribeTableResponse()
{
    return OTSFactoryImpl::NewDescribeTableResponse();
}

ListTableResponse* OTSFactory::NewListTableResponse()
{
    return OTSFactoryImpl::NewListTableResponse();
}

GetRowResponse* OTSFactory::NewGetRowResponse()
{
    return OTSFactoryImpl::NewGetRowResponse();
}

PutRowResponse* OTSFactory::NewPutRowResponse()
{
    return OTSFactoryImpl::NewPutRowResponse();
}

UpdateRowResponse* OTSFactory::NewUpdateRowResponse()
{
    return OTSFactoryImpl::NewUpdateRowResponse();
}

DeleteRowResponse* OTSFactory::NewDeleteRowResponse()
{
    return OTSFactoryImpl::NewDeleteRowResponse();
}

BatchGetRowResponse* OTSFactory::NewBatchGetRowResponse()
{
    return OTSFactoryImpl::NewBatchGetRowResponse();
}

BatchWriteRowResponse* OTSFactory::NewBatchWriteRowResponse()
{
    return OTSFactoryImpl::NewBatchWriteRowResponse();
}

GetRangeResponse* OTSFactory::NewGetRangeResponse()
{
    return OTSFactoryImpl::NewGetRangeResponse();
}

RowIterator* OTSFactory::NewRowIterator()
{
    return OTSFactoryImpl::NewRowIterator();
}
