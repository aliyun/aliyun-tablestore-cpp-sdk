#ifndef OTS_FACTORY_H
#define OTS_FACTORY_H

#include <string>
#include <tr1/functional>
#include <stdint.h>

#include "ots_types.h"

namespace aliyun {
namespace openservices {
namespace ots {

class OTSFactory
{
public:
    CapacityUnit* NewCapacityUnit(int32_t read, int32_t write);

    ReservedThroughput* NewReservedThroughput();

    ReservedThroughputDetails* NewReservedThroughputDetails(
                CapacityUnit* cu, 
                int64_t last_increase_time, 
                int64_t last_decrease_time, 
                int32_t number_of_decreases_today);

    OTSValue* NewInfMinValue();

    OTSValue* NewInfMaxValue();

    OTSValue* NewStringValue(const std::string& value);

    OTSValue* NewIntValue(int64_t value);

    OTSValue* NewBoolValue(bool value);

    OTSValue* NewDoubleValue(double value);

    OTSValue* NewBinaryValue(const char* data, int32_t size);

    ColumnSchema* NewColumnSchema(const std::string& name, ColumnType type);

    Column* NewColumn(const std::string& name, OTSValue* value);

    ColumnUpdate* NewColumnUpdate(OperationType type, const std::string& name, OTSValue* value);

    Condition* NewCondition(RowExistenceExpectation expectation);

    TableMeta* NewTableMeta();

    CreateTableRequest* NewCreateTableRequest();

    UpdateTableRequest* NewUpdateTableRequest();

    GetRowRequest* NewGetRowRequest();

    PutRowRequest* NewPutRowRequest();

    UpdateRowRequest* NewUpdateRowRequest();

    DeleteRowRequest* NewDeleteRowRequest();

    BatchGetRequestRowItem* NewBatchGetRequestRowItem();

    BatchGetRequestTableItem* NewBatchGetRequestTableItem();

    BatchGetRowRequest* NewBatchGetRowRequest();

    BatchWriteRequestPutRowItem* NewBatchWriteRequestPutRowItem();

    BatchWriteRequestUpdateRowItem* NewBatchWriteRequestUpdateRowItem();

    BatchWriteRequestDeleteRowItem* NewBatchWriteRequestDeleteRowItem();

    BatchWriteRequestTableItem* NewBatchWriteRequestTableItem();

    BatchWriteRowRequest* NewBatchWriteRowRequest();

    GetRangeRequest* NewGetRangeRequest();

    UpdateTableResponse* NewUpdateTableResponse();

    DescribeTableResponse* NewDescribeTableResponse();

    ListTableResponse* NewListTableResponse();

    GetRowResponse* NewGetRowResponse();

    PutRowResponse* NewPutRowResponse();

    UpdateRowResponse* NewUpdateRowResponse();

    DeleteRowResponse* NewDeleteRowResponse();

    BatchGetRowResponse* NewBatchGetRowResponse();

    BatchWriteRowResponse* NewBatchWriteRowResponse();

    GetRangeResponse* NewGetRangeResponse();

    RowIterator* NewRowIterator();
};

} //end of ots
} //end of openservices
} //end of aliyun

#endif
