#ifndef OTS_TYPES_IMPL_H
#define OTS_TYPES_IMPL_H

#include "include/ots_types.h"
#include "ots_client_impl.h"
#include "ots_protocol_2.pb.h"

#include <string>
#include <vector>
#include <sys/types.h>
#include <tr1/memory>

namespace aliyun {
namespace openservices {
namespace ots {

class OTSClientImpl;

// Indicate the ownership of the protobuf message. 
enum Ownership
{
    OWN = 0,
    NOT_OWN = 1
};

class OTSAbstractImpl
{
    public:
        virtual ~OTSAbstractImpl() {}

        virtual google::protobuf::Message* GetPBMessage() const = 0;
};


class ErrorImpl : public Error, public OTSAbstractImpl 
{
public:
    ErrorImpl(com::aliyun::cloudservice::ots2::Error* message, Ownership ownership);

    virtual ~ErrorImpl();

    virtual const std::string& GetCode() const;

    virtual const std::string& GetMessage() const;

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::Error* mPBMessage;
};


class CapacityUnitImpl : public CapacityUnit, public OTSAbstractImpl 
{
public:
    CapacityUnitImpl(
                com::aliyun::cloudservice::ots2::CapacityUnit* message,
                Ownership ownership);

    virtual ~CapacityUnitImpl();

    virtual int32_t GetRead() const;

    virtual int32_t GetWrite() const;
    
    virtual google::protobuf::Message* GetPBMessage() const;

    void SetRead(int32_t read);

    void SetWrite(int32_t write);

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::CapacityUnit* mPBMessage;
};

class ReservedThroughputDetailsImpl : public ReservedThroughputDetails, public OTSAbstractImpl 
{
public:
    ReservedThroughputDetailsImpl(
                com::aliyun::cloudservice::ots2::ReservedThroughputDetails* message,
                Ownership ownership);

    virtual ~ReservedThroughputDetailsImpl();

    virtual const CapacityUnit& GetCapacityUnit() const;

    virtual int64_t GetLastIncreaseTime() const;

    virtual int64_t GetLastDecreaseTime() const;
    
    virtual int32_t GetNumberOfDecreasesToday() const;

    virtual google::protobuf::Message* GetPBMessage() const;
    
    void ParseFromPBMessage();

private:
    Ownership mOwnership;
    CapacityUnit* mCapacityUnit;
    com::aliyun::cloudservice::ots2::ReservedThroughputDetails* mPBMessage;
};

class ReservedThroughputImpl : public ReservedThroughput, public OTSAbstractImpl
{
public:
    ReservedThroughputImpl(
                com::aliyun::cloudservice::ots2::ReservedThroughput* message,
                Ownership ownership);

    virtual ~ReservedThroughputImpl();

    virtual void SetCapacityUnit(CapacityUnit* capacity) const;
    
    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::ReservedThroughput* mPBMessage;
};

class OTSValueImpl : public OTSAbstractImpl
{
public:
    virtual ~OTSValueImpl() {}

    virtual google::protobuf::Message* GetPBMessage() const = 0;
};


class OTSInfMinValueImpl : public OTSInfMinValue, public OTSValueImpl 
{
public:
    OTSInfMinValueImpl(
                com::aliyun::cloudservice::ots2::ColumnValue* message,
                Ownership ownership);

    virtual ~OTSInfMinValueImpl();

    virtual ColumnType GetType() const;

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::ColumnValue* mPBMessage;
};


class OTSInfMaxValueImpl : public OTSInfMaxValue, public OTSValueImpl
{
public:
    OTSInfMaxValueImpl(
                com::aliyun::cloudservice::ots2::ColumnValue* message,
                Ownership ownership);

    virtual ~OTSInfMaxValueImpl();

    virtual ColumnType GetType() const;
    
    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::ColumnValue* mPBMessage;
};

class OTSStringValueImpl : public OTSStringValue, public OTSValueImpl
{
public:
    OTSStringValueImpl(
                com::aliyun::cloudservice::ots2::ColumnValue* message,
                Ownership ownership);

    virtual ~OTSStringValueImpl();

    virtual ColumnType GetType() const;

    virtual const std::string& GetValue() const;

    void SetValue(const std::string& value);

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::ColumnValue* mPBMessage;
};


class OTSIntValueImpl : public OTSIntValue, public OTSValueImpl
{
public:
    OTSIntValueImpl(
                com::aliyun::cloudservice::ots2::ColumnValue* message,
                Ownership ownership);

    virtual ~OTSIntValueImpl();

    virtual ColumnType GetType() const;

    virtual int64_t GetValue() const;

    void SetValue(int64_t value);

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::ColumnValue* mPBMessage;
};


class OTSBoolValueImpl : public OTSBoolValue, public OTSValueImpl
{
public:
    OTSBoolValueImpl(
                com::aliyun::cloudservice::ots2::ColumnValue* message,
                Ownership ownership);

    virtual ~OTSBoolValueImpl();

    virtual ColumnType GetType() const;

    virtual bool GetValue() const;

    void SetValue(bool value);

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::ColumnValue* mPBMessage;
};


class OTSDoubleValueImpl : public OTSDoubleValue, public OTSValueImpl
{
public:
    OTSDoubleValueImpl(
                com::aliyun::cloudservice::ots2::ColumnValue* message,
                Ownership ownership);

    virtual ~OTSDoubleValueImpl();

    virtual ColumnType GetType() const;

    virtual double GetValue() const;

    void SetValue(double value);

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::ColumnValue* mPBMessage;
};


class OTSBinaryValueImpl : public OTSBinaryValue, public OTSValueImpl
{
public:
    OTSBinaryValueImpl(
                com::aliyun::cloudservice::ots2::ColumnValue* message,
                Ownership ownership);

    virtual ~OTSBinaryValueImpl();

    virtual ColumnType GetType() const;

    virtual void GetValue(char** data, int32_t* size) const;

    void SetValue(const char* data,  int32_t size);

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::ColumnValue* mPBMessage;
};


class ColumnSchemaImpl : public ColumnSchema, public OTSAbstractImpl
{
public:
    ColumnSchemaImpl(
                com::aliyun::cloudservice::ots2::ColumnSchema* message,
                Ownership ownership);

    virtual ~ColumnSchemaImpl();

    virtual const std::string& GetColumnName() const;

    virtual ColumnType GetColumnType() const;

    void SetColumnName(const std::string& name);

    void SetColumnType(ColumnType type);

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::ColumnSchema* mPBMessage;
};


class ColumnImpl : public Column, public OTSAbstractImpl
{
public:
    ColumnImpl(com::aliyun::cloudservice::ots2::Column* message, Ownership ownership);

    virtual ~ColumnImpl();

    virtual const std::string& GetColumnName() const;

    virtual const OTSValue& GetColumnValue() const;

    void SetColumnName(const std::string& name);
    
    void SetColumnValue(OTSValue* value);

    void ParseFromPBMessage();

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::Column* mPBMessage;
    OTSValue* mColumnValue;
};


class ColumnUpdateImpl : public ColumnUpdate, public OTSAbstractImpl
{
public:
    ColumnUpdateImpl(
                com::aliyun::cloudservice::ots2::ColumnUpdate* message,
                Ownership ownership);

    virtual ~ColumnUpdateImpl();

    void SetOperationType(OperationType type);

    void SetColumnName(const std::string& name);
    
    void SetColumnValue(OTSValue* value);

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::ColumnUpdate* mPBMessage;
};


class RowImpl : public Row, public OTSAbstractImpl
{
public:
    RowImpl(com::aliyun::cloudservice::ots2::Row* message, Ownership ownership);

    virtual ~RowImpl();

    virtual int32_t GetPrimaryKeySize() const;

    virtual const Column& GetPrimaryKey(int32_t index) const;

    virtual int32_t GetColumnSize() const;

    virtual const Column& GetColumn(int32_t index) const;

    virtual google::protobuf::Message* GetPBMessage() const;

    void ParseFromPBMessage();

private:
    Ownership mOwnership;
    std::vector<Column*> mPrimaryKeys;
    std::vector<Column*> mColumns;
    com::aliyun::cloudservice::ots2::Row* mPBMessage;
};


class TableMetaImpl : public TableMeta, public OTSAbstractImpl
{
public:
    TableMetaImpl(com::aliyun::cloudservice::ots2::TableMeta* message, Ownership ownership);

    virtual ~TableMetaImpl();

    virtual void SetTableName(const std::string& tableName);

    virtual void AddPrimaryKeySchema(ColumnSchema* schema);

    virtual const std::string& GetTableName() const;

    virtual int32_t GetPrimaryKeySchemaSize() const;

    virtual const ColumnSchema& GetPrimaryKeySchema(int32_t index) const;

    void ParseFromPBMessage();

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    std::vector<ColumnSchema*> mPrimaryKeySchemas;
    com::aliyun::cloudservice::ots2::TableMeta* mPBMessage;
};


class CreateTableRequestImpl : public CreateTableRequest, public OTSAbstractImpl 
{
public:
    CreateTableRequestImpl(
                com::aliyun::cloudservice::ots2::CreateTableRequest* message,
                Ownership ownership);

    virtual ~CreateTableRequestImpl();

    virtual void SetTableMeta(TableMeta* tableMeta);

    virtual void SetReservedThroughput(ReservedThroughput* throughput);

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::CreateTableRequest* mPBMessage;
};


class UpdateTableRequestImpl : public UpdateTableRequest, public OTSAbstractImpl
{
public:
    UpdateTableRequestImpl(
                com::aliyun::cloudservice::ots2::UpdateTableRequest* message,
                Ownership ownership);

    virtual ~UpdateTableRequestImpl();

    virtual void SetTableName(const std::string& tableName);

    virtual void SetReservedThroughput(ReservedThroughput* throughput);

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::UpdateTableRequest* mPBMessage;
};


class UpdateTableResponseImpl : public UpdateTableResponse, public OTSAbstractImpl
{
public:
    UpdateTableResponseImpl(
                com::aliyun::cloudservice::ots2::UpdateTableResponse* message,
                Ownership ownership);

    virtual ~UpdateTableResponseImpl();

    virtual const ReservedThroughputDetails& GetReservedThroughputDetails() const;

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    ReservedThroughputDetails* mReservedThroughputDetails;
    com::aliyun::cloudservice::ots2::UpdateTableResponse* mPBMessage;
};


class DescribeTableResponseImpl : public DescribeTableResponse, public OTSAbstractImpl
{
public:
    DescribeTableResponseImpl(
                com::aliyun::cloudservice::ots2::DescribeTableResponse* message,
                Ownership ownership);

    virtual ~DescribeTableResponseImpl();

    virtual const TableMeta& GetTableMeta() const;

    virtual const ReservedThroughputDetails& GetReservedThroughputDetails() const;

    void ParseFromPBMessage();

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    TableMeta* mTableMeta;
    ReservedThroughputDetails* mReservedThroughputDetails;
    com::aliyun::cloudservice::ots2::DescribeTableResponse* mPBMessage;
};


class ListTableResponseImpl : public ListTableResponse, public OTSAbstractImpl
{
public:
    ListTableResponseImpl(
                com::aliyun::cloudservice::ots2::ListTableResponse* message,
                Ownership ownership);

    virtual ~ListTableResponseImpl();

    virtual int32_t GetTableNameSize() const;

    virtual const std::string& GetTableName(int32_t index) const;

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::ListTableResponse* mPBMessage;
};


class GetRowRequestImpl : public GetRowRequest, public OTSAbstractImpl
{
public:
    GetRowRequestImpl(
                com::aliyun::cloudservice::ots2::GetRowRequest* message,
                Ownership ownership);

    virtual ~GetRowRequestImpl();

    virtual void SetTableName(const std::string& tableName);

    /**
     * 添加一个主键列，添加完成后Column内部数据的所有权也同时转移。
     */
    virtual void AddPrimaryKey(Column* column);

    virtual void AddColumnName(const std::string& name);

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::GetRowRequest* mPBMessage;
};


class GetRowResponseImpl : public GetRowResponse, public OTSAbstractImpl
{
public:
    GetRowResponseImpl(
                com::aliyun::cloudservice::ots2::GetRowResponse* message,
                Ownership ownership);

    virtual ~GetRowResponseImpl();

    virtual const Row& GetRow() const;

    virtual const CapacityUnit& GetConsumedCapacity() const;

    void ParseFromPBMessage();

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::GetRowResponse* mPBMessage;
    Row* mRow;
    CapacityUnit* mConsumedCapacity;
};


class ConditionImpl : public Condition, public OTSAbstractImpl
{
public:
    ConditionImpl(com::aliyun::cloudservice::ots2::Condition* message, Ownership ownership);

    virtual ~ConditionImpl();

    virtual void SetRowExistenceExpectation(RowExistenceExpectation expect);

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::Condition* mPBMessage;
};


class PutRowRequestImpl : public PutRowRequest, public OTSAbstractImpl
{
public:
    PutRowRequestImpl(com::aliyun::cloudservice::ots2::PutRowRequest* message, Ownership ownership);

    virtual ~PutRowRequestImpl();

    virtual void SetTableName(const std::string& tableName);

    virtual void SetCondition(Condition* condition);

    virtual void AddPrimaryKey(Column* column);

    virtual void AddColumn(Column* column);

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::PutRowRequest* mPBMessage;
};


class PutRowResponseImpl : public PutRowResponse, public OTSAbstractImpl
{
public:
    PutRowResponseImpl(
                com::aliyun::cloudservice::ots2::PutRowResponse* message,
                Ownership ownership);

    virtual ~PutRowResponseImpl();

    virtual const CapacityUnit& GetConsumedCapacity() const;

    void ParseFromPBMessage();

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::PutRowResponse* mPBMessage;
    CapacityUnit* mConsumedCapacity;
};


class UpdateRowRequestImpl : public UpdateRowRequest, public OTSAbstractImpl
{
public:
    UpdateRowRequestImpl(
                com::aliyun::cloudservice::ots2::UpdateRowRequest* message,
                Ownership ownership);

    virtual ~UpdateRowRequestImpl();

    virtual void SetTableName(const std::string& tableName);

    virtual void SetCondition(Condition* condition);

    virtual void AddPrimaryKey(Column* column);

    virtual void AddColumn(ColumnUpdate* column);

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::UpdateRowRequest* mPBMessage;
};


class UpdateRowResponseImpl : public UpdateRowResponse, public OTSAbstractImpl
{
public:
    UpdateRowResponseImpl(
                com::aliyun::cloudservice::ots2::UpdateRowResponse* message,
                Ownership ownership);

    virtual ~UpdateRowResponseImpl();

    virtual const CapacityUnit& GetConsumedCapacity() const;

    void ParseFromPBMessage();

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::UpdateRowResponse* mPBMessage;
    CapacityUnit* mConsumedCapacity;
};


class DeleteRowRequestImpl : public DeleteRowRequest, public OTSAbstractImpl
{
public:
    DeleteRowRequestImpl(
                com::aliyun::cloudservice::ots2::DeleteRowRequest* message,
                Ownership ownership);

    virtual ~DeleteRowRequestImpl();

    virtual void SetTableName(const std::string& tableName);

    virtual void SetCondition(Condition* condition);

    virtual void AddPrimaryKey(Column* column);

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::DeleteRowRequest* mPBMessage;
};


class DeleteRowResponseImpl : public DeleteRowResponse, public OTSAbstractImpl
{
public:
    DeleteRowResponseImpl(
                com::aliyun::cloudservice::ots2::DeleteRowResponse* message,
                Ownership ownership);

    virtual ~DeleteRowResponseImpl();

    virtual const CapacityUnit& GetConsumedCapacity() const;

    void ParseFromPBMessage();

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::DeleteRowResponse* mPBMessage;
    CapacityUnit* mConsumedCapacity;
};


class BatchGetRequestRowItemImpl : public BatchGetRequestRowItem, public OTSAbstractImpl
{
public:
    BatchGetRequestRowItemImpl(
                com::aliyun::cloudservice::ots2::RowInBatchGetRowRequest* message,
                Ownership ownership);

    virtual ~BatchGetRequestRowItemImpl();

    virtual void AddPrimaryKey(Column* column);

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::RowInBatchGetRowRequest* mPBMessage;
};


class BatchGetRequestTableItemImpl : public BatchGetRequestTableItem, public OTSAbstractImpl
{
public:
    BatchGetRequestTableItemImpl(
                com::aliyun::cloudservice::ots2::TableInBatchGetRowRequest* message,
                Ownership ownership);

    virtual ~BatchGetRequestTableItemImpl();

    virtual void SetTableName(const std::string& tableName);

    virtual void AddRowItem(BatchGetRequestRowItem* rowItem);

    virtual void AddColumnName(const std::string& name);

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::TableInBatchGetRowRequest* mPBMessage;
};


class BatchGetRowRequestImpl : public BatchGetRowRequest, public OTSAbstractImpl
{
public:
    BatchGetRowRequestImpl(
                com::aliyun::cloudservice::ots2::BatchGetRowRequest* message,
                Ownership ownership);

    virtual ~BatchGetRowRequestImpl();

    virtual void AddTableItem(BatchGetRequestTableItem* tableItem);

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::BatchGetRowRequest* mPBMessage;
};


class BatchGetResponseRowItemImpl : public BatchGetResponseRowItem, public OTSAbstractImpl
{
public:
    BatchGetResponseRowItemImpl(
                com::aliyun::cloudservice::ots2::RowInBatchGetRowResponse* message,
                Ownership ownership);

    virtual ~BatchGetResponseRowItemImpl();

    virtual bool IsOK() const;

    virtual const Error& GetError() const;

    virtual const CapacityUnit& GetConsumedCapacity() const;

    virtual const Row& GetRow() const;

    void ParseFromPBMessage();

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::RowInBatchGetRowResponse* mPBMessage;
    Error* mError;
    CapacityUnit* mConsumedCapacity;
    Row* mRow;
};


class BatchGetResponseTableItemImpl : public BatchGetResponseTableItem, public OTSAbstractImpl
{
public:
    BatchGetResponseTableItemImpl(
                com::aliyun::cloudservice::ots2::TableInBatchGetRowResponse* message,
                Ownership ownership);

    virtual ~BatchGetResponseTableItemImpl();

    virtual const std::string& GetTableName() const;

    virtual int32_t GetRowSize() const;

    virtual const BatchGetResponseRowItem& GetRowItem(int32_t index) const;

    void ParseFromPBMessage();

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::TableInBatchGetRowResponse* mPBMessage;
    std::vector<BatchGetResponseRowItem*> mRowItems;
};


class BatchGetRowResponseImpl : public BatchGetRowResponse, public OTSAbstractImpl
{
public:
    BatchGetRowResponseImpl(
                com::aliyun::cloudservice::ots2::BatchGetRowResponse* message,
                Ownership ownership);

    virtual ~BatchGetRowResponseImpl();

    virtual int32_t GetTableItemSize() const;

    virtual const BatchGetResponseTableItem& GetTableItem(int32_t index) const;

    void ParseFromPBMessage();

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::BatchGetRowResponse* mPBMessage;
    std::vector<BatchGetResponseTableItem*> mTableItems;
};


class BatchWriteRequestPutRowItemImpl : public BatchWriteRequestPutRowItem, public OTSAbstractImpl
{
public:
    BatchWriteRequestPutRowItemImpl(
                com::aliyun::cloudservice::ots2::PutRowInBatchWriteRowRequest* message,
                Ownership ownership);

    virtual ~BatchWriteRequestPutRowItemImpl();

    virtual void SetCondition(Condition* condition);

    virtual void AddPrimaryKey(Column* column);

    virtual void AddColumn(Column* column);

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::PutRowInBatchWriteRowRequest* mPBMessage;
};


class BatchWriteRequestUpdateRowItemImpl : public BatchWriteRequestUpdateRowItem, public OTSAbstractImpl
{
public:
    BatchWriteRequestUpdateRowItemImpl(
                com::aliyun::cloudservice::ots2::UpdateRowInBatchWriteRowRequest* message,
                Ownership ownership);

    virtual ~BatchWriteRequestUpdateRowItemImpl();

    virtual void SetCondition(Condition* condition);

    virtual void AddPrimaryKey(Column* column);

    virtual void AddColumn(ColumnUpdate* column);

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::UpdateRowInBatchWriteRowRequest* mPBMessage;
};


class BatchWriteRequestDeleteRowItemImpl : public BatchWriteRequestDeleteRowItem, public OTSAbstractImpl
{
public:
    BatchWriteRequestDeleteRowItemImpl(
                com::aliyun::cloudservice::ots2::DeleteRowInBatchWriteRowRequest* message,
                Ownership ownership);

    virtual ~BatchWriteRequestDeleteRowItemImpl();

    virtual void SetCondition(Condition* condition);

    virtual void AddPrimaryKey(Column* column);

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::DeleteRowInBatchWriteRowRequest* mPBMessage;
};


class BatchWriteRequestTableItemImpl : public BatchWriteRequestTableItem, public OTSAbstractImpl
{
public:
    BatchWriteRequestTableItemImpl(
                com::aliyun::cloudservice::ots2::TableInBatchWriteRowRequest* message,
                Ownership ownership);

    virtual ~BatchWriteRequestTableItemImpl();

    virtual void SetTableName(const std::string& tableName);

    virtual void AddPutRowItem(BatchWriteRequestPutRowItem* rowItem);

    virtual void AddUpdateRowItem(BatchWriteRequestUpdateRowItem* rowItem);

    virtual void AddDeleteRowItem(BatchWriteRequestDeleteRowItem* rowItem);

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::TableInBatchWriteRowRequest* mPBMessage;
};


class BatchWriteRowRequestImpl : public BatchWriteRowRequest, public OTSAbstractImpl
{
public:
    BatchWriteRowRequestImpl(
                com::aliyun::cloudservice::ots2::BatchWriteRowRequest* message,
                Ownership ownership);

    virtual ~BatchWriteRowRequestImpl();

    virtual void AddTableItem(BatchWriteRequestTableItem* tableItem);

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::BatchWriteRowRequest* mPBMessage;
};


class BatchWriteResponseRowItemImpl : public BatchWriteResponseRowItem, public OTSAbstractImpl
{
public:
    BatchWriteResponseRowItemImpl(
                com::aliyun::cloudservice::ots2::RowInBatchWriteRowResponse* message,
                Ownership ownership);

    virtual ~BatchWriteResponseRowItemImpl();

    virtual bool IsOK() const;

    virtual const Error& GetError() const;

    virtual const CapacityUnit& GetConsumedCapacity() const;

    void ParseFromPBMessage();

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::RowInBatchWriteRowResponse* mPBMessage;
    Error* mError;
    CapacityUnit* mConsumedCapacity;
};


class BatchWriteResponseTableItemImpl : public BatchWriteResponseTableItem, public OTSAbstractImpl
{
public:
    BatchWriteResponseTableItemImpl(
                com::aliyun::cloudservice::ots2::TableInBatchWriteRowResponse* message,
                Ownership ownership);

    virtual ~BatchWriteResponseTableItemImpl();

    virtual const std::string& GetTableName() const;

    virtual int32_t GetPutRowItemSize() const;

    virtual const BatchWriteResponseRowItem& GetPutRowItem(int32_t index) const;

    virtual int32_t GetUpdateRowItemSize() const;

    virtual const BatchWriteResponseRowItem& GetUpdateRowItem(int32_t index) const;

    virtual int32_t GetDeleteRowItemSize() const;

    virtual const BatchWriteResponseRowItem& GetDeleteRowItem(int32_t index) const;

    void ParseFromPBMessage();

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::TableInBatchWriteRowResponse* mPBMessage;
    std::vector<BatchWriteResponseRowItem*> mPutRowItems;
    std::vector<BatchWriteResponseRowItem*> mUpdateRowItems;
    std::vector<BatchWriteResponseRowItem*> mDeleteRowItems;
};


class BatchWriteRowResponseImpl : public BatchWriteRowResponse, public OTSAbstractImpl
{
public:
    BatchWriteRowResponseImpl(
                com::aliyun::cloudservice::ots2::BatchWriteRowResponse* message,
                Ownership ownership);

    virtual ~BatchWriteRowResponseImpl();

    virtual int32_t GetTableItemSize() const;

    virtual const BatchWriteResponseTableItem& GetTableItem(int32_t index) const;

    void ParseFromPBMessage();

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::BatchWriteRowResponse* mPBMessage;
    std::vector<BatchWriteResponseTableItem*> mTableItems;
};


class GetRangeRequestImpl : public GetRangeRequest, public OTSAbstractImpl
{
public:
    GetRangeRequestImpl(
                com::aliyun::cloudservice::ots2::GetRangeRequest* message,
                Ownership ownership);

    virtual ~GetRangeRequestImpl();

    virtual void SetTableName(const std::string& tableName);

    virtual void SetDirection(RangeDirection direction);

    virtual void SetLimit(int32_t limit);

    virtual void AddStartPrimaryKey(Column* column);

    virtual void AddEndPrimaryKey(Column* column);

    virtual void AddColumnName(const std::string& name);

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::GetRangeRequest* mPBMessage;
};


class GetRangeResponseImpl : public GetRangeResponse, public OTSAbstractImpl
{
public:
    GetRangeResponseImpl(
                com::aliyun::cloudservice::ots2::GetRangeResponse* message,
                Ownership ownership);

    virtual ~GetRangeResponseImpl();

    virtual const CapacityUnit& GetConsumedCapacity() const;

    virtual int32_t GetNextStartPrimaryKeySize() const;

    virtual const Column& GetNextStartPrimaryKey(int32_t index) const;

    virtual int32_t GetRowSize() const;

    virtual const Row& GetRow(int32_t index) const;

    void ParseFromPBMessage();

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::GetRangeResponse* mPBMessage;
    CapacityUnit* mConsumedCapacity;
    std::vector<Column*> mNextStartPrimaryKeys;
    std::vector<Row*> mRows;
};


class RowIteratorImpl : public RowIterator, public OTSAbstractImpl
{
public:
    RowIteratorImpl(
                com::aliyun::cloudservice::ots2::GetRangeRequest* pbRequest,
                com::aliyun::cloudservice::ots2::GetRangeResponse* pbResponse,
                Ownership ownership);

    virtual ~RowIteratorImpl();

    virtual bool HasNext() const;

    virtual const Row& Next();

    void InitForFirstTime(
                OTSClientImpl* clientImpl,
                com::aliyun::cloudservice::ots2::GetRangeRequest* pbRequest,
                int64_t* consumed_read_capacity,
                int32_t row_count);

    virtual google::protobuf::Message* GetPBMessage() const;

private:
    Ownership mOwnership;
    com::aliyun::cloudservice::ots2::GetRangeRequest* mPBRequest;
    com::aliyun::cloudservice::ots2::GetRangeResponse* mPBResponse;
    OTSClientImpl* mClientImpl;
    int64_t* mConsumedReadCapacity;
    int32_t mRowCount;

    int32_t mRowIndex;
    std::vector<Row*> mRows;
};

} //end of ots
} //end of openservices
} //end of aliyun

#endif

