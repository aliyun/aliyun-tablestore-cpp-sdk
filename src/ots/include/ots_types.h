#ifndef OTS_TYPES_H
#define OTS_TYPES_H

#include <string>
#include <stdint.h>

namespace aliyun {
namespace openservices {
namespace ots {

extern const int32_t kCapacityNone;

/**
 * 所有OTS支持的数据类型。
 */
enum ColumnType
{
    /* 无限最小值。 */
    INF_MIN = 0,
    /* 无限最大值。 */
    INF_MAX = 1,
    /* 整数类型。 */
    INTEGER = 2,
    /* 字符串类型。*/
    STRING = 3,
    /* 布尔类型。 */
    BOOLEAN = 4,
    /* 双精度浮点数类型。 */     
    DOUBLE = 5,
    /* 二进制数据类型。 */
    BINARY = 6
};

/**
 * Row存在性检查条件，包括3种情况。
 */
enum RowExistenceExpectation
{
    /* 忽略检查。 */
    IGNORE = 0,
    /* 期望Row已存在。 */
    EXPECT_EXIST = 1,
    /* 期望Row不存在。 */
    EXPECT_NOT_EXIST = 2
};

/**
 * GetRange范围的方向。
 */
enum RangeDirection
{
    /* 范围为正向。 */
    FORWARD = 0,
    /* 范围为反向。 */
    BACKWARD = 1
};

enum OperationType
{
    PUT = 1,
    DELETE = 2
};

class Uncopyable
{
protected:
    Uncopyable() {}
    virtual ~Uncopyable() {}

private:
    Uncopyable(const Uncopyable& o);
    Uncopyable& operator=(const Uncopyable& o);
};


class Error : private Uncopyable
{
public:
    virtual ~Error() {}

    virtual const std::string& GetCode() const = 0;

    virtual const std::string& GetMessage() const = 0;
};

class CapacityUnit : private Uncopyable
{
public:
    virtual ~CapacityUnit() {}

    virtual int32_t GetRead() const = 0;

    virtual int32_t GetWrite() const = 0;
};

class ReservedThroughputDetails : private Uncopyable
{
public:
    virtual ~ReservedThroughputDetails() {}

    virtual const CapacityUnit& GetCapacityUnit() const = 0;

    virtual int64_t GetLastIncreaseTime() const = 0;

    virtual int64_t GetLastDecreaseTime() const = 0;

    virtual int32_t GetNumberOfDecreasesToday() const = 0;
};

class ReservedThroughput : private Uncopyable
{
public:
    virtual ~ReservedThroughput() {}

    virtual void SetCapacityUnit(CapacityUnit* capacity) const = 0;
};

class OTSValue : private Uncopyable
{
public:
    virtual ~OTSValue() {}

    virtual ColumnType GetType() const = 0;
};


class OTSInfMinValue : public OTSValue
{
public:
    virtual ~OTSInfMinValue() {}

    virtual ColumnType GetType() const = 0;
};


class OTSInfMaxValue : public OTSValue
{
public:
    virtual ~OTSInfMaxValue() {}

    virtual ColumnType GetType() const = 0;
};


class OTSStringValue : public OTSValue
{
public:
    virtual ~OTSStringValue() {}

    virtual ColumnType GetType() const = 0;

    virtual const std::string& GetValue() const = 0;
};


class OTSIntValue : public OTSValue
{
public:
    virtual ~OTSIntValue() {}

    virtual ColumnType GetType() const = 0;

    virtual int64_t GetValue() const = 0;
};


class OTSBoolValue : public OTSValue
{
public:
    virtual ~OTSBoolValue() {}

    virtual ColumnType GetType() const = 0;

    virtual bool GetValue() const = 0;
};


class OTSDoubleValue : public OTSValue
{
public:
    virtual ~OTSDoubleValue() {}

    virtual ColumnType GetType() const = 0;

    virtual double GetValue() const = 0;
};


class OTSBinaryValue : public OTSValue
{
public:
    virtual ~OTSBinaryValue() {}

    virtual ColumnType GetType() const = 0;

    virtual void GetValue(char** data, int32_t* size) const = 0;
};


class ColumnSchema : private Uncopyable
{
public:
    virtual ~ColumnSchema() {}

    virtual const std::string& GetColumnName() const = 0;

    virtual ColumnType GetColumnType() const = 0;
};


class Column : private Uncopyable
{
public:
    virtual ~Column() {}

    virtual const std::string& GetColumnName() const = 0;

    virtual const OTSValue& GetColumnValue() const = 0;
};

class ColumnUpdate : private Uncopyable
{
public:
    virtual ~ColumnUpdate() {};
};


class Row : private Uncopyable
{
public:
    virtual ~Row() {}

    virtual int32_t GetPrimaryKeySize() const = 0;

    virtual const Column& GetPrimaryKey(int32_t index) const = 0;

    virtual int32_t GetColumnSize() const = 0;

    virtual const Column& GetColumn(int32_t index) const = 0;
};


class TableMeta : private Uncopyable
{
public:
    virtual ~TableMeta() {}

    virtual void SetTableName(const std::string& tableName) = 0;

    virtual void AddPrimaryKeySchema(ColumnSchema* schema) = 0;

    virtual const std::string& GetTableName() const = 0;

    virtual int32_t GetPrimaryKeySchemaSize() const = 0;

    virtual const ColumnSchema& GetPrimaryKeySchema(int32_t index) const = 0;
};


class CreateTableRequest : private Uncopyable
{
public:
    virtual ~CreateTableRequest() {}

    virtual void SetTableMeta(TableMeta* tableMeta) = 0;

    virtual void SetReservedThroughput(ReservedThroughput* throughput) = 0;
};


class UpdateTableRequest : private Uncopyable
{
public:
    virtual ~UpdateTableRequest() {}

    virtual void SetTableName(const std::string& tableName) = 0;

    virtual void SetReservedThroughput(ReservedThroughput* throughput) = 0;
};


class UpdateTableResponse : private Uncopyable
{
public:
    virtual ~UpdateTableResponse() {}

    virtual const ReservedThroughputDetails& GetReservedThroughputDetails() const = 0;

};


class DescribeTableResponse : private Uncopyable
{
public:
    virtual ~DescribeTableResponse() {}

    virtual const TableMeta& GetTableMeta() const = 0;

    virtual const ReservedThroughputDetails& GetReservedThroughputDetails() const = 0;
};


class ListTableResponse : private Uncopyable
{
public:
    virtual ~ListTableResponse() {}

    virtual int32_t GetTableNameSize() const = 0;

    virtual const std::string& GetTableName(int32_t index) const = 0;
};


class GetRowRequest : private Uncopyable
{
public:
    virtual ~GetRowRequest() {}

    virtual void SetTableName(const std::string& tableName) = 0;

    /**
     * 添加一个主键列，添加完成后Column内部数据的所有权也同时转移。
     */
    virtual void AddPrimaryKey(Column* column) = 0;

    virtual void AddColumnName(const std::string& name) = 0;
};


class GetRowResponse : private Uncopyable
{
public:
    virtual ~GetRowResponse() {}

    virtual const CapacityUnit& GetConsumedCapacity() const = 0;

    virtual const Row& GetRow() const = 0;
};


class Condition : private Uncopyable
{
public:
    virtual ~Condition() {}

    virtual void SetRowExistenceExpectation(RowExistenceExpectation expect) = 0;
};


class PutRowRequest : private Uncopyable
{
public:
    virtual ~PutRowRequest() {}

    virtual void SetTableName(const std::string& tableName) = 0;

    virtual void SetCondition(Condition* condition) = 0;

    virtual void AddPrimaryKey(Column* column) = 0;

    virtual void AddColumn(Column* column) = 0;
};


class PutRowResponse : private Uncopyable
{
public:
    virtual ~PutRowResponse() {}

    virtual const CapacityUnit& GetConsumedCapacity() const = 0;
};


class UpdateRowRequest : private Uncopyable
{
public:
    virtual ~UpdateRowRequest() {}

    virtual void SetTableName(const std::string& tableName) = 0;

    virtual void SetCondition(Condition* condition) = 0;

    virtual void AddPrimaryKey(Column* column) = 0;

    virtual void AddColumn(ColumnUpdate* column) = 0;
};


class UpdateRowResponse : private Uncopyable
{
public:
    virtual ~UpdateRowResponse() {}

    virtual const CapacityUnit& GetConsumedCapacity() const = 0;
};


class DeleteRowRequest : private Uncopyable
{
public:
    virtual ~DeleteRowRequest() {}

    virtual void SetTableName(const std::string& tableName) = 0;

    virtual void SetCondition(Condition* condition) = 0;

    virtual void AddPrimaryKey(Column* column) = 0;
};


class DeleteRowResponse : private Uncopyable
{
public:
    virtual ~DeleteRowResponse() {}

    virtual const CapacityUnit& GetConsumedCapacity() const = 0;
};


class BatchGetRequestRowItem : private Uncopyable
{
public:
    virtual ~BatchGetRequestRowItem() {}

    virtual void AddPrimaryKey(Column* column) = 0;
};


class BatchGetRequestTableItem : private Uncopyable
{
public:
    virtual ~BatchGetRequestTableItem() {}

    virtual void SetTableName(const std::string& tableName) = 0;

    virtual void AddRowItem(BatchGetRequestRowItem* rowItem) = 0;

    virtual void AddColumnName(const std::string& name) = 0;
};


class BatchGetRowRequest : private Uncopyable
{
public:
    virtual ~BatchGetRowRequest() {}

    virtual void AddTableItem(BatchGetRequestTableItem* tableItem) = 0;
};


class BatchGetResponseRowItem : private Uncopyable
{
public:
    virtual ~BatchGetResponseRowItem() {}

    virtual bool IsOK() const = 0;

    virtual const Error& GetError() const = 0;

    virtual const CapacityUnit& GetConsumedCapacity() const = 0;

    virtual const Row& GetRow() const = 0;
};


class BatchGetResponseTableItem : private Uncopyable
{
public:
    virtual ~BatchGetResponseTableItem() {}

    virtual const std::string& GetTableName() const = 0;

    virtual int32_t GetRowSize() const = 0;

    virtual const BatchGetResponseRowItem& GetRowItem(int32_t index) const = 0;
};


class BatchGetRowResponse : private Uncopyable
{
public:
    virtual ~BatchGetRowResponse() {}

    virtual int32_t GetTableItemSize() const = 0;

    virtual const BatchGetResponseTableItem& GetTableItem(int32_t index) const = 0;
};


class BatchWriteRequestPutRowItem : private Uncopyable
{
public:
    virtual ~BatchWriteRequestPutRowItem() {}

    virtual void SetCondition(Condition* condition) = 0;

    virtual void AddPrimaryKey(Column* column) = 0;

    virtual void AddColumn(Column* column) = 0;
};


class BatchWriteRequestUpdateRowItem : private Uncopyable
{
public:
    virtual ~BatchWriteRequestUpdateRowItem() {}

    virtual void SetCondition(Condition* condition) = 0;

    virtual void AddPrimaryKey(Column* column) = 0;

    virtual void AddColumn(ColumnUpdate* column) = 0;
};


class BatchWriteRequestDeleteRowItem : private Uncopyable
{
public:
    virtual ~BatchWriteRequestDeleteRowItem() {}

    virtual void SetCondition(Condition* condition) = 0;

    virtual void AddPrimaryKey(Column* column) = 0;
};


class BatchWriteRequestTableItem : private Uncopyable
{
public:
    virtual ~BatchWriteRequestTableItem() {}

    virtual void SetTableName(const std::string& tableName) = 0;

    virtual void AddPutRowItem(BatchWriteRequestPutRowItem* rowItem) = 0;

    virtual void AddUpdateRowItem(BatchWriteRequestUpdateRowItem* rowItem) = 0;

    virtual void AddDeleteRowItem(BatchWriteRequestDeleteRowItem* rowItem) = 0;
};


class BatchWriteRowRequest : private Uncopyable
{
public:
    virtual ~BatchWriteRowRequest() {}

    virtual void AddTableItem(BatchWriteRequestTableItem* tableItem) = 0;
};


class BatchWriteResponseRowItem : private Uncopyable
{
public:
    virtual ~BatchWriteResponseRowItem() {}

    virtual bool IsOK() const = 0;

    virtual const Error& GetError() const = 0;

    virtual const CapacityUnit& GetConsumedCapacity() const = 0;
};


class BatchWriteResponseTableItem : private Uncopyable
{
public:
    virtual ~BatchWriteResponseTableItem() {}

    virtual const std::string& GetTableName() const = 0;

    virtual int32_t GetPutRowItemSize() const = 0;

    virtual const BatchWriteResponseRowItem& GetPutRowItem(int32_t index) const = 0;

    virtual int32_t GetUpdateRowItemSize() const = 0;

    virtual const BatchWriteResponseRowItem& GetUpdateRowItem(int32_t index) const = 0;

    virtual int32_t GetDeleteRowItemSize() const = 0;

    virtual const BatchWriteResponseRowItem& GetDeleteRowItem(int32_t index) const = 0;
};


class BatchWriteRowResponse : private Uncopyable
{
public:
    virtual ~BatchWriteRowResponse() {}

    virtual int32_t GetTableItemSize() const = 0;

    virtual const BatchWriteResponseTableItem& GetTableItem(int32_t index) const = 0;
};


class GetRangeRequest : private Uncopyable
{
public:
    virtual ~GetRangeRequest() {}

    virtual void SetTableName(const std::string& tableName) = 0;

    virtual void SetDirection(RangeDirection direction) = 0;

    virtual void SetLimit(int32_t limit) = 0;

    virtual void AddStartPrimaryKey(Column* column) = 0;

    virtual void AddEndPrimaryKey(Column* column) = 0;

    virtual void AddColumnName(const std::string& name) = 0;
};


class GetRangeResponse : private Uncopyable
{
public:
    virtual ~GetRangeResponse() {}

    virtual const CapacityUnit& GetConsumedCapacity() const = 0;

    virtual int32_t GetNextStartPrimaryKeySize() const = 0;

    virtual const Column& GetNextStartPrimaryKey(int32_t index) const = 0;

    virtual int32_t GetRowSize() const = 0;

    virtual const Row& GetRow(int32_t index) const = 0;
};

class RowIterator : private Uncopyable
{
public:
    virtual ~RowIterator() {}

    virtual bool HasNext() const = 0;

    virtual const Row& Next() = 0;
};

} //end of ots
} //end of openservices
} //end of aliyun

#endif
