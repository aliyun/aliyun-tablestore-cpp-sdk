/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#ifndef OTS_REQUEST_H
#define OTS_REQUEST_H

#include "ots_common.h"
#include "ots_condition.h"
#include "ots_types.h"
#include <stdint.h>
#include <list>
#include <map>
#include <string>
#include <tr1/memory>
#include <deque>

namespace aliyun {
namespace tablestore {

/**
 * @brief OTSRequest
 *
 * 请求参数。
 */
class OTSRequest
{
public:
    
    OTSRequest();

    virtual ~OTSRequest();
};

/**
 * @brief OTSResult 
 *
 * 请求结果。
 */
class OTSResult
{
public:

    OTSResult();

    OTSResult(
        const std::string& requestId,
        const std::string& traceId);

    virtual ~OTSResult();

    virtual const std::string& GetRequestId() const;

    virtual void SetRequestId(const std::string& requestId);

    virtual const std::string& GetTraceId() const;

    virtual void SetTraceId(const std::string& traceId);

private:

    std::string mRequestId;

    std::string mTraceId;
};

/**
 * @brief CreateTableRequest
 *
 * 建表参数。
 */
class CreateTableRequest : public OTSRequest
{
public:

    CreateTableRequest(
        const TableMeta& tableMeta,
        const ReservedThroughput& reservedThroughput,
        const TableOptions& tableOptions);

    CreateTableRequest(
        const TableMeta& tableMeta,
        const ReservedThroughput& reservedThroughput,
        const TableOptions& tableOptions,
        const std::list<PartitionRange>& partitionRanges);

    virtual ~CreateTableRequest();

    const TableMeta& GetTableMeta() const;

    void SetTableMeta(const TableMeta& tableMeta);

    const ReservedThroughput& GetReservedThroughput() const;

    void SetReservedThroughput(const ReservedThroughput& reservedThroughput);
    
    const TableOptions& GetTableOptions() const;

    void SetTableOptions(const TableOptions& tableOptions);

    bool HasPartitionRanges() const;

    const std::list<PartitionRange>& GetPartitionRanges() const;

    void SetPartitionRanges(const std::list<PartitionRange>& partitionRanges);

private:

    TableMeta mTableMeta;

    ReservedThroughput mReservedThroughput;

    TableOptions mTableOptions;

    OptionalValue<std::list<PartitionRange> > mPartitionRanges;
};

/**
 * @brief CreateTableResult
 *
 * 建表结果。
 */
class CreateTableResult : public OTSResult
{
public:

    CreateTableResult();

    CreateTableResult(const OTSResult& result);

    virtual ~CreateTableResult();
};

/**
 * @brief ListTableRequest
 *
 * 列表参数。
 */
class ListTableRequest : public OTSRequest
{
public:
    
    ListTableRequest();

    virtual ~ListTableRequest();
};

/**
 * @brief ListTableResult
 *
 * 列表结果。
 */
class ListTableResult : public OTSResult
{
public:

    ListTableResult();

    ListTableResult(const OTSResult& result);

    virtual ~ListTableResult();

    const std::list<std::string>& GetTableNames();

    void AddTableName(const std::string& tableName);

private:

    std::list<std::string> mTableNames;
};

/**
 * @brief DescribeTableRequest
 *
 * 查表参数。
 */
class DescribeTableRequest : public OTSRequest
{
public:

    DescribeTableRequest(const std::string& tableName);

    virtual ~DescribeTableRequest();

    const std::string& GetTableName() const;

    void SetTableName(const std::string& tableName);

private:

    std::string mTableName;
};

/**
 * @brief DescribeTableResult
 *
 * 查表结果。
 */
class DescribeTableResult : public OTSResult
{
public:

    DescribeTableResult();

    DescribeTableResult(const OTSResult& result);

    const TableMeta& GetTableMeta() const;

    void SetTableMeta(const TableMeta& tableMeta);

    const ReservedThroughputDetails& GetReservedThroughputDetails() const;

    void SetReservedThroughputDetails(
        const ReservedThroughputDetails& reservedThroughputDetails);

    const TableOptions& GetTableOptions() const;

    void SetTableOptions(const TableOptions& tableOptions);

    const TableStatus& GetTableStatus() const;

    void SetTableStatus(const TableStatus& tableStatus);

    /**
     * 所有shard之间的分裂点。
     * 假设某表有两列pk，三个shard。则返回两个一列的分裂点，分别为A和B。
     * 三个shard的range分别为(-inf,-inf)->(A,-inf), (A,-inf)->(B,-inf), (B,-inf)->(+inf,+inf)
     */
    const std::deque<PrimaryKey>& GetShardSplits() const;

    void SetShardSplits(const std::deque<PrimaryKey>&);

private:

    TableMeta mTableMeta;

    ReservedThroughputDetails mReservedThroughputDetails;

    TableOptions mTableOptions;

    TableStatus mTableStatus;

    std::deque<PrimaryKey> mShardSplits;
};

/**
 * @brief DeleteTableRequest
 *
 * 查表参数。
 */
class DeleteTableRequest : public OTSRequest
{
public:

    DeleteTableRequest(const std::string& tableName);

    virtual ~DeleteTableRequest();

    const std::string& GetTableName() const;

    void SetTableName(const std::string& tableName);

private:

    std::string mTableName;
};

/**
 * @brief DeleteTableResult
 *
 * 建表结果。
 */
class DeleteTableResult : public OTSResult
{
public:

    DeleteTableResult();

    DeleteTableResult(const OTSResult& result);

    virtual ~DeleteTableResult();
};

/**
 * @brief UpdateTableRequest
 *
 * 改表参数。
 */
class UpdateTableRequest : public OTSRequest
{
public:

    UpdateTableRequest(const std::string& tableName);

    virtual ~UpdateTableRequest();

    const std::string& GetTableName() const;

    void SetTableName(const std::string& tableName);

    bool HasReservedThroughputForUpdate() const;

    const ReservedThroughput& GetReservedThroughputForUpdate() const;

    void SetReservedThroughputForUpdate(
        const ReservedThroughput& reservedThroughputForUpdate);

    bool HasTableOptionsForUpdate() const;

    const TableOptions& GetTableOptionsForUpdate() const;

    void SetTableOptionsForUpdate(const TableOptions& tableOptionsForUpdate);

private:

    std::string mTableName;

    OptionalValue<ReservedThroughput> mReservedThroughputForUpdate;

    OptionalValue<TableOptions> mTableOptionsForUpdate;
};

/**
 * @brief UpdateTableResult
 *
 * 改表结果。
 */
class UpdateTableResult : public OTSResult
{
public:

    UpdateTableResult();

    UpdateTableResult(const OTSResult& result);

    virtual ~UpdateTableResult();
};

/**
 * @brief RowQueryCriteria
 *
 * 查询行的参数。
 */
class RowQueryCriteria
{
public:

    RowQueryCriteria(const std::string& tableName);

    virtual ~RowQueryCriteria();

    virtual const std::string& GetTableName() const;

    virtual void SetTableName(const std::string& tableName);

    virtual const std::list<std::string>& GetColumnsToGet() const;

    virtual void SetColumnsToGet(const std::list<std::string>& columnsToGet);

    virtual void AddColumnName(const std::string& columnName);

    virtual void SetMaxVersions(int maxVersions);

    virtual int GetMaxVersions() const;

    virtual bool HasMaxVersions() const;

    virtual void SetTimeRange(const TimeRange& timeRange);

    virtual void SetTimestamp(int64_t ts);

    virtual const TimeRange& GetTimeRange() const;

    virtual bool HasTimeRange() const;

    virtual void SetCacheBlocks(bool cacheBlocks);

    virtual bool GetCacheBlocks() const;

    virtual bool HasCacheBlocks() const;

    virtual void SetFilter(const ColumnConditionPtr& filter);

    virtual const ColumnConditionPtr& GetFilter() const;

    virtual bool HasFilter() const;

private:

    std::string mTableName;

    std::list<std::string> mColumnsToGet;

    OptionalValue<TimeRange> mTimeRange;

    OptionalValue<int> mMaxVersions;

    OptionalValue<bool> mCacheBlocks;

    OptionalValue<ColumnConditionPtr> mFilter;
};

/**
 * @brief SingleRowQueryCriteria
 *
 * 查询一行的条件。
 */
class SingleRowQueryCriteria : public RowQueryCriteria
{
public:

    SingleRowQueryCriteria(const std::string& tableName);

    SingleRowQueryCriteria(
        const std::string& tableName,
        const PrimaryKey& primaryKey);

    virtual ~SingleRowQueryCriteria();

    const PrimaryKey& GetPrimaryKey() const;

    void SetPrimaryKey(const PrimaryKey& primaryKey);

    void AddPrimaryKeyColumn(
        const PrimaryKeyColumn& primaryKeyColumn);

    void AddPrimaryKeyColumn(
        const std::string& name,
        const PrimaryKeyValue& value);

private:

    PrimaryKey mPrimaryKey;
};

/**
 * @brief GetRowRequest
 *
 * GetRow参数。
 */
class GetRowRequest : public OTSRequest
{
public:
    
    GetRowRequest(const SingleRowQueryCriteria& rowQueryCriteria);

    virtual ~GetRowRequest();

    const SingleRowQueryCriteria& GetRowQueryCriteria() const;

    void SetRowQueryCriteria(const SingleRowQueryCriteria& rowQueryCriteria);

private:

    SingleRowQueryCriteria mRowQueryCriteria;
};

/**
 * @brief GetRowResult
 *
 * GetRow返回值。
 */
class GetRowResult : public OTSResult
{
public:

    GetRowResult();

    GetRowResult(
        const OTSResult& result,
        const RowPtr& row,
        const ConsumedCapacity& consumedCapacity); 

    virtual ~GetRowResult();

    const RowPtr& GetRow() const;

    void SetRow(const RowPtr& rowPtr);

    const ConsumedCapacity& GetConsumedCapacity() const;

    void SetConsumedCapacity(const ConsumedCapacity& consumedCapacity);

private:

    RowPtr mRow;

    ConsumedCapacity mConsumedCapacity;
};

/**
 * @brief RowChange
 *
 * 修改行的信息。
 */
class RowChange
{
public:

    RowChange(const std::string& tableName);

    RowChange(const std::string& tableName,
              const PrimaryKey& primaryKey);

    RowChange(const std::string& tableName,
              const PrimaryKey& primaryKey,
              const Condition& condition);

    virtual ~RowChange();

    virtual const std::string& GetTableName() const;
    virtual void SetTableName(const std::string& tableName);

    virtual const PrimaryKey& GetPrimaryKey() const;
    virtual void SetPrimaryKey(const PrimaryKey& primaryKey);

    virtual void AddPrimaryKeyColumn(
        const PrimaryKeyColumn& primaryKeyColumn);
    virtual void AddPrimaryKeyColumn(
        const std::string& name,
        const PrimaryKeyValue& value);

    void SetCondition(const Condition& condition);
    const Condition& GetCondition() const;

    void SetReturnType(ReturnType returnType);
    ReturnType GetReturnType() const;
private:

    std::string mTableName;
    PrimaryKey mPrimaryKey;
    Condition mCondition;
    ReturnType mReturnType;
};

/**
 * @brief RowPutChange
 *
 * PutRow参数。
 */
class RowPutChange : public RowChange
{
public:

    RowPutChange(const std::string& tableName);

    RowPutChange(
        const std::string& tableName,
        const PrimaryKey& primaryKey);

    RowPutChange(
        const std::string& tableName,
        const PrimaryKey& primaryKey,
        const std::list<Column>& columns);

    virtual ~RowPutChange();

    void SetColumns(const std::list<Column>& columnsToPut);

    void AddColumn(const Column& column);

    void AddColumn(
        const std::string& name,
        const ColumnValue& value);

    void AddColumn(
        const std::string& name,
        const ColumnValue& value,
        int64_t ts);

    size_t GetColumnsSize() const;

    const std::list<Column>& GetColumns() const;

private:

    std::list<Column> mColumns;
};

/**
 * @brief PutRowRequest
 *
 * PutRow参数。
 */
class PutRowRequest : public OTSRequest
{
public:

    PutRowRequest(const RowPutChange& rowChange);

    virtual ~PutRowRequest();

    const RowPutChange& GetRowChange() const;

    void SetRowChange(const RowPutChange& rowChange);

private:

    RowPutChange mRowChange;
};

/**
 * @brief PutRowResult
 *
 * PutRow返回值。
 */
class PutRowResult : public OTSResult
{
public:

    PutRowResult(); 

    PutRowResult(
        const OTSResult& result,
        const RowPtr& row,
        const ConsumedCapacity& consumedCapacity); 

    virtual ~PutRowResult();

    const RowPtr& GetRow() const;

    void SetRow(const RowPtr& rowPtr);

    const ConsumedCapacity& GetConsumedCapacity() const;

    void SetConsumedCapacity(const ConsumedCapacity& consumedCapacity);

private:

    RowPtr mRow;

    ConsumedCapacity mConsumedCapacity;
};

/**
 * @brief RowUpdateChange
 *
 * UpdateRow参数。
 */
class RowUpdateChange : public RowChange
{
public:

    /**
     * @brief UpdateType
     *
     * 修改一列的类型。
     */
    enum UpdateType
    {
        PUT,
        DELETE,
        DELETE_ALL
    };

public:

    RowUpdateChange(const std::string& tableName);

    RowUpdateChange(
        const std::string& tableName,
        const PrimaryKey& primaryKey);

    virtual ~RowUpdateChange();

    void PutColumn(const Column& column);

    void PutColumn(
        const std::string& name,
        const ColumnValue& value);

    void PutColumn(
        const std::string& name,
        const ColumnValue& value,
        int64_t ts);

    void PutColumns(const std::list<Column>& columns);

    void DeleteColumn(const std::string& name, int64_t ts);

    void DeleteColumns(const std::string& name);

    size_t GetColumnsSize() const;

    const std::list<Column>& GetColumns() const;

    const std::list<RowUpdateChange::UpdateType>& GetUpdateTypes() const;

private:

    std::list<Column> mColumns;

    std::list<RowUpdateChange::UpdateType> mUpdateTypes;
};

/**
 * @brief UpdateRowRequest
 *
 * UpdateRow参数。
 */
class UpdateRowRequest : public OTSRequest
{
public:

    UpdateRowRequest(const RowUpdateChange& rowChange);

    virtual ~UpdateRowRequest();

    const RowUpdateChange& GetRowChange() const;

    void SetRowChange(const RowUpdateChange& rowChange);

private:

    RowUpdateChange mRowChange;
};

/**
 * @brief UpdateRowResult
 *
 * UpdateRow返回值。
 */
class UpdateRowResult : public OTSResult
{
public:

    UpdateRowResult();

    UpdateRowResult(
        const OTSResult& result,
        const RowPtr& row,
        const ConsumedCapacity& consumedCapacity); 

    virtual ~UpdateRowResult();

    const RowPtr& GetRow() const;

    void SetRow(const RowPtr& rowPtr);

    const ConsumedCapacity& GetConsumedCapacity() const;

    void SetConsumedCapacity(const ConsumedCapacity& consumedCapacity);

private:

    RowPtr mRow;
    ConsumedCapacity mConsumedCapacity;
};

/**
 * @brief DeleteRowChange
 *
 * DeleteRow参数。
 */
class RowDeleteChange : public RowChange
{
public:

    RowDeleteChange(const std::string& tableName);

    RowDeleteChange(
        const std::string& tableName,
        const PrimaryKey& primaryKey);

    virtual ~RowDeleteChange();
};

/**
 * @brief DeleteRowRequest
 *
 * DeleteRow参数。
 */
class DeleteRowRequest : public OTSRequest
{
public:

    DeleteRowRequest(const RowDeleteChange& rowChange);

    virtual ~DeleteRowRequest();

    const RowDeleteChange& GetRowChange() const;

    void SetRowChange(const RowDeleteChange& rowChange);

private:

    RowDeleteChange mRowChange;
};

/**
 * @brief DeleteRowResult
 *
 * DeleteRow返回值。
 */
class DeleteRowResult : public OTSResult
{
public:

    DeleteRowResult();

    DeleteRowResult(
        const OTSResult& result,
        const RowPtr& row,
        const ConsumedCapacity& consumedCapacity); 

    virtual ~DeleteRowResult();

    const RowPtr& GetRow() const;

    void SetRow(const RowPtr& rowPtr);

    const ConsumedCapacity& GetConsumedCapacity() const;

    void SetConsumedCapacity(const ConsumedCapacity& consumedCapacity);

private:

    RowPtr mRow;

    ConsumedCapacity mConsumedCapacity;
};

/**
 * @brief MultiRowQueryCriteria 
 *
 * 批量查询参数。
 */
class MultiRowQueryCriteria : public RowQueryCriteria
{
public:

    MultiRowQueryCriteria();

    MultiRowQueryCriteria(const std::string& tableName); 

    virtual ~MultiRowQueryCriteria();

    void AddPrimaryKey(const PrimaryKey& primaryKey);

    const std::list<PrimaryKey>& GetPrimaryKeys() const;

    void SetPrimaryKeys(const std::list<PrimaryKey>& primaryKeys);

private:

    std::list<PrimaryKey> mRowKeys;
};

/**
 * @brief BatchGetRowRequest
 *
 * 批量读参数。
 */
class BatchGetRowRequest : public OTSRequest
{
public:

    BatchGetRowRequest();

    virtual ~BatchGetRowRequest();

    void AddMultiRowQueryCriteria(const MultiRowQueryCriteria& criteria);

    const std::map<std::string, MultiRowQueryCriteria>& GetCriterias() const;

    const MultiRowQueryCriteria& GetCriteria(const std::string& tableName);

private:

    std::map<std::string, MultiRowQueryCriteria> mCriteriasMap;
};

/**
 * @brief BatchGetRowResult
 *
 * 批量读结果。
 */
class BatchGetRowResult : public OTSResult
{
public:
    /**
     * @brief RowResult
     *
     * 单行结果。
     */
    class RowResult
    {
    public:

        RowResult(const std::string& tableName);

        RowResult(
            const std::string& tableName,
            const Error& error);
    
        RowResult(
            const std::string& tableName,
            const RowPtr& row,
            const ConsumedCapacity& consumedCapacity);
    
        bool IsSuccessful() const;

        void SetIsSuccessful(bool isSuccessful);

        const std::string& GetTableName() const;

        void SetTableName(const std::string& tableName);
    
        const Error& GetError() const;

        void SetError(const Error& error);
    
        const RowPtr& GetRow() const;

        void SetRow(const RowPtr& row);
    
        const ConsumedCapacity& GetConsumedCapacity() const;

        void SetConsumedCapacity(const ConsumedCapacity& consumedCapacity);
    
    private:
    
        bool mIsSuccessful;

        std::string mTableName;
    
        Error mError;
    
        RowPtr mRow;
    
        ConsumedCapacity mConsumedCapacity; 
    };

public:

    BatchGetRowResult();

    BatchGetRowResult(const OTSResult& result);

    const std::map<std::string, std::list<RowResult> >& GetTableToRowResults() const;

    const std::list<RowResult>& GetRowResults(const std::string& tableName) const;

    void AddRowResult(const RowResult& rowResult);

    bool IsAllSuccessful() const;

private:

    std::map<std::string, std::list<BatchGetRowResult::RowResult> > mTableToRowResults;
};

/**
 * @brief BatchWriteRowRequest
 *
 * 批量写参数。
 */
class BatchWriteRowRequest : public OTSRequest
{
public:
    BatchWriteRowRequest();

    virtual ~BatchWriteRowRequest();

    void AddRowPutChange(const RowPutChange& rowPutChange);
    const std::map<std::string, std::list<RowPutChange> >& GetRowPutChanges() const;

    void AddRowUpdateChange(const RowUpdateChange& rowUpdateChange);
    const std::map<std::string, std::list<RowUpdateChange> >& GetRowUpdateChanges() const;

    void AddRowDeleteChange(const RowDeleteChange& rowDeleteChange);
    const std::map<std::string, std::list<RowDeleteChange> >& GetRowDeleteChanges() const;

    bool IsEmpty() const;

private:
    std::map<std::string, std::list<RowPutChange> > mTableToRowPutChanges;
    std::map<std::string, std::list<RowUpdateChange> > mTableToRowUpdateChanges;
    std::map<std::string, std::list<RowDeleteChange> > mTableToRowDeleteChanges;
};

/**
 * @brief BatchWriteRowResult
 *
 * 批量写参数。
 */
class BatchWriteRowResult : public OTSResult
{
public:

    /**
     * @brief RowResult
     *
     * 单行结果。
     */
    class RowResult
    {
    public:

        RowResult(const std::string& tableName);

        RowResult(
            const std::string& tableName,
            const RowPtr& row,
            const Error& error);

        RowResult(
            const std::string& tableName,
            const RowPtr& row,
            const ConsumedCapacity& consumedCapacity);

        bool IsSuccessful() const;

        void SetIsSuccessful(bool isSuccessful);

        const std::string& GetTableName() const;

        void SetTableName(const std::string& tableName);

        const RowPtr& GetRow() const;
        
        void SetRow(const RowPtr& rowPtr);

        const Error& GetError() const;

        void SetError(const Error& error);

        const ConsumedCapacity& GetConsumedCapacity() const;

        void SetConsumedCapacity(const ConsumedCapacity& consumedCapacity);

    private:

        bool mIsSuccessful;

        std::string mTableName;

        RowPtr mRow;

        Error mError;

        ConsumedCapacity mConsumedCapacity;
    };

public:

    BatchWriteRowResult();

    BatchWriteRowResult(const OTSResult& result);

    virtual ~BatchWriteRowResult();

    const std::map<std::string, std::list<RowResult> >& GetTableToPutRowResults() const;

    const std::list<RowResult>& GetPutRowResults(const std::string& tableName) const;

    void AddPutRowResult(const RowResult& rowResult);

    const std::map<std::string, std::list<RowResult> >& GetUpdateRowResults() const;

    const std::list<RowResult>& GetUpdateRowResults(const std::string& tableName) const;

    void AddUpdateRowResult(const RowResult& rowResult);

    const std::map<std::string, std::list<RowResult> >& GetDeleteRowResults() const;

    const std::list<RowResult>& GetDeleteRowResults(const std::string& tableName) const;

    void AddDeleteRowResult(const RowResult& rowResult);

    bool IsAllSuccessful() const;

private:

    bool HasRowFailed(const std::map<std::string, std::list<RowResult> >& tableToRowResults) const;

private:

    std::map<std::string, std::list<RowResult> > mTableToPutRowResults;

    std::map<std::string, std::list<RowResult> > mTableToUpdateRowResults;

    std::map<std::string, std::list<RowResult> > mTableToDeleteRowResults;
};

/**
 * @brief RangeRowQueryCriteria
 *
 * 范围查询参数。
 */
class RangeRowQueryCriteria : public RowQueryCriteria
{
public:

    /**
     * @brief Direction
     *
     * 范围查询的方向
     */
    enum Direction
    {
        FORWARD,
        BACKWARD
    };

public:

    RangeRowQueryCriteria(const std::string& tableName); 

    virtual ~RangeRowQueryCriteria();

    bool HasLimit() const;

    int GetLimit() const;

    void SetLimit(int limit);

    bool HasDirection() const;

    const Direction GetDirection() const;

    void SetDirection(Direction direction);

    const PrimaryKey& GetInclusiveStartPrimaryKey() const;

    void SetInclusiveStartPrimaryKey(const PrimaryKey& primaryKey);

    const PrimaryKey& GetExclusiveEndPrimaryKey() const;

    void SetExclusiveEndPrimaryKey(const PrimaryKey& primaryKey);

private:

    OptionalValue<int> mLimit;

    OptionalValue<Direction> mDirection;

    PrimaryKey mInclusiveStartPrimaryKey;

    PrimaryKey mExclusiveEndPrimaryKey;
};

/**
 * @brief GetRangeRequest
 *
 * 范围查询参数。
 */
class GetRangeRequest : public OTSRequest
{
public:

    GetRangeRequest(const RangeRowQueryCriteria& rangeRowQueryCriteria);

    virtual ~GetRangeRequest();

    const RangeRowQueryCriteria& GetRowQueryCriteria() const;

    void SetRowQueryCriteria(const RangeRowQueryCriteria& rangeRowQueryCriteria);

    void SetInclusiveStartPrimaryKey(const PrimaryKey& startPrimaryKey);

private:

    RangeRowQueryCriteria mRowQueryCriteria;
};

/**
 * @brief GetRangeResult
 *
 * 范围查询的结果。
 */
class GetRangeResult : public OTSResult
{
public:

    GetRangeResult();

    GetRangeResult(const OTSResult& result);

    virtual ~GetRangeResult();

    const ConsumedCapacity& GetConsumedCapacity() const;

    void SetConsumedCapacity(const ConsumedCapacity& consumedCapacity);

    const std::list<RowPtr>& GetRows() const;

    void SetRows(const std::list<RowPtr>& rows);

    bool HasNextStartPrimaryKey() const;

    const PrimaryKey& GetNextStartPrimaryKey() const;

    void SetNextStartPrimaryKey(const PrimaryKey& nextStartPrimaryKey);

private:

    ConsumedCapacity mConsumedCapacity;

    std::list<RowPtr> mRows;

    OptionalValue<PrimaryKey> mNextStartPrimaryKey;
};

/**
 * @brief RangeIteratorParameter
 *
 * 范围查询迭代器的参数。
 */
class RangeIteratorParameter 
{
public:
    RangeIteratorParameter();
};

class ComputeSplitsBySizeRequest : public OTSRequest
{
    static const int64_t kDefaultSplitSize = 5; // 500MB
public:
    ComputeSplitsBySizeRequest()
      : mSplitSize(kDefaultSplitSize)
    {}

    explicit ComputeSplitsBySizeRequest(
        const std::string& tableName,
        int64_t splitSize)
      : mTableName(tableName),
        mSplitSize(splitSize)
    {}
    
    const std::string& GetTableName() const
    {
        return mTableName;
    }

    void SetTableName(const std::string& tableName)
    {
        mTableName = tableName;
    }

    int64_t GetSplitSize() const
    {
        return mSplitSize;
    }

    void SetSplitsSize(int64_t splitSize)
    {
        mSplitSize = splitSize;
    }

private:
    std::string mTableName;
    int64_t mSplitSize;
};

class ComputeSplitsBySizeResult : public OTSResult
{
public:
    ComputeSplitsBySizeResult()
    {}

    explicit ComputeSplitsBySizeResult(const OTSResult& result)
      : OTSResult(result)
    {}

    const CapacityUnit& GetConsumedCapacity() const
    {
        return mConsumedCapacity;
    }

    CapacityUnit* MutableConsumedCapacity()
    {
        return &mConsumedCapacity;
    }


    const IVector<PrimaryKeySchema>& GetSchema() const
    {
        return mSchema;
    }

    IVector<PrimaryKeySchema>* MutableSchema()
    {
        return &mSchema;
    }

    const IVector<Split>& GetSplits() const
    {
        return mSplits;
    }

    IVector<Split>* MutableSplits()
    {
        return &mSplits;
    }

private:
    CapacityUnit mConsumedCapacity;
    DequeBasedVector<PrimaryKeySchema> mSchema;
    DequeBasedVector<Split> mSplits;
};


// shared_ptr for all requests and results
typedef std::tr1::shared_ptr<OTSRequest> OTSRequestPtr;
typedef std::tr1::shared_ptr<CreateTableRequest> CreateTableRequestPtr;
typedef std::tr1::shared_ptr<ListTableRequest> ListTableRequestPtr;
typedef std::tr1::shared_ptr<DescribeTableRequest> DescribeTableRequestPtr;
typedef std::tr1::shared_ptr<DeleteTableRequest> DeleteTableRequestPtr;
typedef std::tr1::shared_ptr<UpdateTableRequest> UpdateTableRequestPtr;
typedef std::tr1::shared_ptr<GetRowRequest> GetRowRequestPtr;
typedef std::tr1::shared_ptr<PutRowRequest> PutRowRequestPtr;
typedef std::tr1::shared_ptr<UpdateRowRequest> UpdateRowRequestPtr;
typedef std::tr1::shared_ptr<DeleteRowRequest> DeleteRowRequestPtr;
typedef std::tr1::shared_ptr<BatchGetRowRequest> BatchGetRowRequestPtr;
typedef std::tr1::shared_ptr<BatchWriteRowRequest> BatchWriteRowRequestPtr;
typedef std::tr1::shared_ptr<GetRangeRequest> GetRangeRequestPtr;
typedef std::tr1::shared_ptr<ComputeSplitsBySizeRequest> ComputeSplitsBySizeRequestPtr;

typedef std::tr1::shared_ptr<OTSResult> OTSResultPtr;
typedef std::tr1::shared_ptr<CreateTableResult> CreateTableResultPtr;
typedef std::tr1::shared_ptr<ListTableResult> ListTableResultPtr;
typedef std::tr1::shared_ptr<DescribeTableResult> DescribeTableResultPtr;
typedef std::tr1::shared_ptr<DeleteTableResult> DeleteTableResultPtr;
typedef std::tr1::shared_ptr<UpdateTableResult> UpdateTableResultPtr;
typedef std::tr1::shared_ptr<GetRowResult> GetRowResultPtr;
typedef std::tr1::shared_ptr<PutRowResult> PutRowResultPtr;
typedef std::tr1::shared_ptr<UpdateRowResult> UpdateRowResultPtr;
typedef std::tr1::shared_ptr<DeleteRowResult> DeleteRowResultPtr;
typedef std::tr1::shared_ptr<BatchGetRowResult> BatchGetRowResultPtr;
typedef std::tr1::shared_ptr<BatchWriteRowResult> BatchWriteRowResultPtr;
typedef std::tr1::shared_ptr<GetRangeResult> GetRangeResultPtr;
typedef std::tr1::shared_ptr<ComputeSplitsBySizeResult> ComputeSplitsBySizeResultPtr;

typedef std::tr1::shared_ptr<Iterator<RowPtr> > RowIteratorPtr;

} // end of tablestore
} // end of aliyun

#endif
