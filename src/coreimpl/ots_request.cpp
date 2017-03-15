/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 *
 * 版权所有 （C）阿里云计算有限公司
 */

#include "ots/ots_request.h"
#include <iostream>

using namespace std;

namespace aliyun {
namespace tablestore {

// OTSRequest

OTSRequest::OTSRequest()
{
}

OTSRequest::~OTSRequest()
{
}

// OTSResult

OTSResult::OTSResult()
{
}

OTSResult::OTSResult(
    const string& requestId,
    const string& traceId)
    : mRequestId(requestId)
    , mTraceId(traceId)
{
}

OTSResult::~OTSResult()
{
}

const string& OTSResult::GetRequestId() const
{
    return mRequestId;
}

void OTSResult::SetRequestId(const string& requestId)
{
    mRequestId = requestId;
}

const string& OTSResult::GetTraceId() const
{
    return mTraceId;
}

void OTSResult::SetTraceId(const string& traceId)
{
    mTraceId = traceId;
}

// CreateTableRequest

CreateTableRequest::CreateTableRequest(
    const TableMeta& tableMeta,
    const ReservedThroughput& reservedThroughput,
    const TableOptions& tableOptions)
    : mTableMeta(tableMeta)
    , mReservedThroughput(reservedThroughput)
    , mTableOptions(tableOptions)
{
}

CreateTableRequest::CreateTableRequest(
    const TableMeta& tableMeta,
    const ReservedThroughput& reservedThroughput,
    const TableOptions& tableOptions,
    const list<PartitionRange>& partitionRanges)
    : mTableMeta(tableMeta)
    , mReservedThroughput(reservedThroughput)
    , mTableOptions(tableOptions)
    , mPartitionRanges(partitionRanges)
{
}

CreateTableRequest::~CreateTableRequest()
{
}

const TableMeta& CreateTableRequest::GetTableMeta() const
{
    return mTableMeta;
}

void CreateTableRequest::SetTableMeta(const TableMeta& tableMeta)
{
    mTableMeta = tableMeta;
}

const ReservedThroughput& CreateTableRequest::GetReservedThroughput() const
{
    return mReservedThroughput;
}

void CreateTableRequest::SetReservedThroughput(const ReservedThroughput& reservedThroughput)
{
    mReservedThroughput = reservedThroughput;
}

const TableOptions& CreateTableRequest::GetTableOptions() const
{
    return mTableOptions;
}

void CreateTableRequest::SetTableOptions(const TableOptions& tableOptions)
{
    mTableOptions = tableOptions;
}

bool CreateTableRequest::HasPartitionRanges() const
{
    return mPartitionRanges.HasValue();
}

const list<PartitionRange>& CreateTableRequest::GetPartitionRanges() const
{
    return mPartitionRanges.GetValue();
}

void CreateTableRequest::SetPartitionRanges(const list<PartitionRange>& partitionRanges)
{
    mPartitionRanges.SetValue(partitionRanges);
}

// CreateTableResult

CreateTableResult::CreateTableResult() 
{
}

CreateTableResult::CreateTableResult(const OTSResult& result)
    : OTSResult(result)
{
}

CreateTableResult::~CreateTableResult()
{
}

// ListTableRequest

ListTableRequest::ListTableRequest()
{
}

ListTableRequest::~ListTableRequest()
{
}

// ListTableResult

ListTableResult::ListTableResult()
{
}

ListTableResult::ListTableResult(const OTSResult& result)
    : OTSResult(result.GetRequestId(), result.GetTraceId())
{
}

ListTableResult::~ListTableResult()
{
}

const list<string>& ListTableResult :: GetTableNames()
{
    return mTableNames;
}

void ListTableResult::AddTableName(const string& tableName)
{
    mTableNames.push_back(tableName);
}


//DescribeTableRequest

DescribeTableRequest::DescribeTableRequest(const string& tableName)
    : mTableName(tableName)
{
}

DescribeTableRequest::~DescribeTableRequest()
{
}

const string& DescribeTableRequest::GetTableName() const
{
    return mTableName;
}

void DescribeTableRequest::SetTableName(const string& tableName)
{
    mTableName = tableName;
}

//DescribeTableResult

DescribeTableResult::DescribeTableResult()
{
}

DescribeTableResult::DescribeTableResult(const OTSResult& result)
    : OTSResult(result.GetRequestId(), result.GetTraceId())
{
}

const TableMeta& DescribeTableResult::GetTableMeta() const
{
    return mTableMeta;
}

void DescribeTableResult::SetTableMeta(const TableMeta& tableMeta)
{
    mTableMeta = tableMeta;
}

const ReservedThroughputDetails& DescribeTableResult::GetReservedThroughputDetails() const
{
    return mReservedThroughputDetails;
}

void DescribeTableResult::SetReservedThroughputDetails(const ReservedThroughputDetails& reservedThroughputDetails)
{
    mReservedThroughputDetails = reservedThroughputDetails;
}

const TableOptions& DescribeTableResult::GetTableOptions() const
{
    return mTableOptions;
}

void DescribeTableResult::SetTableOptions(const TableOptions& tableOptions)
{
    mTableOptions = tableOptions;
}

const TableStatus& DescribeTableResult::GetTableStatus() const
{
    return mTableStatus;
}

void DescribeTableResult::SetTableStatus(const TableStatus& tableStatus)
{
    mTableStatus = tableStatus;
}

const deque<PrimaryKey>& DescribeTableResult::GetShardSplits() const
{
    return mShardSplits;
}

void DescribeTableResult::SetShardSplits(const deque<PrimaryKey>& splits)
{
    mShardSplits = splits;
}

//DeleteTableRequest

DeleteTableRequest::DeleteTableRequest(const string& tableName)
    : mTableName(tableName)
{
}

DeleteTableRequest::~DeleteTableRequest()
{
}

const string& DeleteTableRequest::GetTableName() const
{
    return mTableName;
}

void DeleteTableRequest::SetTableName(const string& tableName)
{
    mTableName = tableName;
}

// DeleteTableResult

DeleteTableResult::DeleteTableResult() 
{
}

DeleteTableResult::DeleteTableResult(const OTSResult& result) : OTSResult(result)
{
}

DeleteTableResult::~DeleteTableResult()
{
}

// UpdateTableRequst

UpdateTableRequest::UpdateTableRequest(const string& tableName)
    : mTableName(tableName)
{
}

UpdateTableRequest::~UpdateTableRequest()
{
}

const string& UpdateTableRequest::GetTableName() const
{
    return mTableName;
}

void UpdateTableRequest::SetTableName(const string& tableName)
{
    mTableName = tableName;
}

bool UpdateTableRequest::HasReservedThroughputForUpdate() const
{
    return mReservedThroughputForUpdate.HasValue();
}

const ReservedThroughput& UpdateTableRequest::GetReservedThroughputForUpdate() const
{
    return mReservedThroughputForUpdate.GetValue();
}

void UpdateTableRequest::SetReservedThroughputForUpdate(const ReservedThroughput& reservedThroughputForUpdate)
{
    mReservedThroughputForUpdate.SetValue(reservedThroughputForUpdate);
}

bool UpdateTableRequest::HasTableOptionsForUpdate() const
{
    return mTableOptionsForUpdate.HasValue();
}

const TableOptions& UpdateTableRequest::GetTableOptionsForUpdate() const
{
    return mTableOptionsForUpdate.GetValue();
}

void UpdateTableRequest::SetTableOptionsForUpdate(const TableOptions& tableOptionsForUpdate)
{
    mTableOptionsForUpdate.SetValue(tableOptionsForUpdate);
}

// UpdateTableResult

UpdateTableResult:: UpdateTableResult()
{
}

UpdateTableResult::UpdateTableResult(const OTSResult& result)
    : OTSResult(result)
{
}

UpdateTableResult::~UpdateTableResult()
{
}

//RowQueryCriteria

RowQueryCriteria::RowQueryCriteria(const string& tableName)
    : mTableName(tableName)
{
}

RowQueryCriteria::~RowQueryCriteria()
{
}

const string& RowQueryCriteria::GetTableName() const
{
    return mTableName;
}

void  RowQueryCriteria::SetTableName(const string& tableName)
{
    mTableName = tableName;
}

const list<string>& RowQueryCriteria::GetColumnsToGet() const
{
    return mColumnsToGet;
}

void RowQueryCriteria::SetColumnsToGet(const list<string>& columnsToGet)
{
    mColumnsToGet = columnsToGet;
}

void RowQueryCriteria::AddColumnName(const string& columnName)
{
    mColumnsToGet.push_back(columnName);
}

void RowQueryCriteria::SetMaxVersions(int maxVersions)
{
    mMaxVersions.SetValue(maxVersions);
}

int RowQueryCriteria::GetMaxVersions() const
{
    if (!mMaxVersions.HasValue()) {
        throw OTSClientException("MaxVersions is not set.");
    }
    return mMaxVersions.GetValue();
}

bool RowQueryCriteria::HasMaxVersions() const
{
    return mMaxVersions.HasValue();
}

void RowQueryCriteria::SetTimeRange(const TimeRange& timeRange)
{
    mTimeRange.SetValue(timeRange);
}

void RowQueryCriteria::SetTimestamp(int64_t ts)
{
    mTimeRange.SetValue(TimeRange(ts, ts + 1));
}

const TimeRange& RowQueryCriteria::GetTimeRange() const
{
    if (!mTimeRange.HasValue()) {
        throw OTSClientException("TimeRange is not set.");
    }
    return mTimeRange.GetValue();
}

bool RowQueryCriteria::HasTimeRange() const
{
    return mTimeRange.HasValue();
}

void RowQueryCriteria::SetCacheBlocks(bool cacheBlocks)
{
    mCacheBlocks.SetValue(cacheBlocks);
}

bool RowQueryCriteria::GetCacheBlocks() const
{
    if (!mCacheBlocks.HasValue()) {
        throw OTSClientException("MaxVersions is not set.");
    }
    return mMaxVersions.GetValue();
}

bool RowQueryCriteria::HasCacheBlocks() const
{
    return mCacheBlocks.HasValue();
}

void RowQueryCriteria::SetFilter(const ColumnConditionPtr& filterPtr)
{
    mFilter.SetValue(filterPtr);
}

const ColumnConditionPtr& RowQueryCriteria::GetFilter() const
{
    if (!mFilter.HasValue()) {
        throw OTSClientException("Filter is not set.");
    }
    return mFilter.GetValue();
}

bool RowQueryCriteria::HasFilter() const
{
    return mFilter.HasValue();
}

// SingleRowQueryCriteria

SingleRowQueryCriteria::SingleRowQueryCriteria(const string& tableName)
    : RowQueryCriteria(tableName)
{
}

SingleRowQueryCriteria::SingleRowQueryCriteria(
    const string& tableName,
    const PrimaryKey& primaryKey)
    : RowQueryCriteria(tableName)
    , mPrimaryKey(primaryKey)
{
}

SingleRowQueryCriteria::~SingleRowQueryCriteria()
{
}

const PrimaryKey& SingleRowQueryCriteria::GetPrimaryKey() const
{
    return mPrimaryKey;
}

void SingleRowQueryCriteria::SetPrimaryKey(const PrimaryKey& primaryKey)
{
    mPrimaryKey = primaryKey;
}

void SingleRowQueryCriteria::AddPrimaryKeyColumn(
    const PrimaryKeyColumn& primaryKeyColumn)
{
    mPrimaryKey.AddPrimaryKeyColumn(primaryKeyColumn);
}

void SingleRowQueryCriteria::AddPrimaryKeyColumn(
    const string& name,
    const PrimaryKeyValue& value)
{
    mPrimaryKey.AddPrimaryKeyColumn(name, value);
}

// GetRowRequest

GetRowRequest::GetRowRequest(const SingleRowQueryCriteria& rowQueryCriteria)
    : mRowQueryCriteria(rowQueryCriteria)
{
}

GetRowRequest::~GetRowRequest()
{
}

const SingleRowQueryCriteria& GetRowRequest::GetRowQueryCriteria() const
{
    return mRowQueryCriteria;
}

void GetRowRequest::SetRowQueryCriteria(const SingleRowQueryCriteria& rowQueryCriteria)
{
    mRowQueryCriteria = rowQueryCriteria;
}

// GetRowResult

GetRowResult::GetRowResult()
{
}

GetRowResult::GetRowResult(
    const OTSResult& result,
    const RowPtr& row,
    const ConsumedCapacity& consumedCapacity)
    : OTSResult(result)
    , mRow(row)
    , mConsumedCapacity(consumedCapacity)
{
}

GetRowResult::~GetRowResult()
{
}

const RowPtr& GetRowResult::GetRow() const
{
    return mRow;
}

void GetRowResult::SetRow(const RowPtr& row)
{
    mRow = row;
}

const ConsumedCapacity& GetRowResult::GetConsumedCapacity() const
{
    return mConsumedCapacity;
}

void GetRowResult::SetConsumedCapacity(const ConsumedCapacity& consumedCapacity)
{
    mConsumedCapacity = consumedCapacity;
}

// RowChange

RowChange::RowChange(const string& tableName)
    : mTableName(tableName)
    , mReturnType(RT_NONE)
{
}

RowChange::RowChange(
    const string& tableName,
    const PrimaryKey& primaryKey)
    : mTableName(tableName)
    , mPrimaryKey(primaryKey)
    , mReturnType(RT_NONE)
{
}

RowChange::RowChange(const std::string& tableName,
                     const PrimaryKey& primaryKey,
                     const Condition& condition)
    : mTableName(tableName)
    , mPrimaryKey(primaryKey)
    , mCondition(condition)
    , mReturnType(RT_NONE)
{
}

RowChange::~RowChange()
{
}

const string& RowChange::GetTableName() const
{
    return mTableName;
}

void RowChange::SetTableName(const string& tableName)
{
    mTableName = tableName;
}

const PrimaryKey& RowChange::GetPrimaryKey() const
{
    return mPrimaryKey;
}

void RowChange::SetPrimaryKey(const PrimaryKey& primaryKey)
{
    mPrimaryKey = primaryKey;
}

void RowChange::AddPrimaryKeyColumn(const PrimaryKeyColumn& primaryKeyColumn)
{
    mPrimaryKey.AddPrimaryKeyColumn(primaryKeyColumn);
}

void RowChange::AddPrimaryKeyColumn(
    const string& name,
    const PrimaryKeyValue& value)
{
    mPrimaryKey.AddPrimaryKeyColumn(name, value);
}

const Condition& RowChange::GetCondition() const
{
    return mCondition;
}

void RowChange::SetCondition(const Condition& condition)
{
    mCondition = condition;
}

void RowChange::SetReturnType(ReturnType returnType)
{
    mReturnType = returnType;
}

ReturnType RowChange::GetReturnType() const
{
    return mReturnType;
}

// RowPutChange

RowPutChange::RowPutChange(const string& tableName)
    : RowChange(tableName)
{
}

RowPutChange::RowPutChange(
    const string& tableName,
    const PrimaryKey& primaryKey)
    : RowChange(tableName, primaryKey)
{
}

RowPutChange::RowPutChange(
    const string& tableName,
    const PrimaryKey& primaryKey,
    const list<Column>& columns)
    : RowChange(tableName, primaryKey)
    , mColumns(columns)
{
}

RowPutChange::~RowPutChange()
{
}

void RowPutChange::SetColumns(const list<Column>& columns)
{
    mColumns = columns;
}

void RowPutChange::AddColumn(const Column& column)
{
    mColumns.push_back(column);
}

void RowPutChange::AddColumn(
    const string& name,
    const ColumnValue& value)
{
    mColumns.push_back(Column(name, value));
}

void RowPutChange::AddColumn(
    const string& name,
    const ColumnValue& value,
    int64_t ts)
{
    mColumns.push_back(Column(name, value, ts));
}

size_t RowPutChange::GetColumnsSize() const
{
    return mColumns.size();
}

const list<Column>& RowPutChange::GetColumns() const
{
    return mColumns;
}

// PutRowRequest

PutRowRequest::PutRowRequest(const RowPutChange& rowChange)
    : mRowChange(rowChange)
{
}

PutRowRequest::~PutRowRequest()
{
}

const RowPutChange& PutRowRequest::GetRowChange() const
{
    return mRowChange;
}

void PutRowRequest::SetRowChange(const RowPutChange& rowChange)
{
    mRowChange = rowChange;
}

// PutRowResult

PutRowResult::PutRowResult()
{
}

PutRowResult::PutRowResult(
    const OTSResult& result,
    const RowPtr& row,
    const ConsumedCapacity& consumedCapacity)
    : OTSResult(result)
    , mRow(row)
    , mConsumedCapacity(consumedCapacity)
{
}

PutRowResult::~PutRowResult()
{
}

const ConsumedCapacity& PutRowResult::GetConsumedCapacity() const
{
    return mConsumedCapacity;
}

void PutRowResult::SetConsumedCapacity(const ConsumedCapacity& consumedCapacity)
{
    mConsumedCapacity = consumedCapacity;
}

const RowPtr& PutRowResult::GetRow() const
{
    return mRow;
}

void PutRowResult::SetRow(const RowPtr& rowPtr)
{
    mRow = rowPtr;
}

// RowUpdateChange

RowUpdateChange::RowUpdateChange(const string& tableName)
    : RowChange(tableName)
{
}

RowUpdateChange::RowUpdateChange(
    const string& tableName,
    const PrimaryKey& primaryKey)
    : RowChange(tableName, primaryKey)
{
}

RowUpdateChange::~RowUpdateChange()
{
}

void RowUpdateChange::PutColumn(const Column& column)
{
    mColumns.push_back(column);
    mUpdateTypes.push_back(RowUpdateChange::PUT);
}

void RowUpdateChange::PutColumn(
    const string& name,
    const ColumnValue& value)
{
    mColumns.push_back(Column(name, value));
    mUpdateTypes.push_back(RowUpdateChange::PUT);
}

void RowUpdateChange::PutColumn(
    const string& name,
    const ColumnValue& value,
    int64_t ts)
{
    mColumns.push_back(Column(name, value, ts));
    mUpdateTypes.push_back(RowUpdateChange::PUT);
}

void RowUpdateChange::PutColumns(const list<Column>& columns)
{
    for (typeof(columns.begin()) iter = columns.begin(); iter != columns.end(); ++iter) {
        mColumns.push_back(*iter);
        mUpdateTypes.push_back(RowUpdateChange::PUT);
    }
}

void RowUpdateChange::DeleteColumn(const string& name, int64_t ts)
{
    Column column(name);
    column.SetTimestamp(ts);
    mColumns.push_back(column);
    mUpdateTypes.push_back(RowUpdateChange::DELETE);
}

void RowUpdateChange::DeleteColumns(const string& name)
{
    Column column(name);
    mColumns.push_back(column);
    mUpdateTypes.push_back(RowUpdateChange::DELETE_ALL);
}

size_t RowUpdateChange::GetColumnsSize() const
{
    return mColumns.size();
}

const list<Column>& RowUpdateChange::GetColumns() const
{
    return mColumns;
}

const list<RowUpdateChange::UpdateType>& RowUpdateChange::GetUpdateTypes() const
{
    return mUpdateTypes;
}

// UpdateRowRequest

UpdateRowRequest::UpdateRowRequest(const RowUpdateChange& rowChange)
    : mRowChange(rowChange)
{
}

UpdateRowRequest::~UpdateRowRequest()
{
}

const RowUpdateChange& UpdateRowRequest::GetRowChange() const
{
    return mRowChange;
}

void UpdateRowRequest::SetRowChange(const RowUpdateChange& rowChange)
{
    mRowChange = rowChange;
}

// UpdateRowResult

UpdateRowResult::UpdateRowResult()
{
}

UpdateRowResult::UpdateRowResult(
    const OTSResult& result,
    const RowPtr& row,
    const ConsumedCapacity& consumedCapacity)
    : OTSResult(result)
    , mRow(row)
    , mConsumedCapacity(consumedCapacity)
{
} 

UpdateRowResult::~UpdateRowResult()
{
}

const ConsumedCapacity& UpdateRowResult::GetConsumedCapacity() const
{
    return mConsumedCapacity;
}

void UpdateRowResult::SetConsumedCapacity(const ConsumedCapacity& consumedCapacity)
{
    mConsumedCapacity = consumedCapacity;
}

const RowPtr& UpdateRowResult::GetRow() const
{
    return mRow;
}

void UpdateRowResult::SetRow(const RowPtr& rowPtr)
{
    mRow = rowPtr;
}

// RowDeleteChange

RowDeleteChange::RowDeleteChange(const string& tableName)
    : RowChange(tableName)
{
}

RowDeleteChange::RowDeleteChange(
    const string& tableName,
    const PrimaryKey& primaryKey)
    : RowChange(tableName, primaryKey)
{
}

RowDeleteChange::~RowDeleteChange()
{
}

// DeleteRowRequest

DeleteRowRequest::DeleteRowRequest(const RowDeleteChange& rowChange)
    : mRowChange(rowChange)
{
}

DeleteRowRequest::~DeleteRowRequest()
{
}

const RowDeleteChange& DeleteRowRequest::GetRowChange() const
{
    return mRowChange;
}

void DeleteRowRequest::SetRowChange(const RowDeleteChange& rowChange)
{
    mRowChange = rowChange;
}

// DeleteRowResult

DeleteRowResult::DeleteRowResult()
{
}

DeleteRowResult::DeleteRowResult(
    const OTSResult& result,
    const RowPtr& row,
    const ConsumedCapacity& consumedCapacity)
    : OTSResult(result)
    , mRow(row)
    , mConsumedCapacity(consumedCapacity)
{
} 

DeleteRowResult::~DeleteRowResult()
{
}

const ConsumedCapacity& DeleteRowResult::GetConsumedCapacity() const
{
    return mConsumedCapacity;
}

void DeleteRowResult::SetConsumedCapacity(const ConsumedCapacity& consumedCapacity)
{
    mConsumedCapacity = consumedCapacity;
}

const RowPtr& DeleteRowResult::GetRow() const
{
    return mRow;
}

void DeleteRowResult::SetRow(const RowPtr& rowPtr)
{
    mRow = rowPtr;
}


// MultiRowQueryCriteria

MultiRowQueryCriteria::MultiRowQueryCriteria()
    : RowQueryCriteria("")
{
}

MultiRowQueryCriteria::MultiRowQueryCriteria(const string& tableName)
    : RowQueryCriteria(tableName)
{
} 

MultiRowQueryCriteria::~MultiRowQueryCriteria()
{
}

void MultiRowQueryCriteria::AddPrimaryKey(const PrimaryKey& primaryKey)
{
    mRowKeys.push_back(primaryKey);
}

const list<PrimaryKey>& MultiRowQueryCriteria::GetPrimaryKeys() const
{
    return mRowKeys;
}

void MultiRowQueryCriteria::SetPrimaryKeys(const list<PrimaryKey>& primaryKeys)
{
    mRowKeys = primaryKeys;
}

// BatchGetRowRequest

BatchGetRowRequest::BatchGetRowRequest()
{
}

BatchGetRowRequest::~BatchGetRowRequest()
{
}

void BatchGetRowRequest::AddMultiRowQueryCriteria(const MultiRowQueryCriteria& criteria)
{
    mCriteriasMap[criteria.GetTableName()] = criteria;
    //mCriteriasMap.insert(pair<string, MultiRowQueryCriteria>(criteria.GetTableName, criteria));
}

const map<string, MultiRowQueryCriteria>& BatchGetRowRequest::GetCriterias() const
{
    return mCriteriasMap;
}

const MultiRowQueryCriteria& BatchGetRowRequest::GetCriteria(const string& tableName)
{
    typeof(mCriteriasMap.begin()) iter = mCriteriasMap.find(tableName); 
    if (iter == mCriteriasMap.end()) {
        throw OTSClientException("The table is not found.");
    }
    return iter->second;
}

// BatchGetRowResult
        
BatchGetRowResult::RowResult::RowResult(const string& tableName)
    : mIsSuccessful(true)
    , mTableName(tableName)
{
}
    
BatchGetRowResult::RowResult::RowResult(
    const string& tableName,
    const Error& error)
    : mIsSuccessful(false)
    , mTableName(tableName)
    , mError(error)
{
}

BatchGetRowResult::RowResult::RowResult(
    const string& tableName,
    const RowPtr& row,
    const ConsumedCapacity& consumedCapacity)
    : mIsSuccessful(true)
    , mTableName(tableName)
    , mRow(row)
    , mConsumedCapacity(consumedCapacity)
{
}

bool BatchGetRowResult::RowResult::IsSuccessful() const
{
    return mIsSuccessful;
}

void BatchGetRowResult::RowResult::SetIsSuccessful(bool isSuccessful)
{
    mIsSuccessful = isSuccessful;
}

const string& BatchGetRowResult::RowResult::GetTableName() const
{
    return mTableName;
}

void BatchGetRowResult::RowResult::SetTableName(const string& tableName)
{
    mTableName = tableName;
}

const Error& BatchGetRowResult::RowResult::GetError() const
{
    return mError;
}

void BatchGetRowResult::RowResult::SetError(const Error& error)
{
    mIsSuccessful = false;
    mError = error;
}

const RowPtr& BatchGetRowResult::RowResult::GetRow() const
{
    return mRow;
}

void BatchGetRowResult::RowResult::SetRow(const RowPtr& row)
{
    mIsSuccessful = true;
    mRow = row;
}

const ConsumedCapacity& BatchGetRowResult::RowResult::GetConsumedCapacity() const
{
    return mConsumedCapacity;
}

void BatchGetRowResult::RowResult::SetConsumedCapacity(const ConsumedCapacity& consumedCapacity)
{
    mConsumedCapacity = consumedCapacity;
}

BatchGetRowResult::BatchGetRowResult()
{
}

BatchGetRowResult::BatchGetRowResult(const OTSResult& result)
    : OTSResult(result)
{
}

const map<string, list<BatchGetRowResult::RowResult> >& BatchGetRowResult::GetTableToRowResults() const
{
    return mTableToRowResults;
}

const list<BatchGetRowResult::RowResult>& BatchGetRowResult::GetRowResults(const string& tableName) const
{
    typeof(mTableToRowResults.begin()) iter = mTableToRowResults.find(tableName);
    if (iter == mTableToRowResults.end()) {
        throw OTSClientException("The table is not found.");
    }
    return iter->second;
}

void BatchGetRowResult::AddRowResult(const BatchGetRowResult::RowResult& rowResult)
{
    mTableToRowResults[rowResult.GetTableName()].push_back(rowResult);
}

bool BatchGetRowResult::IsAllSuccessful() const
{
    bool isAllSuccessful = true;
    typeof(mTableToRowResults.begin()) tableIter = mTableToRowResults.begin();
    for (; tableIter != mTableToRowResults.end(); ++tableIter) {
        const list<RowResult>& rowResults = tableIter->second;
        typeof(rowResults.begin()) rowIter = rowResults.begin();
        for (; rowIter != rowResults.end(); ++rowIter) {
            if (!rowIter->IsSuccessful()) {
                isAllSuccessful = false;
                return isAllSuccessful;
            }
        }
    }
    return isAllSuccessful;
}

// BatchWriteRowRequest

BatchWriteRowRequest::BatchWriteRowRequest()
{
}

BatchWriteRowRequest::~BatchWriteRowRequest()
{
}

void BatchWriteRowRequest::AddRowPutChange(const RowPutChange& rowPutChange)
{
    mTableToRowPutChanges[rowPutChange.GetTableName()].push_back(rowPutChange);
}

const map<string, list<RowPutChange> >& BatchWriteRowRequest::GetRowPutChanges() const
{
    return mTableToRowPutChanges;
}

void BatchWriteRowRequest::AddRowUpdateChange(const RowUpdateChange& rowUpdateChange)
{
    mTableToRowUpdateChanges[rowUpdateChange.GetTableName()].push_back(rowUpdateChange);
}

const map<string, list<RowUpdateChange> >& BatchWriteRowRequest::GetRowUpdateChanges() const
{
    return mTableToRowUpdateChanges;
}

void BatchWriteRowRequest::AddRowDeleteChange(const RowDeleteChange& rowDeleteChange)
{
    mTableToRowDeleteChanges[rowDeleteChange.GetTableName()].push_back(rowDeleteChange);
}

const map<string, list<RowDeleteChange> >& BatchWriteRowRequest::GetRowDeleteChanges() const
{
    return mTableToRowDeleteChanges;
}

bool BatchWriteRowRequest::IsEmpty() const
{
    if (mTableToRowPutChanges.empty() && 
        mTableToRowUpdateChanges.empty() && 
        mTableToRowDeleteChanges.empty()) {
        return true;
    }
    return false;
}

// BatchWriteRowResult

BatchWriteRowResult::RowResult::RowResult(const string& tableName)
    : mIsSuccessful(false)
    , mTableName(tableName)
{
}

BatchWriteRowResult::RowResult::RowResult(
    const string& tableName,
    const RowPtr& row,
    const Error& error)
    : mIsSuccessful(false)
    , mTableName(tableName)
    , mRow(row)
    , mError(error)
{
}

BatchWriteRowResult::RowResult::RowResult(
    const string& tableName,
    const RowPtr& row,
    const ConsumedCapacity& consumedCapacity)
    : mIsSuccessful(true)
    , mTableName(tableName)
    , mRow(row)
    , mConsumedCapacity(consumedCapacity)
{
}

bool BatchWriteRowResult::RowResult::IsSuccessful() const
{
    return mIsSuccessful;
}

void BatchWriteRowResult::RowResult::SetIsSuccessful(bool isSuccessful)
{
    mIsSuccessful = isSuccessful;
}

const string& BatchWriteRowResult::RowResult::GetTableName() const
{
    return mTableName;
}

void BatchWriteRowResult::RowResult::SetTableName(const string& tableName)
{
    mTableName = tableName;
}

const Error& BatchWriteRowResult::RowResult::GetError() const
{
    return mError;
}

void BatchWriteRowResult::RowResult::SetError(const Error& error)
{
    mError = error;
}

const ConsumedCapacity& BatchWriteRowResult::RowResult::GetConsumedCapacity() const
{
    return mConsumedCapacity;
}

void BatchWriteRowResult::RowResult::SetConsumedCapacity(const ConsumedCapacity& consumedCapacity)
{
    mConsumedCapacity = consumedCapacity;
}

const RowPtr& BatchWriteRowResult::RowResult::GetRow() const
{
    return mRow;
}

void BatchWriteRowResult::RowResult::SetRow(const RowPtr& rowPtr)
{
    mRow = rowPtr;
}


BatchWriteRowResult::BatchWriteRowResult()
{
}

BatchWriteRowResult::BatchWriteRowResult(const OTSResult& result)
    : OTSResult(result)
{
}

BatchWriteRowResult::~BatchWriteRowResult()
{
}

const map<string, list<BatchWriteRowResult::RowResult> >& BatchWriteRowResult::GetTableToPutRowResults() const
{
    return mTableToPutRowResults;
}

const list<BatchWriteRowResult::RowResult>& BatchWriteRowResult::GetPutRowResults(const string& tableName) const
{
    typeof(mTableToPutRowResults.begin()) iter = mTableToPutRowResults.find(tableName);
    if (iter == mTableToPutRowResults.end()) {
        throw OTSClientException("The table is not found.");
    }
    return iter->second;
}

void BatchWriteRowResult::AddPutRowResult(const BatchWriteRowResult::RowResult& rowResult)
{
    mTableToPutRowResults[rowResult.GetTableName()].push_back(rowResult);
}

const map<string, list<BatchWriteRowResult::RowResult> >& BatchWriteRowResult::GetUpdateRowResults() const
{
    return mTableToUpdateRowResults;
}

const list<BatchWriteRowResult::RowResult>& BatchWriteRowResult::GetUpdateRowResults(const string& tableName) const
{
    typeof(mTableToUpdateRowResults.begin()) iter = mTableToUpdateRowResults.find(tableName);
    if (iter == mTableToUpdateRowResults.end()) {
        throw OTSClientException("The table is not found.");
    }
    return iter->second;
}

void BatchWriteRowResult::AddUpdateRowResult(const BatchWriteRowResult::RowResult& rowResult)
{
    mTableToUpdateRowResults[rowResult.GetTableName()].push_back(rowResult);
}

const map<string, list<BatchWriteRowResult::RowResult> >& BatchWriteRowResult::GetDeleteRowResults() const
{
    return mTableToDeleteRowResults;
}

const list<BatchWriteRowResult::RowResult>& BatchWriteRowResult::GetDeleteRowResults(const string& tableName) const
{
    typeof(mTableToDeleteRowResults.begin()) iter = mTableToDeleteRowResults.find(tableName);
    if (iter == mTableToDeleteRowResults.end()) {
        throw OTSClientException("The table is not found.");
    }
    return iter->second;
}

void BatchWriteRowResult::AddDeleteRowResult(const BatchWriteRowResult::RowResult& rowResult)
{
    mTableToDeleteRowResults[rowResult.GetTableName()].push_back(rowResult);
}

bool BatchWriteRowResult::IsAllSuccessful() const
{
    if (HasRowFailed(mTableToPutRowResults) || 
        HasRowFailed(mTableToUpdateRowResults) ||
        HasRowFailed(mTableToDeleteRowResults)) {
        return false;
    }
    return true;
}

bool BatchWriteRowResult::HasRowFailed(const map<string, list<BatchWriteRowResult::RowResult> >& tableToRowResults) const
{
    bool hasRowFailed = false;
    typeof(tableToRowResults.begin()) tableIter = tableToRowResults.begin();
    for (; tableIter != tableToRowResults.end(); ++tableIter) {
        const list<BatchWriteRowResult::RowResult>& rowResults = tableIter->second;
        typeof(rowResults.begin()) rowIter = rowResults.begin();
        for (; rowIter != rowResults.end(); ++rowIter) {
            if (!rowIter->IsSuccessful()) {
                hasRowFailed = true;
                return hasRowFailed;
            }
        }
    }
    return hasRowFailed;
}

// RangeRowQueryCriteria

RangeRowQueryCriteria::RangeRowQueryCriteria(const string& tableName)
    : RowQueryCriteria(tableName)
{
}

RangeRowQueryCriteria::~RangeRowQueryCriteria()
{
}

bool RangeRowQueryCriteria::HasLimit() const
{
    return mLimit.HasValue();
}

int RangeRowQueryCriteria::GetLimit() const
{
    return mLimit.GetValue();
}

void RangeRowQueryCriteria::SetLimit(int limit)
{
    mLimit.SetValue(limit);
}

bool RangeRowQueryCriteria::HasDirection() const
{
    return mDirection.HasValue();
}

const RangeRowQueryCriteria::Direction RangeRowQueryCriteria::GetDirection() const
{
    return mDirection.GetValue();
}

void RangeRowQueryCriteria::SetDirection(RangeRowQueryCriteria::Direction direction)
{
    mDirection.SetValue(direction);
}

const PrimaryKey& RangeRowQueryCriteria::GetInclusiveStartPrimaryKey() const
{
    return mInclusiveStartPrimaryKey;
}

void RangeRowQueryCriteria::SetInclusiveStartPrimaryKey(const PrimaryKey& primaryKey)
{
    mInclusiveStartPrimaryKey = primaryKey;
}

const PrimaryKey& RangeRowQueryCriteria::GetExclusiveEndPrimaryKey() const
{
    return mExclusiveEndPrimaryKey;
}

void RangeRowQueryCriteria::SetExclusiveEndPrimaryKey(const PrimaryKey& primaryKey)
{
    mExclusiveEndPrimaryKey = primaryKey;
}

// GetRangeRequest

GetRangeRequest::GetRangeRequest(const RangeRowQueryCriteria& rangeRowQueryCriteria)
    : mRowQueryCriteria(rangeRowQueryCriteria)
{
}

GetRangeRequest::~GetRangeRequest()
{
}

const RangeRowQueryCriteria& GetRangeRequest::GetRowQueryCriteria() const
{
    return mRowQueryCriteria;
}

void GetRangeRequest::SetRowQueryCriteria(const RangeRowQueryCriteria& rangeRowQueryCriteria)
{
    mRowQueryCriteria = rangeRowQueryCriteria;
}

void GetRangeRequest::SetInclusiveStartPrimaryKey(const PrimaryKey& startPrimaryKey)
{
    mRowQueryCriteria.SetInclusiveStartPrimaryKey(startPrimaryKey);
}

// GetRangeResult

GetRangeResult::GetRangeResult()
{
}

GetRangeResult::GetRangeResult(const OTSResult& result)
    : OTSResult(result)
{
}

GetRangeResult::~GetRangeResult()
{
}

const ConsumedCapacity& GetRangeResult::GetConsumedCapacity() const
{
    return mConsumedCapacity;
}

void GetRangeResult::SetConsumedCapacity(const ConsumedCapacity& consumedCapacity)
{
    mConsumedCapacity = consumedCapacity;
}

const list<RowPtr>& GetRangeResult::GetRows() const
{
    return mRows;
}

void GetRangeResult::SetRows(const list<RowPtr>& rows)
{
    mRows = rows;
}

bool GetRangeResult::HasNextStartPrimaryKey() const
{
    return mNextStartPrimaryKey.HasValue();
}

const PrimaryKey& GetRangeResult::GetNextStartPrimaryKey() const
{
    if (!mNextStartPrimaryKey.HasValue()) {
        throw OTSClientException("No NextStartPrimaryKey in result.");
    }
    return mNextStartPrimaryKey.GetValue();
}

void GetRangeResult::SetNextStartPrimaryKey(const PrimaryKey& nextStartPrimaryKey)
{
    mNextStartPrimaryKey.SetValue(nextStartPrimaryKey);
}


} // end of tablestore
} // end of aliyun
