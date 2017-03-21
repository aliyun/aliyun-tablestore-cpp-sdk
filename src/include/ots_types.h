/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#ifndef OTS_TYPES_H
#define OTS_TYPES_H

#include "ots_common.h"
#include <tr1/memory>
#include <map>
#include <list>
#include <deque>
#include <string>
#include <cstring>
#include <stdint.h>

namespace aliyun {
namespace tablestore {

template<typename Elem>
class IVector
{
public:
    typedef Elem ElemType;

    virtual ~IVector() {}

    virtual int64_t Size() const =0;
    virtual const Elem& Get(int64_t idx) const =0;
    virtual Elem* MutableGet(int64_t idx) =0;
    virtual Elem* Append() =0;
};

template<typename Elem>
class DequeBasedVector : public IVector<Elem>
{
public:
    typedef typename IVector<Elem>::ElemType ElemType;

    int64_t Size() const
    {
        return mElems.size();
    }

    const Elem& Get(int64_t idx) const
    {
        return mElems.at(idx);
    }

    Elem* MutableGet(int64_t idx)
    {
        return &(mElems.at(idx));
    }

    Elem* Append()
    {
        mElems.push_back(Elem());
        return &(mElems.back());
    }
    
private:
    std::deque<Elem> mElems;
};

/**
 * @brief Error
 *
 * 错误信息。
 */
class Error
{
public:

    Error();

    Error(const std::string& code, const std::string& message);
    
    const std::string& GetCode() const;

    void SetCode(const std::string& code);

    const std::string& GetMessage() const;

    void SetMessage(const std::string& message);

private:
    
    std::string mCode;

    std::string mMessage;
};

/**
 * @brief Blob
 *
 * 二进制数据。
 */
class Blob
{
public:

    Blob();

    Blob(const std::string& value);

    Blob(const char* data, size_t size);

    const std::string& GetValue() const;

    const char* Data() const;

    size_t Size() const;

private:

    std::string mValue;
};

inline bool operator==(const Blob& a, const Blob& b)
{
    size_t asz = a.Size();
    size_t bsz = b.Size();
    if (asz != bsz) {
        return false;
    }
    int c = ::memcmp(a.Data(), b.Data(), asz);
    return c == 0;
}

inline bool operator!=(const Blob& a, const Blob& b)
{
    return !(a == b);
}

/**
 * @brief PrimaryKeyType
 *
 * 主键的类型。
 */
enum PrimaryKeyType
{
    PKT_INTEGER = 1,
    PKT_STRING  = 2,
    PKT_BINARY  = 3
};

namespace impl {

template<>
class ToStringImpl<PrimaryKeyType>
{
public:
    void operator()(std::string* out, PrimaryKeyType pkt) const
    {
        switch(pkt) {
        case PKT_INTEGER:
            out->append("PKT_INTEGER");
            break;
        case PKT_STRING:
            out->append("PKT_STRING");
            break;
        case PKT_BINARY:
            out->append("PKT_BINARY");
            break;
        }
    }
};

} // namespace impl


enum PrimaryKeyOption
{
    PKO_AUTO_INCREMENT = 1
};

namespace impl {

template<>
class ToStringImpl<PrimaryKeyOption>
{
public:
    void operator()(std::string* out, PrimaryKeyOption opt) const
    {
        switch(opt) {
        case PKO_AUTO_INCREMENT:
            out->append("auto-incr");
            break;
        }
    }
};

} // namespace impl

/**
 * @brief PrimaryKeySchema
 *
 * 主键列的定义。
 */
class PrimaryKeySchema
{
public:
    PrimaryKeySchema()
      : mType(PKT_INTEGER)
    {}

    PrimaryKeySchema(
        const std::string& name,
        PrimaryKeyType type);

    PrimaryKeySchema(
        const std::string& name,
        PrimaryKeyType type,
        PrimaryKeyOption option);

    const std::string& GetName() const;

    void SetName(const std::string& name);

    PrimaryKeyType GetType() const;

    void SetType(PrimaryKeyType type);

    bool HasOption() const;

    void SetOption(const PrimaryKeyOption option);

    const PrimaryKeyOption GetOption() const;

private:
    std::string mName;
    PrimaryKeyType mType;
    OptionalValue<PrimaryKeyOption> mOption;
};

namespace impl {

template<>
class ToStringImpl<PrimaryKeySchema>
{
public:
    void operator()(std::string* out, const PrimaryKeySchema& schema) const;
};

} // namespace impl

/**
 * @brief InfMin
 *
 * 无限小值，仅用于范围查询。
 */
class InfMin
{
};

/**
 * @brief InfMax
 *
 * 无限大值，仅用于范围查询。
 */
class InfMax
{
};

/**
 * @brief AutoIncrement
 *
 * 自动递增，仅用于PutRowRequest。
 */
class AutoIncrement
{
};

/**
 * @brief PrimaryKeyValue
 *
 * 主键列的值。
 */
class PrimaryKeyValue
{
public:

    explicit PrimaryKeyValue(int value);

    explicit PrimaryKeyValue(int64_t value);

    explicit PrimaryKeyValue(const std::string& value);

    explicit PrimaryKeyValue(const char* value);

    explicit PrimaryKeyValue(const Blob& value);

    explicit PrimaryKeyValue(const InfMin& value);

    explicit PrimaryKeyValue(const InfMax& value);

    explicit PrimaryKeyValue(const AutoIncrement& value);

    PrimaryKeyType GetType() const;

    int64_t AsInteger() const;

    void SetValue(int value);

    void SetValue(int64_t value);

    const std::string& AsString() const;

    void SetValue(const std::string& value);

    void SetValue(const char* value);

    const Blob& AsBinary() const;

    void SetValue(const Blob& value);

    void SetValue(const InfMin& value);

    void SetValue(const InfMax& value);

    void SetValue(const AutoIncrement& value);

    bool IsInfMin() const;

    bool IsInfMax() const;

    bool IsPlaceholderForAutoIncrement() const;

private:

    PrimaryKeyType mType;

    std::string mStringValue;

    int64_t mIntegerValue;

    Blob mBinaryValue;

    bool mIsInfMin;

    bool mIsInfMax;

    bool mIsPlaceholderForAutoIncr;
};

namespace impl {

template<>
class ToStringImpl<PrimaryKeyValue>
{
public:
    void operator()(std::string* out, const PrimaryKeyValue& v) const;
};

} // namespace impl

/**
 * @brief PrimaryKeyColumn
 *
 * 主键列。
 */
class PrimaryKeyColumn
{
public:

    PrimaryKeyColumn(
        const std::string& name,
        const PrimaryKeyValue& value);

    const std::string& GetName() const;

    void SetName(const std::string& name);

    const PrimaryKeyValue& GetValue() const;

    void SetValue(const PrimaryKeyValue& value);

private:

    std::string mName;

    PrimaryKeyValue mValue;
};

/**
 * @brief PrimaryKey
 *
 * 主键。
 */
class PrimaryKey
{
public:

    void AddPrimaryKeyColumn(const PrimaryKeyColumn& column);
    void AddPrimaryKeyColumn(
        const std::string& name,
        const PrimaryKeyValue& value);

    size_t GetPrimaryKeyColumnsSize() const;

    const std::list<PrimaryKeyColumn>& GetPrimaryKeyColumns() const;
    const PrimaryKeyColumn& GetColumn(size_t idx) const;
    PrimaryKeyColumn& GetColumn(size_t idx);

private:
    std::list<PrimaryKeyColumn> mPrimaryKeyColumns;
};

namespace impl {

template<>
class ToStringImpl<PrimaryKey>
{
public:
    void operator()(std::string*, const PrimaryKey&) const;
};

} // namespace impl

/**
 * @brief TableMeta
 *
 * 表的结构。
 */
class TableMeta
{
public:

    TableMeta();

    TableMeta(const std::string& tableName);

    void SetTableName(const std::string& tableName);

    const std::string& GetTableName() const;

    void AddPrimaryKeySchema(const std::string& name, PrimaryKeyType type);

    void AddPrimaryKeySchema(const std::string& name, PrimaryKeyType type, PrimaryKeyOption option);

    void AddPrimaryKeySchema(const PrimaryKeySchema& key);

    void AddPrimaryKeySchemas(const std::list<PrimaryKeySchema>& pks);

    const std::list<PrimaryKeySchema>& GetPrimaryKeySchemas() const;

private:

    std::string mTableName;

    std::list<PrimaryKeySchema> mPrimaryKey;
};

/**
 * @brief BloomFilterType
 *
 * BloomFilter类型。
 */
enum BloomFilterType
{
    BFT_NONE = 1,
    BFT_CELL = 2,
    BFT_ROW  = 3
};

/**
 * @brief TableOptions
 *
 * 表的选项。
 */
class TableOptions
{
public:

    TableOptions();

    TableOptions(
        int timeToLive,
        int maxVersions);

    TableOptions(
        int timeToLive,
        int maxVersions,
        BloomFilterType bloomFilterType);

    TableOptions(
        int timeToLive,
        int maxVersions,
        BloomFilterType bloomFilterType,
        int blockSize);

    TableOptions(
        int timeToLive,
        int maxVersions,
        BloomFilterType bloomFilterType,
        int blockSize,
        int64_t deviationCellVersionInSec);

    int GetTimeToLive() const;
    void SetTimeToLive(int timeToLive);
    bool HasTimeToLive() const;

    int GetMaxVersions() const;
    void SetMaxVersions(int maxVersions);
    bool HasMaxVersions() const;

    BloomFilterType GetBloomFilterType() const;
    void SetBloomFilterType(BloomFilterType bloomFilterType);
    bool HasBloomFilterType() const;

    int GetBlockSize() const;
    void SetBlockSize(int blockSize);
    bool HasBlockSize() const;

    int64_t GetDeviationCellVersionInSec() const;
    void SetDeviationCellVersionInSec(int64_t deviationCellVersionInSec);
    bool HasDeviationCellVersionInSec() const;

private:
    
    OptionalValue<int> mTimeToLive;
    OptionalValue<int> mMaxVersions;
    OptionalValue<BloomFilterType> mBloomFilterType;
    OptionalValue<int> mBlockSize;
    OptionalValue<int64_t> mDeviationCellVersionInSec;
};

/**
 * @brief CapacityUnit
 *
 * 表的预留吞吐量配置。
 */
class CapacityUnit
{
public:

    CapacityUnit();

    CapacityUnit(
        int readCapacityUnit,
        int writeCapacityUnit);

    int GetReadCapacityUnit() const;

    void SetReadCapacityUnit(int readCapacityUnit);

    bool HasReadCapacityUnit() const;

    int GetWriteCapacityUnit() const;

    void SetWriteCapacityUnit(int writeCapacityUnit);

    bool HasWriteCapacityUnit() const;
    
    bool HasSetValue() const;

private:

    OptionalValue<int> mReadCapacityUnit;

    OptionalValue<int> mWriteCapacityUnit;
};

/**
 * @brief ReservedThroughput
 *
 * 表的预留吞吐量。
 */
class ReservedThroughput
{
public:

    ReservedThroughput();

    ReservedThroughput(
        int readCapacityUnit,
        int writeCapacityUnit);

    ReservedThroughput(const CapacityUnit& capacityUnit);

    const CapacityUnit& GetCapacityUnit() const;

    void SetCapacityUnit(const CapacityUnit& capacityUnit);

private:

    CapacityUnit mCapacityUnit;
};

/**
 * @brief PartitionRange
 *
 * 表的分区。
 */
class PartitionRange
{
public:

    PartitionRange(
        const PrimaryKeyValue& begin,
        const PrimaryKeyValue& end);

    const PrimaryKeyValue& GetBegin() const;

    void SetBegin(const PrimaryKeyValue& begin);

    const PrimaryKeyValue& GetEnd() const;

    void SetEnd(const PrimaryKeyValue& end);

private:
    
    PrimaryKeyValue mBegin;

    PrimaryKeyValue mEnd;
};

/**
 * @brief ReservedThroughputDetails
 *
 * 表的预留吞吐量详情信息。
 */
class ReservedThroughputDetails
{
public:
    ReservedThroughputDetails();

    ReservedThroughputDetails(
        const CapacityUnit& capacityUnit,
        int64_t lastIncreaseTime,
        int64_t lastDecreaseTime);

    const CapacityUnit& GetCapacityUnit() const;
    void SetCapacityUnit(const CapacityUnit& capacityUnit);

    int64_t GetLastIncreaseTime() const;
    void SetLastIncreaseTime(int64_t lastIncreaseTime);

    int64_t GetLastDecreaseTime() const;
    void SetLastDecreaseTime(int64_t lastDecreaseTime);

private:
    CapacityUnit mCapacityUnit;
    int64_t mLastIncreaseTime;
    int64_t mLastDecreaseTime;
};

/**
 * @brief TableStatus
 *
 * 表的状态。
 */
enum TableStatus
{
    ACTIVE      = 1,
    INACTIVE    = 2,
    LOADING     = 3,
    UNLOADING   = 4,
    UPDATING    = 5
};

/**
 * @brief ColumnType
 *
 * 属性列类型。
 */
enum ColumnType
{
    CT_STRING,
    CT_INTEGER,
    CT_BINARY,
    CT_BOOLEAN,
    CT_DOUBLE
};

enum ReturnType
{
    RT_NONE = 0,
    RT_PK = 1,
};

/**
 * @brief ColumnValue
 *
 * 属性列值。
 */
class ColumnValue
{
public:

    ColumnValue();

    explicit ColumnValue(int value);

    explicit ColumnValue(int64_t value);

    explicit ColumnValue(const std::string& value);

    explicit ColumnValue(const char* value);

    explicit ColumnValue(const Blob& value);

    explicit ColumnValue(bool value);

    explicit ColumnValue(double value);

    ColumnType GetType() const;

    int64_t AsInteger() const;

    void SetValue(int value);

    void SetValue(int64_t value);

    const std::string& AsString() const;

    void SetValue(const std::string& value);

    void SetValue(const char* value);

    const Blob& AsBinary() const;

    void SetValue(const Blob& value);

    bool AsBoolean() const;

    void SetValue(bool value);

    double AsDouble() const;

    void SetValue(double value);

private:

    bool mHasSetValue;

    ColumnType mType;

    std::string mStringValue;

    int64_t mIntegerValue;

    bool mBooleanValue;

    double mDoubleValue;

    Blob mBinaryValue;
};

namespace impl {

template<>
class ToStringImpl<ColumnValue>
{
public:
    void operator()(std::string* out, const ColumnValue& v) const;
};

} // namespace impl

/**
 * @brief Column
 *
 * 一列数据。
 */
class Column
{
public:

    Column(const std::string& name);

    Column(
        const std::string& name,
        const ColumnValue& value);

    Column(
        const std::string& name,
        const ColumnValue& value,
        int64_t ts);

    const std::string& GetName() const;

    void SetName(const std::string& name);

    bool HasValue() const;

    const ColumnValue& GetValue() const;
    
    void SetValue(const ColumnValue& value);

    bool HasTimestamp() const;

    int64_t GetTimestamp() const;

    void SetTimestamp(int64_t ts);

private:

    std::string mName;

    OptionalValue<ColumnValue> mValue;

    OptionalValue<int64_t> mTimestamp;
};

/**
 * @brief ConsumedCapacity
 *
 * 消耗的服务能力单元。
 */
class ConsumedCapacity
{
public:

    ConsumedCapacity();

    ConsumedCapacity(const CapacityUnit& capacityUnit);

    const CapacityUnit& GetCapacityUnit() const;

    void SetCapacityUnit(const CapacityUnit& capacityUnit);

private:

    CapacityUnit mCapacityUnit;
};

/**
 * @brief Row
 *
 * 一行数据。
 */
class Row
{
public:

    Row();

    Row(
        const PrimaryKey& mPrimaryKey,
        const std::list<Column>& columns);

    const PrimaryKey& GetPrimaryKey() const;

    void SetPrimaryKey(const PrimaryKey& primaryKey);

    void AddPrimaryKeyColumn(const PrimaryKeyColumn& column);

    void AddPrimaryKeyColumn(
        const std::string& name,
        const PrimaryKeyValue& value);

    const std::list<Column>& GetColumns() const;

    void SetColumns(const std::list<Column>& columns);

    void AddColumn(const Column& column);

    void AddColumn(const std::string& name, const ColumnValue& value);

private:

    PrimaryKey mPrimaryKey;

    std::list<Column> mColumns;
};
typedef std::tr1::shared_ptr<Row> RowPtr;

/**
 * @brief TimeRange
 *
 * 时间范围条件。
 */
class TimeRange
{
public:

    TimeRange();

    TimeRange(int64_t minStamp, int64_t maxStamp);

    int64_t GetMinStamp() const;

    void SetMinStamp(int64_t minStamp);

    int64_t GetMaxStamp() const;

    void SetMaxStamp(int64_t maxStamp);

    bool WithinTimeRange(int64_t ts) const;

private:

    int64_t mMinStamp;

    int64_t mMaxStamp;
};

struct Split
{
    /**
     * The (inclusive) lower bound of the split, as long as the columns of primary key.
     */
    std::tr1::shared_ptr<PrimaryKey> mLowerBound;

    /**
     * The (exclusive) upper bound of the split, as long as the columns of primary key.
     */
    std::tr1::shared_ptr<PrimaryKey> mUpperBound;

    /**
     * A hint of the location where the split lies in.
     *
     * If a location is not suitable to be seen, it will be empty.
     */
    std::string mLocation;
};
    
} // end of tablestore
} // end of aliyun

#endif
