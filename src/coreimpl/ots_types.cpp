/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#include "ots/ots_types.h"
#include "ots/ots_exception.h"
#include <sstream>
#include <limits>
#include <utility>
#include <cassert>

using namespace std;

namespace aliyun {
namespace tablestore {

// Error

Error::Error()
{
}

Error::Error(const string& code, const string& message)
    : mCode(code)
    , mMessage(message)
{
}
    
const string& Error::GetCode() const
{
    return mCode;
}

void Error::SetCode(const string& code)
{
    mCode = code;
}

const string& Error::GetMessage() const
{
    return mMessage;
}

void Error::SetMessage(const string& message)
{
    mMessage = message;
}

// Blob

Blob::Blob()
{
}

Blob::Blob(const string& value)
    : mValue(value)
{
}

Blob::Blob(const char* data, size_t size)
{
    mValue.assign(data, size);
}

const string& Blob::GetValue() const
{
    return mValue;
}

const char* Blob::Data() const
{
    return mValue.c_str();
}

size_t Blob::Size() const
{
    return mValue.size();
}

// PrimaryKeySchema

PrimaryKeySchema::PrimaryKeySchema(
    const string& name,
    PrimaryKeyType type)
    : mName(name)
    , mType(type)
{
}

PrimaryKeySchema::PrimaryKeySchema(
        const std::string& name,
        PrimaryKeyType type,
        PrimaryKeyOption option)
    : mName(name)
    , mType(type)
    , mOption(option)
{
}

const string& PrimaryKeySchema::GetName() const
{
    return mName;
}

void PrimaryKeySchema::SetName(const string& name)
{
    mName = name;
}

PrimaryKeyType PrimaryKeySchema::GetType() const
{
    return mType;
}

void PrimaryKeySchema::SetType(PrimaryKeyType type)
{
    mType = type;
}

bool PrimaryKeySchema::HasOption() const
{
    return mOption.HasValue();
}

void PrimaryKeySchema::SetOption(const PrimaryKeyOption option)
{
    mOption.SetValue(option);
}

const PrimaryKeyOption PrimaryKeySchema::GetOption() const
{
    if (!mOption.HasValue()) {
        throw OTSClientException("PrimaryKeyOption is not set.");
    }
    return mOption.GetValue();
}

namespace impl {

void ToStringImpl<PrimaryKeySchema>::operator()(
    string* out,
    const PrimaryKeySchema& schema) const
{
    out->append("{\"name\":\"");
    out->append(schema.GetName());
    out->append("\",\"type\":\"");
    ToString(out, schema.GetType());
    if (schema.HasOption()) {
        out->append("\",\"option\":\"");
        ToString(out, schema.GetOption());
        out->append("\"}");
    } else {
        out->append("\"}");
    }
}

} // namespace impl

// PrimaryKeyValue

PrimaryKeyValue::PrimaryKeyValue(int value)
    : mType(PKT_INTEGER)
    , mIntegerValue(value)
    , mIsInfMin(false)
    , mIsInfMax(false)
    , mIsPlaceholderForAutoIncr(false)
{
}

PrimaryKeyValue::PrimaryKeyValue(int64_t value)
    : mType(PKT_INTEGER)
    , mIntegerValue(value)
    , mIsInfMin(false)
    , mIsInfMax(false)
    , mIsPlaceholderForAutoIncr(false)
{
}

PrimaryKeyValue::PrimaryKeyValue(const string& value)
    : mType(PKT_STRING)
    , mStringValue(value)
    , mIsInfMin(false)
    , mIsInfMax(false)
    , mIsPlaceholderForAutoIncr(false)
{
}

PrimaryKeyValue::PrimaryKeyValue(const char* value)
    : mType(PKT_STRING)
    , mStringValue(value)
    , mIsInfMin(false)
    , mIsInfMax(false)
    , mIsPlaceholderForAutoIncr(false)
{
}

PrimaryKeyValue::PrimaryKeyValue(const Blob& value)
    : mType(PKT_BINARY)
    , mBinaryValue(value)
    , mIsInfMin(false)
    , mIsInfMax(false)
    , mIsPlaceholderForAutoIncr(false)
{
}

PrimaryKeyValue::PrimaryKeyValue(const InfMin& value)
    : mIsInfMin(true)
    , mIsInfMax(false)
    , mIsPlaceholderForAutoIncr(false)
{
}

PrimaryKeyValue::PrimaryKeyValue(const InfMax& value)
    : mIsInfMin(false)
    , mIsInfMax(true)
    , mIsPlaceholderForAutoIncr(false)
{
}

PrimaryKeyValue::PrimaryKeyValue(const AutoIncrement& value)
  : mIsInfMin(false)
  , mIsInfMax(false)
  , mIsPlaceholderForAutoIncr(true)
{
}

PrimaryKeyType PrimaryKeyValue::GetType() const
{
    if (mIsInfMin || mIsInfMax || mIsPlaceholderForAutoIncr) {
        throw OTSClientException("Primary key type is InfMin or InfMax or AutoIncrement.");
    }
    return mType;
}

int64_t PrimaryKeyValue::AsInteger() const
{
    if (mIsInfMin || mIsInfMax || mIsPlaceholderForAutoIncr) {
        throw OTSClientException("Primary key type is InfMin or InfMax or AutoIncrement.");
    }
    if (mType != PKT_INTEGER) {
        throw OTSClientException("PrimaryKeyValue is not INTEGER type.");
    }
    return mIntegerValue;
}

void PrimaryKeyValue::SetValue(int value)
{
    mType = PKT_INTEGER;
    mIntegerValue = value;
    mIsInfMin = false;
    mIsInfMax = false;
    mIsPlaceholderForAutoIncr = false;
}

void PrimaryKeyValue::SetValue(int64_t value)
{
    mType = PKT_INTEGER;
    mIntegerValue = value;
    mIsInfMin = false;
    mIsInfMax = false;
    mIsPlaceholderForAutoIncr = false;
}

const string& PrimaryKeyValue::AsString() const
{
    if (mIsInfMin || mIsInfMax || mIsPlaceholderForAutoIncr) {
        throw OTSClientException("Primary key type is InfMin or InfMax or AutoIncrement.");
    }
    if (mType != PKT_STRING) {
        throw OTSClientException("PrimaryKeyValue is not STRING type.");
    }
    return mStringValue;
}

void PrimaryKeyValue::SetValue(const string& value)
{
    mType = PKT_STRING;
    mStringValue.assign(value);
    mIsInfMin = false;
    mIsInfMax = false;
    mIsPlaceholderForAutoIncr = false;
}

void PrimaryKeyValue::SetValue(const char* value)
{
    mType = PKT_STRING;
    mStringValue.assign(value);
    mIsInfMin = false;
    mIsInfMax = false;
    mIsPlaceholderForAutoIncr = false;
}

const Blob& PrimaryKeyValue::AsBinary() const
{
    if (mIsInfMin || mIsInfMax || mIsPlaceholderForAutoIncr) {
        throw OTSClientException("Primary key type is InfMin or InfMax or AutoIncrement.");
    }
    if (mType != PKT_BINARY) {
        throw OTSClientException("PrimaryKeyValue is not BINARY type.");
    }
    return mBinaryValue;
}

void PrimaryKeyValue::SetValue(const Blob& value)
{
    mType = PKT_BINARY;
    mBinaryValue = value;
    mIsInfMin = false;
    mIsInfMax = false;
    mIsPlaceholderForAutoIncr = false;
}

void PrimaryKeyValue::SetValue(const InfMin& value)
{
    mIsInfMin = true;
    mIsInfMax = false;
    mIsPlaceholderForAutoIncr = false;
}

void PrimaryKeyValue::SetValue(const InfMax& value)
{
    mIsInfMin = false;
    mIsInfMax = true;
    mIsPlaceholderForAutoIncr = false;
}

void PrimaryKeyValue::SetValue(const AutoIncrement& value)
{
    mIsInfMin = false;
    mIsInfMax = false;
    mIsPlaceholderForAutoIncr = true;
}

bool PrimaryKeyValue::IsInfMin() const
{
    return mIsInfMin;
}

bool PrimaryKeyValue::IsInfMax() const
{
    return mIsInfMax;
}

namespace {

void EscapedString(string* out, const string& s)
{
    out->push_back('"');
    for(string::const_iterator i = s.begin();
        i != s.end();
        ++i)
    {
        char c = *i;
        if (c == '"') {
            out->append("\\\"");
        } else {
            out->push_back(c);
        }
    }
    out->push_back('"');
}

void PositiveToStr(string* out, int64_t x)
{
    static const int64_t kRadix = 10;
    static const char kAlphabet[] = "0123456789";
    assert(x >= 0);

    if (x == 0) {
        out->push_back('0');
        return;
    }

    int64_t initPos = out->size();

    for(; x > 0; x /= kRadix) {
        out->push_back(kAlphabet[x % kRadix]);
    }

    char* dat = const_cast<char*>(out->data());
    for(int64_t l = initPos, r = out->size() - 1; l < r; ++l, --r) {
        ::std::swap(dat[l], dat[r]);
    }
}

void IntToStr(string* out, int64_t x)
{
    if (x >= 0) {
        PositiveToStr(out, x);
    } else {
        out->push_back('-');
        PositiveToStr(out, -x);
    }
}

inline void Hex(string* out, uint8_t x)
{
    static const char kAlphabet[] = "0123456789ABCDEF";
    out->push_back(kAlphabet[x >> 4]);
    out->push_back(kAlphabet[x & 0xF]);
}

} // namespace

namespace impl {

void ToStringImpl<PrimaryKeyValue>::operator()(string* out, const PrimaryKeyValue& v) const
{
    if (v.IsInfMin()) {
        out->append("-inf");
    } else if (v.IsInfMax()) {
        out->append("+inf");
    } else if (v.GetType() == PKT_STRING) {
        EscapedString(out, v.AsString());
    } else if (v.GetType() == PKT_INTEGER) {
        IntToStr(out, v.AsInteger());
    } else if (v.GetType() == PKT_BINARY) {
        out->append("b\"");
        const Blob& b = v.AsBinary();
        const char* s = b.Data();
        for(int64_t i = 0, sz = b.Size(); i < sz; ++i) {
            Hex(out, s[i]);
        }
        out->append("\"");
    } else {
        assert(false);
    }
}

} // namespace impl

bool PrimaryKeyValue::IsPlaceholderForAutoIncrement() const
{
    return mIsPlaceholderForAutoIncr;
}

// PrimaryKeyColumn

PrimaryKeyColumn::PrimaryKeyColumn(
    const string& name,
    const PrimaryKeyValue& value)
    : mName(name)
    , mValue(value)
{
}

const string& PrimaryKeyColumn::GetName() const
{
    return mName;
}

void PrimaryKeyColumn::SetName(const string& name)
{
    mName = name;
}

const PrimaryKeyValue& PrimaryKeyColumn::GetValue() const
{
    return mValue;
}

void PrimaryKeyColumn::SetValue(const PrimaryKeyValue& value)
{
    mValue = value;
}

// PrimaryKey

void PrimaryKey::AddPrimaryKeyColumn(const PrimaryKeyColumn& column)
{
    mPrimaryKeyColumns.push_back(column);
}

void PrimaryKey::AddPrimaryKeyColumn(
    const string& name,
    const PrimaryKeyValue& value)
{
    mPrimaryKeyColumns.push_back(PrimaryKeyColumn(name, value));
}

size_t PrimaryKey::GetPrimaryKeyColumnsSize() const
{
    return mPrimaryKeyColumns.size();
}

const list<PrimaryKeyColumn>& PrimaryKey::GetPrimaryKeyColumns() const
{
    return mPrimaryKeyColumns;
}

const PrimaryKeyColumn& PrimaryKey::GetColumn(size_t idx) const
{
    size_t index = 0;
    list<PrimaryKeyColumn>::const_iterator iter = mPrimaryKeyColumns.begin();
    for (; iter != mPrimaryKeyColumns.end(); ++iter, ++index) {
        if (index == idx) {
            return *iter;
        }
    }

    throw OTSClientException("out of range of PrimaryKey");
}

PrimaryKeyColumn& PrimaryKey::GetColumn(size_t idx)
{
    size_t index = 0;
    list<PrimaryKeyColumn>::iterator iter = mPrimaryKeyColumns.begin();
    for (; iter != mPrimaryKeyColumns.end(); ++iter, ++index) {
        if (index == idx) {
            return *iter;
        }
    }

    throw OTSClientException("out of range of PrimaryKey");
}

namespace impl {

void ToStringImpl<PrimaryKey>::operator()(string* out, const PrimaryKey& v) const
{
    out->push_back('{');
    for(size_t i = 0, sz = v.GetPrimaryKeyColumnsSize(); i < sz; ++i)
    {
        if (i > 0) {
            out->push_back(',');
        }
        const PrimaryKeyColumn& c = v.GetColumn(i);
        EscapedString(out, c.GetName());
        out->push_back(':');
        ToString(out, c.GetValue());
    }
    out->push_back('}');
}

} // namespace impl

// TableMeta

TableMeta::TableMeta()
{
}

TableMeta::TableMeta(const string& tableName)
    : mTableName(tableName)
{
}

void TableMeta::SetTableName(const string& tableName)
{
    mTableName = tableName;
}

const string& TableMeta::GetTableName() const
{
    return mTableName;
}

void TableMeta::AddPrimaryKeySchema(const string& name, PrimaryKeyType type)
{
    mPrimaryKey.push_back(PrimaryKeySchema(name, type));
}

void TableMeta::AddPrimaryKeySchema(const string& name, PrimaryKeyType type, PrimaryKeyOption option)
{
    mPrimaryKey.push_back(PrimaryKeySchema(name, type, option));
}

void TableMeta::AddPrimaryKeySchema(const PrimaryKeySchema& key)
{
    mPrimaryKey.push_back(key);
}

void TableMeta::AddPrimaryKeySchemas(const list<PrimaryKeySchema>& pks)
{
    for (typeof(pks.begin()) iter = pks.begin(); iter != pks.end(); ++iter) {
        mPrimaryKey.push_back(*iter);
    }
}

const list<PrimaryKeySchema>& TableMeta::GetPrimaryKeySchemas() const
{
    return mPrimaryKey;
}

// TableOptions

TableOptions::TableOptions()
{
}

TableOptions::TableOptions(
    int timeToLive,
    int maxVersions)
    : mTimeToLive(timeToLive)
    , mMaxVersions(maxVersions)
{
}

TableOptions::TableOptions(
    int timeToLive,
    int maxVersions,
    BloomFilterType bloomFilterType)
    : mTimeToLive(timeToLive)
    , mMaxVersions(maxVersions)
    , mBloomFilterType(bloomFilterType)
{
}

TableOptions::TableOptions(
    int timeToLive,
    int maxVersions,
    BloomFilterType bloomFilterType,
    int blockSize)
    : mTimeToLive(timeToLive)
    , mMaxVersions(maxVersions)
    , mBloomFilterType(bloomFilterType)
    , mBlockSize(blockSize)
{
}

TableOptions::TableOptions(
        int timeToLive,
        int maxVersions,
        BloomFilterType bloomFilterType,
        int blockSize,
        int64_t deviationCellVersionInSec)
    : mTimeToLive(timeToLive)
    , mMaxVersions(maxVersions)
    , mBloomFilterType(bloomFilterType)
    , mBlockSize(blockSize)
    , mDeviationCellVersionInSec(deviationCellVersionInSec)
{
}

int TableOptions::GetTimeToLive() const
{
    if (!mTimeToLive.HasValue()) {
        throw OTSClientException("TimeToLive is not set.");
    }
    return mTimeToLive.GetValue();
}

void TableOptions::SetTimeToLive(int timeToLive)
{
    mTimeToLive.SetValue(timeToLive);
}

bool TableOptions::HasTimeToLive() const
{
    return mTimeToLive.HasValue();
}

int TableOptions::GetMaxVersions() const
{
    if (!mMaxVersions.HasValue()) {
        throw OTSClientException("MaxVersions is not set.");
    }
    return mMaxVersions.GetValue();
}

void TableOptions::SetMaxVersions(int maxVersions)
{
    mMaxVersions.SetValue(maxVersions);
}

bool TableOptions::HasMaxVersions() const
{
    return mMaxVersions.HasValue();
}

BloomFilterType TableOptions::GetBloomFilterType() const
{
    if (!mBloomFilterType.HasValue()) {
        throw OTSClientException("BloomFilterType is not set.");
    }
    return mBloomFilterType.GetValue();
}

void TableOptions::SetBloomFilterType(BloomFilterType bloomFilterType)
{
    mBloomFilterType.SetValue(bloomFilterType);
}

bool TableOptions::HasBloomFilterType() const
{
    return mBloomFilterType.HasValue();
}

int TableOptions::GetBlockSize() const
{
    if (!mBlockSize.HasValue()) {
        throw OTSClientException("BlockSize is not set.");
    }
    return mBlockSize.GetValue();
}

void TableOptions::SetBlockSize(int blockSize)
{
    mBlockSize.SetValue(blockSize);
}

bool TableOptions::HasBlockSize() const
{
    return mBlockSize.HasValue();
}

int64_t TableOptions::GetDeviationCellVersionInSec() const
{
    if (!mDeviationCellVersionInSec.HasValue()) {
        throw OTSClientException("DeviationCellVersionInSec is not set.");
    }
    return mDeviationCellVersionInSec.GetValue();
}

void TableOptions::SetDeviationCellVersionInSec(int64_t deviationCellVersionInSec)
{
    mDeviationCellVersionInSec.SetValue(deviationCellVersionInSec);
}

bool TableOptions::HasDeviationCellVersionInSec() const
{
    return mDeviationCellVersionInSec.HasValue();
}

// CapacityUnit

CapacityUnit::CapacityUnit()
{
}

CapacityUnit::CapacityUnit(
    int readCapacityUnit,
    int writeCapacityUnit)
    : mReadCapacityUnit(readCapacityUnit)
    , mWriteCapacityUnit(writeCapacityUnit)
{
}

int CapacityUnit::GetReadCapacityUnit() const
{
    if (!mReadCapacityUnit.HasValue()) {
        throw OTSClientException("ReadCapacityUnit is not set.");
    }
    return mReadCapacityUnit.GetValue();
}

void CapacityUnit::SetReadCapacityUnit(int readCapacityUnit)
{
    mReadCapacityUnit.SetValue(readCapacityUnit);
}

bool CapacityUnit::HasReadCapacityUnit() const
{
    return mReadCapacityUnit.HasValue();
}

int CapacityUnit::GetWriteCapacityUnit() const
{
    if (!mWriteCapacityUnit.HasValue()) {
        throw OTSClientException("WriteCapacityUnit is not set.");
    }
    return mWriteCapacityUnit.GetValue();
}

void CapacityUnit::SetWriteCapacityUnit(int writeCapacityUnit)
{
    mWriteCapacityUnit.SetValue(writeCapacityUnit);
}

bool CapacityUnit::HasWriteCapacityUnit() const
{
    return mWriteCapacityUnit.HasValue();
}

bool CapacityUnit::HasSetValue() const
{
    if (mReadCapacityUnit.HasValue() || mWriteCapacityUnit.HasValue()) {
        return true;
    } else {
        return false;
    }
}

// ReservedThroughput

ReservedThroughput::ReservedThroughput()
{
}

ReservedThroughput::ReservedThroughput(
    int readCapacityUnit,
    int writeCapacityUnit)
    : mCapacityUnit(readCapacityUnit, writeCapacityUnit)
{
}

ReservedThroughput::ReservedThroughput(const CapacityUnit& capacityUnit)
    : mCapacityUnit(capacityUnit)
{
}

const CapacityUnit& ReservedThroughput::GetCapacityUnit() const
{
    return mCapacityUnit;
}

void ReservedThroughput::SetCapacityUnit(const CapacityUnit& capacityUnit)
{
    mCapacityUnit = capacityUnit;
}

// PartitionRange

PartitionRange::PartitionRange(
    const PrimaryKeyValue& begin,
    const PrimaryKeyValue& end)
    : mBegin(begin)
    , mEnd(end)
{
}

const PrimaryKeyValue& PartitionRange::GetBegin() const
{
    return mBegin;
}

void PartitionRange::SetBegin(const PrimaryKeyValue& begin)
{
    mBegin = begin;
}

const PrimaryKeyValue& PartitionRange::GetEnd() const
{
    return mEnd;
}

void PartitionRange::SetEnd(const PrimaryKeyValue& end)
{
    mEnd = end;
}

// ReservedThroughputDetails
ReservedThroughputDetails::ReservedThroughputDetails()
{
}


ReservedThroughputDetails::ReservedThroughputDetails(
        const CapacityUnit& capacityUnit,
        int64_t lastIncreaseTime,
        int64_t lastDecreaseTime) 
  : mCapacityUnit(capacityUnit), 
    mLastIncreaseTime(lastIncreaseTime), 
    mLastDecreaseTime(lastDecreaseTime) 
{
}

const CapacityUnit& ReservedThroughputDetails::GetCapacityUnit() const {
    return mCapacityUnit;
}

void ReservedThroughputDetails::SetCapacityUnit(const CapacityUnit& capacityUnit) {
    mCapacityUnit = capacityUnit;
}

int64_t ReservedThroughputDetails::GetLastIncreaseTime() const {
    return mLastIncreaseTime;

}

void ReservedThroughputDetails::SetLastIncreaseTime(int64_t lastIncreaseTime) {
    mLastIncreaseTime = lastIncreaseTime;
}

int64_t ReservedThroughputDetails::GetLastDecreaseTime() const {
    return mLastDecreaseTime;
}

void ReservedThroughputDetails::SetLastDecreaseTime(int64_t lastDecreaseTime) {
    mLastDecreaseTime = lastDecreaseTime;
}

// ColumnValue

ColumnValue::ColumnValue()
    : mHasSetValue(false)
{
}

ColumnValue::ColumnValue(int value)
    : mHasSetValue(true)
    , mType(CT_INTEGER)
    , mIntegerValue(value)
{
}

ColumnValue::ColumnValue(int64_t value)
    : mHasSetValue(true)
    , mType(CT_INTEGER)
    , mIntegerValue(value)
{
}

ColumnValue::ColumnValue(const std::string& value)
    : mHasSetValue(true)
    , mType(CT_STRING)
    , mStringValue(value)
{
}

ColumnValue::ColumnValue(const char* value)
    : mHasSetValue(true)
    , mType(CT_STRING)
    , mStringValue(value)
{
}

ColumnValue::ColumnValue(const Blob& value)
    : mHasSetValue(true)
    , mType(CT_BINARY)
    , mBinaryValue(value)
{
}

ColumnValue::ColumnValue(bool value)
    : mHasSetValue(true)
    , mType(CT_BOOLEAN)
    , mBooleanValue(value)
{
}

ColumnValue::ColumnValue(double value)
    : mHasSetValue(true)
    , mType(CT_DOUBLE)
    , mDoubleValue(value)
{
}

ColumnType ColumnValue::GetType() const
{
    if (!mHasSetValue) {
        throw OTSClientException("Column value is not set.");
    }
    return mType;
}

int64_t ColumnValue::AsInteger() const
{
    if (!mHasSetValue) {
        throw OTSClientException("Column value is not set.");
    }
    if (mType != CT_INTEGER) {
        throw OTSClientException("Column type is not CT_INTEGER.");
    }
    return mIntegerValue;
}

void ColumnValue::SetValue(int value)
{
    mType = CT_INTEGER;
    mIntegerValue = value;
    mHasSetValue = true;
}

void ColumnValue::SetValue(int64_t value)
{
    mType = CT_INTEGER;
    mIntegerValue = value;
    mHasSetValue = true;
}

const std::string& ColumnValue::AsString() const
{
    if (!mHasSetValue) {
        throw OTSClientException("Column value is not set.");
    }
    if (mType != CT_STRING) {
        throw OTSClientException("Column type is not CT_STRING.");
    }
    return mStringValue;
}

void ColumnValue::SetValue(const std::string& value)
{
    mType = CT_STRING;
    mStringValue = value;
    mHasSetValue = true;
}

void ColumnValue::SetValue(const char* value)
{
    mType = CT_STRING;
    mStringValue.assign(value);
    mHasSetValue = true;
}

const Blob& ColumnValue::AsBinary() const
{
    if (!mHasSetValue) {
        throw OTSClientException("Column value is not set.");
    }
    if (mType != CT_BINARY) {
        throw OTSClientException("Column type is not CT_BINARY.");
    }
    return mBinaryValue;
}

void ColumnValue::SetValue(const Blob& value)
{
    mType = CT_BINARY;
    mBinaryValue = value;
    mHasSetValue = true;
}

bool ColumnValue::AsBoolean() const
{
    if (!mHasSetValue) {
        throw OTSClientException("Column value is not set.");
    }
    if (mType != CT_BOOLEAN) {
        throw OTSClientException("Column type is not CT_BOOLEAN.");
    }
    return mBooleanValue;
}

void ColumnValue::SetValue(bool value)
{
    mType = CT_BOOLEAN;
    mBooleanValue = value;
    mHasSetValue = true;
}

double ColumnValue::AsDouble() const
{
    if (!mHasSetValue) {
        throw OTSClientException("Column value is not set.");
    }
    if (mType != CT_DOUBLE) {
        throw OTSClientException("Column type is not CT_DOUBLE.");
    }
    return mDoubleValue;
}

void ColumnValue::SetValue(double value)
{
    mType = CT_DOUBLE;
    mDoubleValue = value;
    mHasSetValue = true;
}

namespace impl {

template<>
class ToStringImpl<double>
{
public:
    void operator()(string* out, const double& v) const;
};

void ToStringImpl<double>::operator()(string* out, const double& v) const
{
    ostringstream oss;
    oss << v;
    string res = oss.str();
    out->append(res);
    if (res.find('.') == string::npos) {
        out->append(".0");
    }
}

void ToStringImpl<ColumnValue>::operator()(string* out, const ColumnValue& v) const
{
    if (v.GetType() == CT_STRING) {
        EscapedString(out, v.AsString());
    } else if (v.GetType() == CT_INTEGER) {
        IntToStr(out, v.AsInteger());
    } else if (v.GetType() == CT_BINARY) {
        out->append("b\"");
        const Blob& b = v.AsBinary();
        const char* s = b.Data();
        for(int64_t i = 0, sz = b.Size(); i < sz; ++i) {
            Hex(out, s[i]);
        }
        out->append("\"");
    } else if (v.GetType() == CT_BOOLEAN) {
        if (v.AsBoolean()) {
            out->append("true");
        } else {
            out->append("false");
        }
    } else if (v.GetType() == CT_DOUBLE) {
        ToString(out, v.AsDouble());
    } else {
        assert(false);
    }
}

} // namespace impl

// Column

Column::Column(const string& name)
    : mName(name)
{
}

Column::Column(
    const string& name,
    const ColumnValue& value)
    : mName(name)
    , mValue(value)
{
}

Column::Column(
    const string& name,
    const ColumnValue& value,
    int64_t ts)
    : mName(name)
    , mValue(value)
    , mTimestamp(ts)
{
}

const string& Column::GetName() const
{
    return mName;
}

void Column::SetName(const string& name)
{
    mName = name;
}

bool Column::HasValue() const
{
    return mValue.HasValue();
}

const ColumnValue& Column::GetValue() const
{
    if (!mValue.HasValue()) {
        throw OTSClientException("ColumnValue is not set.");
    }
    return mValue.GetValue();
}

void Column::SetValue(const ColumnValue& value)
{
    mValue.SetValue(value);
}

bool Column::HasTimestamp() const
{
    return mTimestamp.HasValue();
}

int64_t Column::GetTimestamp() const
{
    if (!mTimestamp.HasValue()) {
        throw OTSClientException("Timestamp is not set.");
    }
    return mTimestamp.GetValue();
}

void Column::SetTimestamp(int64_t ts)
{
    mTimestamp.SetValue(ts);
}

// ConsumedCapacity

ConsumedCapacity::ConsumedCapacity()
{
}

ConsumedCapacity::ConsumedCapacity(const CapacityUnit& capacityUnit)
    : mCapacityUnit(capacityUnit)
{
}

const CapacityUnit& ConsumedCapacity::GetCapacityUnit() const
{
    return mCapacityUnit;
}

void ConsumedCapacity::SetCapacityUnit(const CapacityUnit& capacityUnit)
{
    mCapacityUnit = capacityUnit;
}

// Row

Row::Row()
{
}

Row::Row(
    const PrimaryKey& primaryKey,
    const std::list<Column>& columns)
    : mPrimaryKey(primaryKey)
    , mColumns(columns)
{
}

const PrimaryKey& Row::GetPrimaryKey() const
{
    return mPrimaryKey;
}

void Row::SetPrimaryKey(const PrimaryKey& primaryKey)
{
    mPrimaryKey = primaryKey;
}

void Row::AddPrimaryKeyColumn(const PrimaryKeyColumn& column)
{
    mPrimaryKey.AddPrimaryKeyColumn(column);
}

void Row::AddPrimaryKeyColumn(
    const std::string& name,
    const PrimaryKeyValue& value)
{
    mPrimaryKey.AddPrimaryKeyColumn(name, value);
}

const std::list<Column>& Row::GetColumns() const
{
    return mColumns;
}

void Row::SetColumns(const std::list<Column>& columns)
{
    mColumns = columns;
}

void Row::AddColumn(const Column& column)
{
    mColumns.push_back(column);
}

void Row::AddColumn(const std::string& name, const ColumnValue& value)
{
    mColumns.push_back(Column(name, value));
}

// TimeRange

TimeRange::TimeRange()
{
}

TimeRange::TimeRange(int64_t minStamp, int64_t maxStamp)
    : mMinStamp(minStamp)
    , mMaxStamp(maxStamp)
{
}

int64_t TimeRange::GetMinStamp() const
{
    return mMinStamp;
}

void TimeRange::SetMinStamp(int64_t minStamp)
{
    mMinStamp = minStamp;
}

int64_t TimeRange::GetMaxStamp() const
{
    return mMaxStamp;
}

void TimeRange::SetMaxStamp(int64_t maxStamp)
{
    mMaxStamp = maxStamp;
}

bool TimeRange::WithinTimeRange(int64_t ts) const
{
    return ts >= mMinStamp && ts < mMaxStamp;
}

} // end of tablestore
} // end of aliyun
