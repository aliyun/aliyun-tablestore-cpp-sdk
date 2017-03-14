#ifndef OTS_STATIC_INDEX_TYPE_DELEGATES_H
#define OTS_STATIC_INDEX_TYPE_DELEGATES_H

#include "string_tools.h"
#include "logging_assert.h"
#include "foreach.h"
#include "jsonize.h"
#include "slice.h"
#include "ots_static_index/exceptional.h"
#include "ots/ots_types.h"
#include <tr1/memory>
#include <tr1/tuple>
#include <map>
#include <deque>
#include <string>
#include <algorithm>
#include <stdint.h>

namespace static_index {

class Logger;

class Value
{
public:
    enum Type
    {
        INVALID,
        INF_MIN,
        INF_MAX,
        INTEGER,
        STRING,
        BOOLEAN,
        DOUBLE,
    };

private:
    Logger* mLogger; // not own
    Type mType;
    int64_t mIntValue;
    ::std::string mStrValue;
    bool mBoolValue;
    double mDblValue;

    void Init(Logger*, Type);

public:
    explicit Value(Logger* logger)
    {
        Init(logger, INVALID);
    }
    
    class InfMin {};
    explicit Value(Logger* logger, InfMin)
    {
        Init(logger, INF_MIN);
    }
    class InfMax {};
    explicit Value(Logger* logger, InfMax)
    {
        Init(logger, INF_MAX);
    }
    
    
    explicit Value(Logger* logger, int64_t v)
    {
        Init(logger, INTEGER);
        mIntValue = v;
    }
    explicit Value(Logger* logger, const ::std::string& v)
    {
        Init(logger, STRING);
        mStrValue = v;
    }
    explicit Value(Logger* logger, bool v)
    {
        Init(logger, BOOLEAN);
        mBoolValue = v;
    }
    explicit Value(Logger* logger, double v)
    {
        Init(logger, DOUBLE);
        mDblValue = v;
    }

    Type GetType() const
    {
        return mType;
    }

    bool IsInfMin() const
    {
        return mType == INF_MIN;
    }

    bool IsInfMax() const
    {
        return mType == INF_MAX;
    }

    int64_t Int() const;
    ::std::string Str() const;
    bool Bool() const;
    double Double() const;

    void SetInfMin();
    void SetInfMax();
    void SetInt(int64_t);
    void SetStr(const ::std::string&);
    void SetBool(bool);
    void SetDouble(double);

    enum ComparisonResult
    {
        TYPE_MISMATCH = 0,
        SMALLER = 1,
        EQUIV = 2,
        LARGER = 4,
    };

    ComparisonResult Compare(const Value&) const;

    Value Next() const;

    friend bool operator==(const Value&, const Value&);
};

bool operator==(const Value&, const Value&);
inline bool operator!=(const Value& a, const Value& b)
{
    return !(a == b);
}

class ValueHasher
{
public:
    ::std::size_t operator()(const Value& v) const;
};

class Column
{
    Logger* mLogger;

public:
    ::std::string mName;
    Value mValue;

    explicit Column(Logger* logger)
      : mLogger(logger),
        mValue(logger)
    {}
};

class ColumnsHasher
{
public:
    ::std::size_t operator()(const ::std::deque<Column>& v) const;
};

struct Condition
{
    ::aliyun::openservices::ots::RowExistenceExpectation mRowExist;
};

struct PutRowRequest
{
    ::std::string mTracker;
    ::std::string mTableName;
    Condition mCondition;
    ::std::deque<Column> mPrimaryKey;
    ::std::deque<Column> mAttributes;

    void Swap(PutRowRequest& req)
    {
        ::std::swap(mTracker, req.mTracker);
        ::std::swap(mTableName, req.mTableName);
        ::std::swap(mCondition, req.mCondition);
        ::std::swap(mPrimaryKey, req.mPrimaryKey);
        ::std::swap(mAttributes, req.mAttributes);
    }
};

class PutRowResponse
{
};


struct DeleteRowRequest
{
    ::std::string mTracker;
    ::std::string mTableName;
    Condition mCondition;
    ::std::deque<Column> mPrimaryKey;

    void Swap(PutRowRequest& req)
    {
        ::std::swap(mTracker, req.mTracker);
        ::std::swap(mTableName, req.mTableName);
        ::std::swap(mCondition, req.mCondition);
        ::std::swap(mPrimaryKey, req.mPrimaryKey);
    }
};

class DeleteRowResponse
{
};

struct BatchWriteRequest
{
    ::std::string mTracker;
    ::std::deque< ::std::tr1::tuple<PutRowRequest, const void*> > mPutRows;
    ::std::deque< ::std::tr1::tuple<DeleteRowRequest, const void*> > mDelRows;
};

struct BatchWriteResponse
{
    ::std::deque< ::std::tr1::tuple<Exceptional, const void*> > mPutRows;
    ::std::deque< ::std::tr1::tuple<Exceptional, const void*> > mDelRows;
};

struct GetRangeRequest
{
    ::std::string mTracker;
    ::std::string mTableName;
    int32_t mLimit;
    ::std::deque<Column> mStartPkey;
    ::std::deque<Column> mStopPkey;
    ::std::deque< ::std::string > mWantedColumns;

    GetRangeRequest()
      : mLimit(0)
    {}
};

struct Row
{
    ::std::deque<Column> mPrimaryKey;
    ::std::deque<Column> mAttributes;
};

struct GetRangeResponse
{
    ::std::deque<Column> mNextPkey;
    ::std::deque< ::std::tr1::shared_ptr<Row> > mRows;
};

struct GetRowRequest
{
    ::std::string mTracker;
    ::std::string mTableName;
    ::std::deque<Column> mPrimaryKey;
};

struct GetRowResponse
{
    ::std::string mTracker;
    ::std::tr1::shared_ptr<Row> mRow;
};

struct BatchGetRowRequest
{
    ::std::string mTracker;
    ::std::deque< ::std::tr1::tuple<GetRowRequest, const void*> > mGetRows;
};

struct BatchGetRowResponse
{
    ::std::string mTracker;
    ::std::deque< ::std::tr1::tuple<Exceptional, GetRowResponse, const void*> > mGetRows;
};

namespace details {

template<>
class Jsonizer< ::static_index::PutRowRequest >
{
    ::static_index::Logger* mLogger;
public:
    explicit Jsonizer(::static_index::Logger* logger)
      : mLogger(logger)
    {}

    ::Json::Value operator()(const ::static_index::PutRowRequest& val) const;
};

template<>
class Jsonizer< ::static_index::DeleteRowRequest >
{
    ::static_index::Logger* mLogger;
public:
    explicit Jsonizer(::static_index::Logger* logger)
      : mLogger(logger)
    {}

    ::Json::Value operator()(const ::static_index::DeleteRowRequest& val) const;
};

template<>
class Jsonizer< ::static_index::BatchWriteRequest >
{
    ::static_index::Logger* mLogger;
public:
    explicit Jsonizer(::static_index::Logger* logger)
      : mLogger(logger)
    {}

    ::Json::Value operator()(const ::static_index::BatchWriteRequest& val) const;
};

template<>
class Jsonizer< ::static_index::BatchWriteResponse >
{
    ::static_index::Logger* mLogger;
public:
    explicit Jsonizer(::static_index::Logger* logger)
      : mLogger(logger)
    {}

    ::Json::Value operator()(const ::static_index::BatchWriteResponse& val) const;
};

template<>
class Unjsonizer< ::static_index::BatchWriteResponse >
{
    Logger* mLogger;
public:
    explicit Unjsonizer(Logger* logger)
      : mLogger(logger)
    {}

    void operator()(::static_index::BatchWriteResponse*, const ::Json::Value&) const;
};

template<>
class Jsonizer< ::static_index::GetRangeRequest >
{
    ::static_index::Logger* mLogger;
public:
    explicit Jsonizer(::static_index::Logger* logger)
      : mLogger(logger)
    {}

    ::Json::Value operator()(const ::static_index::GetRangeRequest& val) const;
};

template<>
class Jsonizer< ::static_index::GetRangeResponse >
{
    ::static_index::Logger* mLogger;
public:
    explicit Jsonizer(::static_index::Logger* logger)
      : mLogger(logger)
    {}

    ::Json::Value operator()(const ::static_index::GetRangeResponse& val) const;
};

template<>
class Unjsonizer< ::static_index::GetRangeResponse >
{
    Logger* mLogger;
public:
    explicit Unjsonizer(Logger* logger)
      : mLogger(logger)
    {}

    void operator()(::static_index::GetRangeResponse*, const ::Json::Value&) const;
};

template<>
class Jsonizer< ::static_index::BatchGetRowRequest >
{
    ::static_index::Logger* mLogger;
public:
    explicit Jsonizer(::static_index::Logger* logger)
      : mLogger(logger)
    {}

    ::Json::Value operator()(const ::static_index::BatchGetRowRequest& val) const;
};

template<>
class Jsonizer< ::static_index::BatchGetRowResponse >
{
    ::static_index::Logger* mLogger;
public:
    explicit Jsonizer(::static_index::Logger* logger)
      : mLogger(logger)
    {}

    ::Json::Value operator()(const ::static_index::BatchGetRowResponse& val) const;
};

template<>
class Unjsonizer< ::static_index::BatchGetRowResponse >
{
    Logger* mLogger;
public:
    explicit Unjsonizer(Logger* logger)
      : mLogger(logger)
    {}

    void operator()(::static_index::BatchGetRowResponse* out, const ::Json::Value&) const;
};

template<>
class Jsonizer< ::static_index::GetRowRequest >
{
    ::static_index::Logger* mLogger;
public:
    explicit Jsonizer(::static_index::Logger* logger)
      : mLogger(logger)
    {}

    ::Json::Value operator()(const ::static_index::GetRowRequest& val) const;
};

template<>
class Jsonizer< ::static_index::GetRowResponse >
{
    ::static_index::Logger* mLogger;
public:
    explicit Jsonizer(::static_index::Logger* logger)
      : mLogger(logger)
    {}

    ::Json::Value operator()(const ::static_index::GetRowResponse& val) const;
};

template<>
class Unjsonizer< ::static_index::GetRowResponse >
{
    Logger* mLogger;
public:
    explicit Unjsonizer(Logger* logger)
      : mLogger(logger)
    {}

    void operator()(
        ::static_index::GetRowResponse*,
        const ::Json::Value&) const;
};

template<>
class Jsonizer< ::static_index::Row >
{
    ::static_index::Logger* mLogger;
public:
    explicit Jsonizer(::static_index::Logger* logger)
      : mLogger(logger)
    {}

    ::Json::Value operator()(const ::static_index::Row& val) const;
};

template<>
class Unjsonizer< ::static_index::Row >
{
    Logger* mLogger;
public:
    explicit Unjsonizer(Logger* logger)
      : mLogger(logger)
    {}

    void operator()(
        ::static_index::Row*,
        const ::Json::Value&) const;
};

template<>
class Jsonizer< ::std::deque< ::static_index::Column > >
{
    ::static_index::Logger* mLogger;
public:
    explicit Jsonizer(::static_index::Logger* logger)
      : mLogger(logger)
    {}

    ::Json::Value operator()(const ::std::deque< ::static_index::Column >& val) const;
};

template<>
class Unjsonizer< ::std::deque< ::static_index::Column > >
{
    Logger* mLogger;
public:
    explicit Unjsonizer(Logger* logger)
      : mLogger(logger)
    {}

    void operator()(
        ::std::deque< ::static_index::Column >*,
        const ::Json::Value&) const;
};

template<>
class Jsonizer< ::static_index::Column >
{
    ::static_index::Logger* mLogger;
public:
    explicit Jsonizer(::static_index::Logger* logger)
      : mLogger(logger)
    {}

    ::Json::Value operator()(const ::static_index::Column& val) const;
};

template<>
class Unjsonizer< ::static_index::Column >
{
    Logger* mLogger;
public:
    explicit Unjsonizer(Logger* logger)
      : mLogger(logger)
    {}

    void operator()(
        ::static_index::Column*,
        const ::Json::Value&) const;
};

template<>
class Jsonizer< ::static_index::Value >
{
    ::static_index::Logger* mLogger;
public:
    explicit Jsonizer(::static_index::Logger* logger)
      : mLogger(logger)
    {}

    ::Json::Value operator()(const ::static_index::Value& val) const;
};

template<>
class Unjsonizer< ::static_index::Value >
{
    Logger* mLogger;
public:
    explicit Unjsonizer(Logger* logger)
      : mLogger(logger)
    {}

    void operator()(::static_index::Value* out, const ::Json::Value&) const;
};

template<>
class Jsonizer< ::static_index::Exceptional >
{
    ::static_index::Logger* mLogger;
public:
    explicit Jsonizer(::static_index::Logger* logger)
      : mLogger(logger)
    {}

    ::Json::Value operator()(const ::static_index::Exceptional& val) const;
};

template<>
class Unjsonizer< ::static_index::Exceptional >
{
    Logger* mLogger;
public:
    explicit Unjsonizer(Logger* logger)
      : mLogger(logger)
    {}

    void operator()(::static_index::Exceptional* out, const ::Json::Value&) const;
};

} // namespace details

} // namespace static_index

#endif /* OTS_STATIC_INDEX_TYPE_DELEGATES_H */
