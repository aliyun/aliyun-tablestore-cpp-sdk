/* 
BSD 3-Clause License

Copyright (c) 2017, Alibaba Cloud
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
#pragma once

#include "core/error.hpp"
#include "util/prettyprint.hpp"
#include "util/move.hpp"
#include "util/mempiece.hpp"
#include "util/optional.hpp"
#include "util/timestamp.hpp"
#include <tr1/memory>
#include <string>
#include <deque>

namespace aliyun {
namespace tablestore {
namespace util {
namespace random {
class IRandom;
} // namespace random
} // namespace util

namespace core {

class IRetryStrategy;

enum Action
{
    API_CREATE_TABLE,
    API_LIST_TABLE,
    API_DESCRIBE_TABLE,
    API_DELETE_TABLE,
    API_UPDATE_TABLE,
    API_GET_ROW,
    API_PUT_ROW,
    API_UPDATE_ROW,
    API_DELETE_ROW,
    API_BATCH_GET_ROW,
    API_BATCH_WRITE_ROW,
    API_GET_RANGE,
    API_COMPUTE_SPLIT_POINTS_BY_SIZE,
};

template<class T>
void collectActions(T* xs)
{
    xs->push_back(API_CREATE_TABLE);
    xs->push_back(API_LIST_TABLE);
    xs->push_back(API_DESCRIBE_TABLE);
    xs->push_back(API_DELETE_TABLE);
    xs->push_back(API_UPDATE_TABLE);
    xs->push_back(API_GET_ROW);
    xs->push_back(API_PUT_ROW);
    xs->push_back(API_UPDATE_ROW);
    xs->push_back(API_DELETE_ROW);
    xs->push_back(API_BATCH_GET_ROW);
    xs->push_back(API_BATCH_WRITE_ROW);
    xs->push_back(API_GET_RANGE);
    xs->push_back(API_COMPUTE_SPLIT_POINTS_BY_SIZE);
}

/**
 * Types of primary key
 */
enum PrimaryKeyType
{
    PKT_INTEGER = 1,
    PKT_STRING,
    PKT_BINARY,
};

template<class T>
void collectPrimaryKeyTypes(T* xs)
{
    xs->push_back(PKT_INTEGER);
    xs->push_back(PKT_STRING);
    xs->push_back(PKT_BINARY);
}

/**
 * Options of primary key
 */
enum PrimaryKeyOption
{
    PKO_NONE = 0,
    PKO_AUTO_INCREMENT,
};

template<class T>
void collectPrimaryKeyOptions(T* xs)
{
    xs->push_back(PKO_NONE);
    xs->push_back(PKO_AUTO_INCREMENT);
}

/**
 * for internal usage. use it wisely.
 */
enum BloomFilterType
{
    BFT_NONE = 1,
    BFT_CELL,
    BFT_ROW,
};

template<class T>
void collectBloomFilterType(T* xs)
{
    xs->push_back(BFT_NONE);
    xs->push_back(BFT_CELL);
    xs->push_back(BFT_ROW);
}


enum TableStatus
{
    ACTIVE = 1,
    INACTIVE,
    LOADING,
    UNLOADING,
    UPDATING,
};

template<class T>
void collectTableStatuses(T* xs)
{
    xs->push_back(ACTIVE);
    xs->push_back(INACTIVE);
    xs->push_back(LOADING);
    xs->push_back(UNLOADING);
    xs->push_back(UPDATING);
}


enum ColumnType
{
    CT_STRING,
    CT_INTEGER,
    CT_BINARY,
    CT_BOOLEAN,
    CT_DOUBLE
};

template<class T>
void collectColumnTypes(T* xs)
{
    xs->push_back(CT_STRING);
    xs->push_back(CT_INTEGER);
    xs->push_back(CT_BINARY);
    xs->push_back(CT_BOOLEAN);
    xs->push_back(CT_DOUBLE);
}


enum ReturnType
{
    RT_NONE,
    RT_PRIMARY_KEY,
};

template<class T>
void collectReturnTypes(T* xs)
{
    xs->push_back(RT_NONE);
    xs->push_back(RT_PRIMARY_KEY);
}

enum CompareResult
{
    UNCOMPARABLE,
    EQUAL_TO,
    LESS_THAN,
    GREATER_THAN,
};
    

template<class Elem>
class IVector
{
public:
    typedef Elem ElemType;

    virtual ~IVector() {}

    virtual int64_t size() const =0;
    virtual const Elem& operator[](int64_t idx) const =0;
    virtual Elem& operator[](int64_t idx) =0;
    virtual const Elem& back() const =0;
    virtual Elem& back() =0;
    virtual Elem& append() =0;
    virtual void clear() =0;
};

template<class Elem>
class DequeBasedVector : public IVector<Elem>, public util::Moveable
{
public:
    typedef typename IVector<Elem>::ElemType ElemType;

    explicit DequeBasedVector()
    {}
    explicit DequeBasedVector(const util::MoveHolder<DequeBasedVector<ElemType> >& a)
    {
        *this = a;
    }

    DequeBasedVector<ElemType>& operator=(
        const util::MoveHolder<DequeBasedVector<ElemType> >& a)
    {
        util::moveAssign(&mElems, util::move(a->mElems));
        return *this;
    }

    int64_t size() const
    {
        return mElems.size();
    }

    const Elem& operator[](int64_t idx) const
    {
        return mElems.at(idx);
    }

    Elem& operator[](int64_t idx)
    {
        return mElems.at(idx);
    }

    const Elem& back() const
    {
        OTS_ASSERT(size() > 0);
        return mElems.back();
    }

    Elem& back()
    {
        OTS_ASSERT(size() > 0);
        return mElems.back();
    }
    
    Elem& append()
    {
        mElems.push_back(Elem());
        return mElems.back();
    }

    void clear()
    {
        mElems.clear();
    }
    
    void prettyPrint(std::string* out) const
    {
        pp::prettyPrint(out, mElems);
    }

private:
    std::deque<Elem> mElems;
};


class Endpoint: public util::Moveable
{
public:
    explicit Endpoint() {}
    explicit Endpoint(const std::string& endpoint, const std::string& instance);
    explicit Endpoint(const util::MoveHolder<Endpoint>& a);
    Endpoint& operator=(const util::MoveHolder<Endpoint>& a);

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;

    const std::string& endpoint() const
    {
        return mEndpoint;
    }

    std::string& mutableEndpoint()
    {
        return mEndpoint;
    }

    const std::string& instanceName() const
    {
        return mInstanceName;
    }

    std::string& mutableInstanceName()
    {
        return mInstanceName;
    }

private:
    std::string mEndpoint;
    std::string mInstanceName;
};

class Credential: public util::Moveable
{
public:
    explicit Credential() {}

    explicit Credential(
        const std::string& accessKeyId, const std::string& accessKeySecret)
      : mAccessKeyId(accessKeyId),
        mAccessKeySecret(accessKeySecret)
    {}

    explicit Credential(
        const std::string& accessKeyId, const std::string& accessKeySecret,
        const std::string& securityToken)
      : mAccessKeyId(accessKeyId),
        mAccessKeySecret(accessKeySecret),
        mSecurityToken(securityToken)
    {}
        
    explicit Credential(const util::MoveHolder<Credential>&);

    Credential& operator=(const util::MoveHolder<Credential>&);

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;

    const std::string& accessKeyId() const
    {
        return mAccessKeyId;
    }

    std::string& mutableAccessKeyId()
    {
        return mAccessKeyId;
    }

    const std::string& accessKeySecret() const
    {
        return mAccessKeySecret;
    }

    std::string& mutableAccessKeySecret()
    {
        return mAccessKeySecret;
    }

    const std::string& securityToken() const
    {
        return mSecurityToken;
    }

    std::string& mutableSecurityToken()
    {
        return mSecurityToken;
    }

private:
    std::string mAccessKeyId;
    std::string mAccessKeySecret;
    std::string mSecurityToken;
};

class ClientOptions: public util::Moveable
{
public:
    explicit ClientOptions();
    explicit ClientOptions(const util::MoveHolder<ClientOptions>&);

    ClientOptions& operator=(const util::MoveHolder<ClientOptions>&);

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;

    int64_t maxConnections() const
    {
        return mMaxConnections;
    }

    int64_t& mutableMaxConnections()
    {
        return mMaxConnections;
    }

    const util::Duration& connectTimeout() const
    {
        return mConnectTimeout;
    }

    util::Duration& mutableConnectTimeout()
    {
        return mConnectTimeout;
    }

    const util::Duration& requestTimeout() const
    {
        return mRequestTimeout;
    }

    util::Duration& mutableRequestTimeout()
    {
        return mRequestTimeout;
    }

    const util::Duration& traceThreshold() const
    {
        return mTraceThreshold;
    }

    util::Duration& mutableTraceThreshold()
    {
        return mTraceThreshold;
    }

    bool checkResponesDigest() const
    {
        return mCheckResponseDigest;
    }

    bool& mutableCheckResponseDigest()
    {
        return mCheckResponseDigest;
    }

    const std::tr1::shared_ptr<IRetryStrategy> retryStrategy() const
    {
        return mRetryStrategy;
    }

    std::tr1::shared_ptr<IRetryStrategy>& mutableRetryStrategy()
    {
        return mRetryStrategy;
    }

private:
    std::tr1::shared_ptr<util::random::IRandom> mRandom;
    
    int64_t mMaxConnections;
    util::Duration mConnectTimeout;
    util::Duration mRequestTimeout;
    util::Duration mTraceThreshold;
    bool mCheckResponseDigest;
    std::tr1::shared_ptr<IRetryStrategy> mRetryStrategy;
};

/**
 * Schema of a single primary key
 */
class PrimaryKeyColumnSchema: public util::Moveable
{
public:
    explicit PrimaryKeyColumnSchema()
      : mType(PKT_INTEGER),
        mOption(PKO_NONE)
    {}
    explicit PrimaryKeyColumnSchema(const std::string& name, PrimaryKeyType type)
      : mName(name),
        mType(PKT_INTEGER),
        mOption(PKO_NONE)
    {}
    explicit PrimaryKeyColumnSchema(
        const std::string& name, PrimaryKeyType type, PrimaryKeyOption opt)
      : mName(name),
        mType(type),
        mOption(opt)
    {}

    explicit PrimaryKeyColumnSchema(
        const util::MoveHolder<PrimaryKeyColumnSchema>& a)
      : mType(a->mType),
        mOption(a->mOption)
    {
        util::moveAssign(&mName, util::move(a->mName));
    }

    PrimaryKeyColumnSchema& operator=(
        const util::MoveHolder<PrimaryKeyColumnSchema>& a)
    {
        mType = a->mType;
        mOption = a->mOption;
        util::moveAssign(&mName, util::move(a->mName));
        return *this;
    }

    const std::string& name() const
    {
        return mName;
    }

    std::string& mutableName()
    {
        return mName;
    }

    PrimaryKeyType type() const
    {
        return mType;
    }

    PrimaryKeyType& mutableType()
    {
        return mType;
    }

    PrimaryKeyOption option() const
    {
        return mOption;
    }

    PrimaryKeyOption& mutableOption()
    {
        return mOption;
    }
    
    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    
private:
    std::string mName;
    PrimaryKeyType mType;
    PrimaryKeyOption mOption;
};

class PrimaryKeySchema: public IVector<PrimaryKeyColumnSchema>, public util::Moveable
{
public:
    explicit PrimaryKeySchema() {}

    explicit PrimaryKeySchema(const util::MoveHolder<PrimaryKeySchema>& a)
    {
        util::moveAssign(&mColumns, util::move(a->mColumns));
    }

    PrimaryKeySchema& operator=(const util::MoveHolder<PrimaryKeySchema>& a)
    {
        util::moveAssign(&mColumns, util::move(a->mColumns));
        return *this;
    }

    int64_t size() const
    {
        return mColumns.size();
    }

    const PrimaryKeyColumnSchema& operator[](int64_t idx) const
    {
        return mColumns[idx];
    }

    PrimaryKeyColumnSchema& operator[](int64_t idx)
    {
        return mColumns[idx];
    }

    const PrimaryKeyColumnSchema& back() const
    {
        return mColumns.back();
    }

    PrimaryKeyColumnSchema& back()
    {
        return mColumns.back();
    }

    PrimaryKeyColumnSchema& append()
    {
        return mColumns.append();
    }

    void clear()
    {
        return mColumns.clear();
    }

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;

private:
    DequeBasedVector<PrimaryKeyColumnSchema> mColumns;
};

class PrimaryKeyValue: public util::Moveable
{
public:
    enum Category
    {
        NONE,
        INF_MIN,
        INF_MAX,
        AUTO_INCR,
        INTEGER,
        STRING,
        BINARY,
    };

    static PrimaryKeyType toPrimaryKeyType(Category);

private:
    class InfMin {};
    class InfMax {};
    class AutoIncrement {};

    explicit PrimaryKeyValue(int64_t);
    explicit PrimaryKeyValue(PrimaryKeyType, const util::MemPiece&);
    explicit PrimaryKeyValue(InfMin);
    explicit PrimaryKeyValue(InfMax);
    explicit PrimaryKeyValue(AutoIncrement);

public:
    explicit PrimaryKeyValue();
    explicit PrimaryKeyValue(const util::MoveHolder<PrimaryKeyValue>&);
    PrimaryKeyValue& operator=(const util::MoveHolder<PrimaryKeyValue>&);

    Category category() const;
    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    CompareResult compare(const PrimaryKeyValue&) const;
    bool isReal() const
    {
        switch(category()) {
        case NONE: case INF_MIN: case INF_MAX: case AUTO_INCR:
            return false;
        case INTEGER: case STRING: case BINARY:
            return true;
        }
        return false;
    }

    bool isInfinity() const
    {
        switch(category()) {
        case INF_MIN: case INF_MAX:
            return true;
        case NONE: case AUTO_INCR: case INTEGER: case STRING: case BINARY:
            return false;
        }
        return false;
    }

public:
    // for integers
    static PrimaryKeyValue toInteger(int64_t);
    int64_t integer() const;
    int64_t& mutableInteger();

public:
    // for string
    /**
     * PrimaryKeyValue does not own underlying piece of memory of the string.
     * So, make sure it lives longer than the PrimaryKeyValue.
     */
    static PrimaryKeyValue toStr(const util::MemPiece&);
    const util::MemPiece& str() const;
    util::MemPiece& mutableStr();

public:
    // for blob
    /**
     * PrimaryKeyValue does not own underlying piece of memory of the blob.
     * So, make sure it lives longer than the PrimaryKeyValue.
     */
    static PrimaryKeyValue toBlob(const util::MemPiece&);
    const util::MemPiece& blob() const;
    util::MemPiece& mutableBlob();

public:
    // for +inf
    static PrimaryKeyValue toInfMax();
    bool isInfMax() const;
    void setInfMax();
    
public:
    // for -inf
    static PrimaryKeyValue toInfMin();
    bool isInfMin() const;
    void setInfMin();

public:
    // for placeholder for auto-increment
    static PrimaryKeyValue toAutoIncrement();
    bool isAutoIncrement() const;
    void setAutoIncrement();

private:
    Category mCategory;
    int64_t mIntValue;
    util::MemPiece mStrBlobValue;
};

/**
 * A single column of primary key
 */
class PrimaryKeyColumn: public util::Moveable
{
public:
    explicit PrimaryKeyColumn();
    explicit PrimaryKeyColumn(const std::string&, const PrimaryKeyValue&);
    explicit PrimaryKeyColumn(const util::MoveHolder<PrimaryKeyColumn>& a)
    {
        *this = a;
    }

    PrimaryKeyColumn& operator=(const util::MoveHolder<PrimaryKeyColumn>&);

    const std::string& name() const
    {
        return mName;
    }

    std::string& mutableName()
    {
        return mName;
    }

    const PrimaryKeyValue& value() const
    {
        return mValue;
    }

    PrimaryKeyValue& mutableValue()
    {
        return mValue;
    }

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;

private:
    std::string mName;
    PrimaryKeyValue mValue;
};

class PrimaryKey: public IVector<PrimaryKeyColumn>, public util::Moveable
{
public:
    explicit PrimaryKey() {}
    explicit PrimaryKey(const util::MoveHolder<PrimaryKey>& a)
    {
        *this = a;
    }

    PrimaryKey& operator=(const util::MoveHolder<PrimaryKey>& a)
    {
        util::moveAssign(&mColumns, util::move(a->mColumns));
        return *this;
    }

    int64_t size() const
    {
        return mColumns.size();
    }

    const PrimaryKeyColumn& operator[](int64_t idx) const
    {
        return mColumns[idx];
    }

    PrimaryKeyColumn& operator[](int64_t idx)
    {
        return mColumns[idx];
    }

    const PrimaryKeyColumn& back() const
    {
        return mColumns.back();
    }

    PrimaryKeyColumn& back()
    {
        return mColumns.back();
    }

    PrimaryKeyColumn& append()
    {
        return mColumns.append();
    }

    void clear()
    {
        return mColumns.clear();
    }

    void prettyPrint(std::string* out) const;
    util::Optional<Error> validate() const;
    CompareResult compare(const PrimaryKey& a) const;

private:
    DequeBasedVector<PrimaryKeyColumn> mColumns;
};

/**
 * Meta of a table.
 * Once the table is created, it will never be modified.
 */
class TableMeta: public util::Moveable
{
public:
    explicit TableMeta() {}

    explicit TableMeta(const std::string& tableName)
      : mTableName(tableName)
    {}

    explicit TableMeta(const util::MoveHolder<TableMeta>& a)
    {
        *this = a;
    }

    TableMeta& operator=(const util::MoveHolder<TableMeta>& a)
    {
        util::moveAssign(&mTableName, util::move(a->mTableName));
        util::moveAssign(&mSchema, util::move(a->mSchema));
        return *this;
    }

    const std::string& tableName() const
    {
        return mTableName;
    }

    std::string& mutableTableName()
    {
        return mTableName;
    }

    const PrimaryKeySchema& schema() const
    {
        return mSchema;
    }

    PrimaryKeySchema& mutableSchema()
    {
        return mSchema;
    }

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;

private:
    std::string mTableName;
    PrimaryKeySchema mSchema;
};

class CapacityUnit: public util::Moveable
{
public:
    explicit CapacityUnit() {}

    explicit CapacityUnit(int64_t readCU, int64_t writeCU)
      : mRead(readCU),
        mWrite(writeCU)
    {}

    explicit CapacityUnit(const util::MoveHolder<CapacityUnit>& a)
    {
        *this = a;
    }

    CapacityUnit& operator=(const util::MoveHolder<CapacityUnit>& a)
    {
        util::moveAssign(&mRead, util::move(a->mRead));
        util::moveAssign(&mWrite, util::move(a->mWrite));
        return *this;
    }
    
    const util::Optional<int64_t> read() const
    {
        return mRead;
    }

    util::Optional<int64_t>& mutableRead()
    {
        return mRead;
    }

    const util::Optional<int64_t>& write() const
    {
        return mWrite;
    }

    util::Optional<int64_t>& mutableWrite()
    {
        return mWrite;
    }

    util::Optional<Error> validate() const;
    void prettyPrint(std::string*) const;
        
private:
    util::Optional<int64_t> mRead;
    util::Optional<int64_t> mWrite;
};

/**
 * Options of tables, which can be updated by UpdateTable
 */
class TableOptions: public util::Moveable
{
public:
    explicit TableOptions()
      : mReservedThroughput(0, 0)
    {}

    explicit TableOptions(const util::MoveHolder<TableOptions>& a)
    {
        *this = a;
    }

    TableOptions& operator=(const util::MoveHolder<TableOptions>& a);

    void clear();
    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;

    const util::Optional<util::Duration>& timeToLive() const
    {
        return mTimeToLive;
    }

    util::Optional<util::Duration>& mutableTimeToLive()
    {
        return mTimeToLive;
    }

    const util::Optional<int64_t> maxVersions() const
    {
        return mMaxVersions;
    }

    util::Optional<int64_t>& mutableMaxVersions()
    {
        return mMaxVersions;
    }

    const util::Optional<BloomFilterType> bloomFilterType() const
    {
        return mBloomFilterType;
    }

    util::Optional<BloomFilterType>& mutableBloomFilterType()
    {
        return mBloomFilterType;
    }

    const util::Optional<int64_t> blockSize() const
    {
        return mBlockSize;
    }

    util::Optional<int64_t>& mutableBlockSize()
    {
        return mBlockSize;
    }

    const util::Optional<util::Duration>& maxTimeDeviation() const
    {
        return mMaxTimeDeviation;
    }

    util::Optional<util::Duration>& mutableMaxTimeDeviation()
    {
        return mMaxTimeDeviation;
    }

    const CapacityUnit& reservedThroughput() const
    {
        return mReservedThroughput;
    }

    CapacityUnit& mutableReservedThroughput()
    {
        return mReservedThroughput;
    }
    
private:
    CapacityUnit mReservedThroughput;
    util::Optional<util::Duration> mTimeToLive;
    util::Optional<int64_t> mMaxVersions;
    util::Optional<BloomFilterType> mBloomFilterType;
    util::Optional<int64_t> mBlockSize;
    util::Optional<util::Duration> mMaxTimeDeviation;
};

class ColumnValue: public util::Moveable
{
public:
    enum Category
    {
        NONE,
        STRING,
        INTEGER,
        BINARY,
        BOOLEAN,
        FLOATING_POINT,
    };

    static ColumnType toColumnType(Category);

private:
    explicit ColumnValue(int64_t);
    explicit ColumnValue(Category, const util::MemPiece&);
    explicit ColumnValue(bool);
    explicit ColumnValue(double);

public:
    explicit ColumnValue();
    explicit ColumnValue(const util::MoveHolder<ColumnValue>& a)
    {
        *this = a;
    }
    ColumnValue& operator=(const util::MoveHolder<ColumnValue>&);

    Category category() const
    {
        return mCategory;
    }

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;

public:
    // for strings
    /**
     * ColumnValue does not own underlying piece of memory of the string.
     * So, make sure it lives longer than the ColumnValue.
     */
    static ColumnValue toStr(const util::MemPiece&);
    const util::MemPiece& str() const;
    util::MemPiece& mutableStr();

public:
    // for blob
    /**
     * ColumnValue does not own underlying piece of memory of the blob.
     * So, make sure it lives longer than the ColumnValue.
     */
    static ColumnValue toBlob(const util::MemPiece&);
    const util::MemPiece& blob() const;
    util::MemPiece& mutableBlob();

public:
    // for integers
    static ColumnValue toInteger(int64_t);
    int64_t integer() const;
    int64_t& mutableInteger();

public:
    // for floating point numbers
    static ColumnValue toFloatPoint(double);
    double floatPoint() const;
    double& mutableFloatPoint();

public:
    // for booleans
    static ColumnValue toBoolean(bool);
    bool boolean() const;
    bool& mutableBoolean();

private:
    Category mCategory;
    int64_t mIntValue;
    util::MemPiece mStrBlobValue;
    bool mBoolValue;
    double mFloatingValue;
};


class Column: public util::Moveable
{
public:
    explicit Column() {}
    explicit Column(const std::string&, const ColumnValue&);
    explicit Column(const std::string&, const ColumnValue&, int64_t);

    explicit Column(const util::MoveHolder<Column>& a)
    {
        *this = a;
    }

    Column& operator=(const util::MoveHolder<Column>&);

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    
    const std::string& name() const
    {
        return mName;
    }

    std::string& mutableName()
    {
        return mName;
    }

    const ColumnValue& value() const
    {
        return mValue;
    }

    ColumnValue& mutableValue()
    {
        return mValue;
    }

    const util::Optional<int64_t>& timestamp() const
    {
        return mTimestamp;
    }

    util::Optional<int64_t>& mutableTimestamp()
    {
        return mTimestamp;
    }
    
private:
    std::string mName;
    ColumnValue mValue;
    util::Optional<int64_t> mTimestamp;
};

class Row: public util::Moveable
{
public:
    explicit Row() {}
    explicit Row(const util::MoveHolder<Row>& a)
    {
        *this = a;
    }

    Row& operator=(const util::MoveHolder<Row>&);

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    
    const PrimaryKey& primaryKey() const
    {
        return mPkey;
    }

    PrimaryKey& mutablePrimaryKey()
    {
        return mPkey;
    }

    const IVector<Column>& attributes() const
    {
        return mAttrs;
    }

    IVector<Column>& mutableAttributes()
    {
        return mAttrs;
    }

private:
    PrimaryKey mPkey;
    DequeBasedVector<Column> mAttrs;
};

/**
 * a range of UTC time.
 * Both (inclusive) start and (exclusive) end  must be integral multiple of milliseconds.
 */
class TimeRange: public util::Moveable
{
public:
    explicit TimeRange() {}
    explicit TimeRange(util::UtcTime start, util::UtcTime end)
      : mStart(start),
        mEnd(end)
    {}

    explicit TimeRange(const util::MoveHolder<TimeRange>& a)
    {
        *this = a;
    }

    TimeRange& operator=(const util::MoveHolder<TimeRange>& a);

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    
    util::UtcTime start() const
    {
        return mStart;
    }

    util::UtcTime& mutableStart()
    {
        return mStart;
    }

    util::UtcTime end() const
    {
        return mEnd;
    }

    util::UtcTime& mutableEnd()
    {
        return mEnd;
    }

private:
    util::UtcTime mStart;
    util::UtcTime mEnd;
};

class Split: public util::Moveable
{
public:
    explicit Split() {}

    explicit Split(const util::MoveHolder<Split>& a)
    {
        *this = a;
    }

    Split& operator=(const util::MoveHolder<Split>&);

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;

    /**
     * The (inclusive) lower bound of the split, as the same length as the primary key columns of the table.
     */
    const std::tr1::shared_ptr<PrimaryKey>& lowerBound() const
    {
        return mLowerBound;
    }

    std::tr1::shared_ptr<PrimaryKey>& mutableLowerBound()
    {
        return mLowerBound;
    }

    /**
     * The (exclusive) upper bound of the split, as the same length as the primary key columns of the table.
     */
    const std::tr1::shared_ptr<PrimaryKey>& upperBound() const
    {
        return mUpperBound;
    }

    std::tr1::shared_ptr<PrimaryKey>& mutableUpperBound()
    {
        return mUpperBound;
    }

    /**
     * A hint of the location where the split lies in.
     * If a location is not comfortable to be seen, it will keep empty.
     */
    const std::string location() const
    {
        return mLocation;
    }

    std::string& mutableLocation()
    {
        return mLocation;
    }
    
private:
    std::tr1::shared_ptr<PrimaryKey> mLowerBound;
    std::tr1::shared_ptr<PrimaryKey> mUpperBound;
    std::string mLocation;
};

class Response
{
public:
    virtual ~Response() {}

    Response& operator=(const util::MoveHolder<Response>&);
    
    virtual const std::string& requestId() const
    {
        return mRequestId;
    }

    virtual std::string& mutableRequestId()
    {
        return mRequestId;
    }

    virtual const std::string& traceId() const
    {
        return mTraceId;
    }

    virtual std::string& mutableTraceId()
    {
        return mTraceId;
    }

private:
    std::string mRequestId;
    std::string mTraceId;
};

class CreateTableRequest: public util::Moveable
{
public:
    explicit CreateTableRequest() {}
    explicit CreateTableRequest(const util::MoveHolder<CreateTableRequest>& a)
    {
        *this = a;
    }

    CreateTableRequest& operator=(const util::MoveHolder<CreateTableRequest>&);

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;

    const TableMeta& meta() const
    {
        return mMeta;
    }

    TableMeta& mutableMeta()
    {
        return mMeta;
    }

    const TableOptions& options() const
    {
        return mOptions;
    }

    /**
     * For now, each partition split point must contains exactly one primary 
     * key column and conforms to the table meta.
     */
    const IVector<PrimaryKey>& partitionSplitPoints() const
    {
        return mPartitionSplitPoints;
    }

    IVector<PrimaryKey>& mutablePartitionSplitPoints()
    {
        return mPartitionSplitPoints;
    }

private:
    TableMeta mMeta;
    TableOptions mOptions;
    DequeBasedVector<PrimaryKey> mPartitionSplitPoints;
};

class CreateTableResponse: public Response, public util::Moveable
{
public:
    explicit CreateTableResponse() {}
    explicit CreateTableResponse(const util::MoveHolder<CreateTableResponse>& a)
    {
        *this = a;
    }

    CreateTableResponse& operator=(const util::MoveHolder<CreateTableResponse>& a);
};

} // namespace core
} // namespace tablestore
} // namespace aliyun

namespace pp {
namespace impl {

template<class T>
struct PrettyPrinterCategory<T, typename mp::EnableIf<std::tr1::is_same<T, aliyun::tablestore::core::Action>::value, void>::Type>
{
    typedef aliyun::tablestore::core::Action Category;
};

template<>
struct PrettyPrinter<aliyun::tablestore::core::Action, aliyun::tablestore::core::Action>
{
    void operator()(std::string*, aliyun::tablestore::core::Action) const;
};

template<class T>
struct PrettyPrinterCategory<T, typename mp::EnableIf<std::tr1::is_same<T, aliyun::tablestore::core::PrimaryKeyType>::value, void>::Type>
{
    typedef aliyun::tablestore::core::PrimaryKeyType Category;
};

template<>
struct PrettyPrinter<aliyun::tablestore::core::PrimaryKeyType, aliyun::tablestore::core::PrimaryKeyType>
{
    void operator()(std::string*, aliyun::tablestore::core::PrimaryKeyType) const;
};

template<class T>
struct PrettyPrinterCategory<T, typename mp::EnableIf<std::tr1::is_same<T, aliyun::tablestore::core::PrimaryKeyOption>::value, void>::Type>
{
    typedef aliyun::tablestore::core::PrimaryKeyOption Category;
};

template<>
struct PrettyPrinter<aliyun::tablestore::core::PrimaryKeyOption, aliyun::tablestore::core::PrimaryKeyOption>
{
    void operator()(std::string*, aliyun::tablestore::core::PrimaryKeyOption) const;
};

template<class T>
struct PrettyPrinterCategory<T, typename mp::EnableIf<std::tr1::is_same<T, aliyun::tablestore::core::BloomFilterType>::value, void>::Type>
{
    typedef aliyun::tablestore::core::BloomFilterType Category;
};

template<>
struct PrettyPrinter<aliyun::tablestore::core::BloomFilterType, aliyun::tablestore::core::BloomFilterType>
{
    void operator()(std::string*, aliyun::tablestore::core::BloomFilterType) const;
};

template<class T>
struct PrettyPrinterCategory<T, typename mp::EnableIf<std::tr1::is_same<T, aliyun::tablestore::core::PrimaryKeyValue::Category>::value, void>::Type>
{
    typedef typename aliyun::tablestore::core::PrimaryKeyValue::Category Category;
};

template<>
struct PrettyPrinter<aliyun::tablestore::core::PrimaryKeyValue::Category, aliyun::tablestore::core::PrimaryKeyValue::Category>
{
    void operator()(std::string*, aliyun::tablestore::core::PrimaryKeyValue::Category) const;
};

template<class T>
struct PrettyPrinterCategory<T, typename mp::EnableIf<std::tr1::is_same<T, aliyun::tablestore::core::CompareResult>::value, void>::Type>
{
    typedef typename aliyun::tablestore::core::CompareResult Category;
};

template<>
struct PrettyPrinter<aliyun::tablestore::core::CompareResult, aliyun::tablestore::core::CompareResult>
{
    void operator()(std::string*, aliyun::tablestore::core::CompareResult) const;
};

template<class T>
struct PrettyPrinterCategory<T, typename mp::EnableIf<std::tr1::is_same<T, aliyun::tablestore::core::TableStatus>::value, void>::Type>
{
    typedef typename aliyun::tablestore::core::TableStatus Category;
};

template<>
struct PrettyPrinter<aliyun::tablestore::core::TableStatus, aliyun::tablestore::core::TableStatus>
{
    void operator()(std::string*, aliyun::tablestore::core::TableStatus) const;
};

template<class T>
struct PrettyPrinterCategory<T, typename mp::EnableIf<std::tr1::is_same<T, aliyun::tablestore::core::ColumnType>::value, void>::Type>
{
    typedef typename aliyun::tablestore::core::ColumnType Category;
};

template<>
struct PrettyPrinter<aliyun::tablestore::core::ColumnType, aliyun::tablestore::core::ColumnType>
{
    void operator()(std::string*, aliyun::tablestore::core::ColumnType) const;
};

template<class T>
struct PrettyPrinterCategory<T, typename mp::EnableIf<std::tr1::is_same<T, aliyun::tablestore::core::ReturnType>::value, void>::Type>
{
    typedef typename aliyun::tablestore::core::ReturnType Category;
};

template<>
struct PrettyPrinter<aliyun::tablestore::core::ReturnType, aliyun::tablestore::core::ReturnType>
{
    void operator()(std::string*, aliyun::tablestore::core::ReturnType) const;
};

template<class T>
struct PrettyPrinterCategory<T, typename mp::EnableIf<std::tr1::is_same<T, aliyun::tablestore::core::ColumnValue::Category>::value, void>::Type>
{
    typedef typename aliyun::tablestore::core::ColumnValue::Category Category;
};

template<>
struct PrettyPrinter<aliyun::tablestore::core::ColumnValue::Category, aliyun::tablestore::core::ColumnValue::Category>
{
    void operator()(std::string*, aliyun::tablestore::core::ColumnValue::Category) const;
};

} // namespace impl
} // namespace pp
