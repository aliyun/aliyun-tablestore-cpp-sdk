#pragma once
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
#include "tablestore/core/error.hpp"
#include "tablestore/util/prettyprint.hpp"
#include "tablestore/util/move.hpp"
#include "tablestore/util/mempiece.hpp"
#include "tablestore/util/optional.hpp"
#include "tablestore/util/result.hpp"
#include "tablestore/util/timestamp.hpp"
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

    virtual void prettyPrint(std::string* out) const
    {
        if (size() == 0) {
            out->append("[]");
            return;
        }
        out->push_back('[');
        pp::prettyPrint(out, (*this)[0]);
        for(int64_t i = 1, sz = size(); i < sz; ++i) {
            out->push_back(',');
            pp::prettyPrint(out, (*this)[i]);
        }
        out->push_back(']');
    }

    virtual int64_t size() const =0;
    virtual const Elem& operator[](int64_t idx) const =0;
    virtual Elem& operator[](int64_t idx) =0;
    virtual const Elem& back() const =0;
    virtual Elem& back() =0;
    virtual Elem& append() =0;
    virtual void reset() =0;
};

template<class Elem>
class DequeBasedVector : public IVector<Elem>
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

    void reset()
    {
        mElems.clear();
    }
    
private:
    std::deque<Elem> mElems;
};


class Endpoint
{
public:
    explicit Endpoint() {}
    explicit Endpoint(const std::string& endpoint, const std::string& instance);
    explicit Endpoint(const util::MoveHolder<Endpoint>& a);
    Endpoint& operator=(const util::MoveHolder<Endpoint>& a);

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    const std::string& endpoint() const
    {
        return mEndpoint;
    }

    std::string* mutableEndpoint()
    {
        return &mEndpoint;
    }

    const std::string& instanceName() const
    {
        return mInstanceName;
    }

    std::string* mutableInstanceName()
    {
        return &mInstanceName;
    }

private:
    std::string mEndpoint;
    std::string mInstanceName;
};

class Credential
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
    void reset();

    const std::string& accessKeyId() const
    {
        return mAccessKeyId;
    }

    std::string* mutableAccessKeyId()
    {
        return &mAccessKeyId;
    }

    const std::string& accessKeySecret() const
    {
        return mAccessKeySecret;
    }

    std::string* mutableAccessKeySecret()
    {
        return &mAccessKeySecret;
    }

    const std::string& securityToken() const
    {
        return mSecurityToken;
    }

    std::string* mutableSecurityToken()
    {
        return &mSecurityToken;
    }

private:
    std::string mAccessKeyId;
    std::string mAccessKeySecret;
    std::string mSecurityToken;
};

class ClientOptions
{
public:
    explicit ClientOptions();
    explicit ClientOptions(const util::MoveHolder<ClientOptions>&);

    ClientOptions& operator=(const util::MoveHolder<ClientOptions>&);

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    int64_t maxConnections() const
    {
        return mMaxConnections;
    }

    int64_t* mutableMaxConnections()
    {
        return &mMaxConnections;
    }

    const util::Duration& connectTimeout() const
    {
        return mConnectTimeout;
    }

    util::Duration* mutableConnectTimeout()
    {
        return &mConnectTimeout;
    }

    const util::Duration& requestTimeout() const
    {
        return mRequestTimeout;
    }

    util::Duration* mutableRequestTimeout()
    {
        return &mRequestTimeout;
    }

    const util::Duration& traceThreshold() const
    {
        return mTraceThreshold;
    }

    util::Duration* mutableTraceThreshold()
    {
        return &mTraceThreshold;
    }

    bool checkResponseDigest() const
    {
        return mCheckResponseDigest;
    }

    bool* mutableCheckResponseDigest()
    {
        return &mCheckResponseDigest;
    }

    const std::tr1::shared_ptr<IRetryStrategy> retryStrategy() const
    {
        return mRetryStrategy;
    }

    std::tr1::shared_ptr<IRetryStrategy>* mutableRetryStrategy()
    {
        return &mRetryStrategy;
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
class PrimaryKeyColumnSchema
{
public:
    explicit PrimaryKeyColumnSchema()
      : mType(PKT_INTEGER),
        mOption(PKO_NONE)
    {}

    explicit PrimaryKeyColumnSchema(const std::string& name, PrimaryKeyType type)
      : mName(name),
        mType(type),
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

    std::string* mutableName()
    {
        return &mName;
    }

    PrimaryKeyType type() const
    {
        return mType;
    }

    PrimaryKeyType* mutableType()
    {
        return &mType;
    }

    PrimaryKeyOption option() const
    {
        return mOption;
    }

    PrimaryKeyOption* mutableOption()
    {
        return &mOption;
    }
    
    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

private:
    std::string mName;
    PrimaryKeyType mType;
    PrimaryKeyOption mOption;
};

class Schema: public IVector<PrimaryKeyColumnSchema>
{
public:
    explicit Schema() {}

    explicit Schema(const util::MoveHolder<Schema>& a)
    {
        util::moveAssign(&mColumns, util::move(a->mColumns));
    }

    Schema& operator=(const util::MoveHolder<Schema>& a)
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

    void reset()
    {
        return mColumns.reset();
    }

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;

private:
    DequeBasedVector<PrimaryKeyColumnSchema> mColumns;
};

class PrimaryKeyValue
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
    void reset();
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
    int64_t* mutableInteger();

public:
    // for string
    /**
     * PrimaryKeyValue does not own underlying piece of memory of the string.
     * So, make sure it lives longer than the PrimaryKeyValue.
     */
    static PrimaryKeyValue toStr(const util::MemPiece&);
    const util::MemPiece& str() const;
    util::MemPiece* mutableStr();

public:
    // for blob
    /**
     * PrimaryKeyValue does not own underlying piece of memory of the blob.
     * So, make sure it lives longer than the PrimaryKeyValue.
     */
    static PrimaryKeyValue toBlob(const util::MemPiece&);
    const util::MemPiece& blob() const;
    util::MemPiece* mutableBlob();

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
class PrimaryKeyColumn
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

    std::string* mutableName()
    {
        return &mName;
    }

    const PrimaryKeyValue& value() const
    {
        return mValue;
    }

    PrimaryKeyValue* mutableValue()
    {
        return &mValue;
    }

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

private:
    std::string mName;
    PrimaryKeyValue mValue;
};

class PrimaryKey: public IVector<PrimaryKeyColumn>
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

    void reset()
    {
        return mColumns.reset();
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
class TableMeta
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

    std::string* mutableTableName()
    {
        return &mTableName;
    }

    const Schema& schema() const
    {
        return mSchema;
    }

    Schema* mutableSchema()
    {
        return &mSchema;
    }

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

private:
    std::string mTableName;
    Schema mSchema;
};

class CapacityUnit
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

    util::Optional<int64_t>* mutableRead()
    {
        return &mRead;
    }

    const util::Optional<int64_t>& write() const
    {
        return mWrite;
    }

    util::Optional<int64_t>* mutableWrite()
    {
        return &mWrite;
    }

    util::Optional<Error> validate() const;
    void prettyPrint(std::string*) const;
    void reset();

private:
    util::Optional<int64_t> mRead;
    util::Optional<int64_t> mWrite;
};

/**
 * Options of tables, which can be updated by UpdateTable
 */
class TableOptions
{
public:
    explicit TableOptions() {}

    explicit TableOptions(const util::MoveHolder<TableOptions>& a)
    {
        *this = a;
    }

    TableOptions& operator=(const util::MoveHolder<TableOptions>& a);

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    const util::Optional<util::Duration>& timeToLive() const
    {
        return mTimeToLive;
    }

    util::Optional<util::Duration>* mutableTimeToLive()
    {
        return &mTimeToLive;
    }

    const util::Optional<int64_t> maxVersions() const
    {
        return mMaxVersions;
    }

    util::Optional<int64_t>* mutableMaxVersions()
    {
        return &mMaxVersions;
    }

    const util::Optional<BloomFilterType> bloomFilterType() const
    {
        return mBloomFilterType;
    }

    util::Optional<BloomFilterType>* mutableBloomFilterType()
    {
        return &mBloomFilterType;
    }

    const util::Optional<int64_t> blockSize() const
    {
        return mBlockSize;
    }

    util::Optional<int64_t>* mutableBlockSize()
    {
        return &mBlockSize;
    }

    const util::Optional<util::Duration>& maxTimeDeviation() const
    {
        return mMaxTimeDeviation;
    }

    util::Optional<util::Duration>* mutableMaxTimeDeviation()
    {
        return &mMaxTimeDeviation;
    }

    const util::Optional<CapacityUnit>& reservedThroughput() const
    {
        return mReservedThroughput;
    }

    util::Optional<CapacityUnit>* mutableReservedThroughput()
    {
        return &mReservedThroughput;
    }
    
private:
    util::Optional<CapacityUnit> mReservedThroughput;
    util::Optional<util::Duration> mTimeToLive;
    util::Optional<int64_t> mMaxVersions;
    util::Optional<BloomFilterType> mBloomFilterType;
    util::Optional<int64_t> mBlockSize;
    util::Optional<util::Duration> mMaxTimeDeviation;
};

class AttributeValue
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

private:
    explicit AttributeValue(int64_t);
    explicit AttributeValue(Category, const util::MemPiece&);
    explicit AttributeValue(bool);
    explicit AttributeValue(double);

public:
    explicit AttributeValue();
    explicit AttributeValue(const util::MoveHolder<AttributeValue>& a)
    {
        *this = a;
    }
    AttributeValue& operator=(const util::MoveHolder<AttributeValue>&);

    Category category() const
    {
        return mCategory;
    }

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();
    CompareResult compare(const AttributeValue&) const;
    
public:
    // for strings
    /**
     * AttributeValue does not own underlying piece of memory of the string.
     * So, make sure it lives longer than the AttributeValue.
     */
    static AttributeValue toStr(const util::MemPiece&);
    const util::MemPiece& str() const;
    util::MemPiece* mutableStr();

public:
    // for blob
    /**
     * AttributeValue does not own underlying piece of memory of the blob.
     * So, make sure it lives longer than the AttributeValue.
     */
    static AttributeValue toBlob(const util::MemPiece&);
    const util::MemPiece& blob() const;
    util::MemPiece* mutableBlob();

public:
    // for integers
    static AttributeValue toInteger(int64_t);
    int64_t integer() const;
    int64_t* mutableInteger();

public:
    // for floating point numbers
    static AttributeValue toFloatPoint(double);
    double floatPoint() const;
    double* mutableFloatPoint();

public:
    // for booleans
    static AttributeValue toBoolean(bool);
    bool boolean() const;
    bool* mutableBoolean();

private:
    Category mCategory;
    int64_t mIntValue;
    util::MemPiece mStrBlobValue;
    bool mBoolValue;
    double mFloatingValue;
};


class Attribute
{
public:
    explicit Attribute() {}
    explicit Attribute(const std::string&, const AttributeValue&);
    explicit Attribute(const std::string&, const AttributeValue&, util::UtcTime);

    explicit Attribute(const util::MoveHolder<Attribute>& a)
    {
        *this = a;
    }

    Attribute& operator=(const util::MoveHolder<Attribute>&);

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();
    
    const std::string& name() const
    {
        return mName;
    }

    std::string* mutableName()
    {
        return &mName;
    }

    const AttributeValue& value() const
    {
        return mValue;
    }

    AttributeValue* mutableValue()
    {
        return &mValue;
    }

    const util::Optional<util::UtcTime>& timestamp() const
    {
        return mTimestamp;
    }

    util::Optional<util::UtcTime>* mutableTimestamp()
    {
        return &mTimestamp;
    }
    
private:
    std::string mName;
    AttributeValue mValue;
    util::Optional<util::UtcTime> mTimestamp;
};

class Row
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
    void reset();

    const PrimaryKey& primaryKey() const
    {
        return mPkey;
    }

    PrimaryKey* mutablePrimaryKey()
    {
        return &mPkey;
    }

    const IVector<Attribute>& attributes() const
    {
        return mAttrs;
    }

    IVector<Attribute>* mutableAttributes()
    {
        return &mAttrs;
    }

private:
    PrimaryKey mPkey;
    DequeBasedVector<Attribute> mAttrs;
};

/**
 * a range of UTC time.
 * Both (inclusive) start and (exclusive) end  must be integral multiple of milliseconds.
 */
class TimeRange
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
    void reset();
    
    util::UtcTime start() const
    {
        return mStart;
    }

    util::UtcTime* mutableStart()
    {
        return &mStart;
    }

    util::UtcTime end() const
    {
        return mEnd;
    }

    util::UtcTime* mutableEnd()
    {
        return &mEnd;
    }

private:
    util::UtcTime mStart;
    util::UtcTime mEnd;
};

class Split
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
    void reset();

    /**
     * The (inclusive) lower bound of the split, as the same length as the primary key columns of the table.
     */
    const std::tr1::shared_ptr<PrimaryKey>& lowerBound() const
    {
        return mLowerBound;
    }

    std::tr1::shared_ptr<PrimaryKey>* mutableLowerBound()
    {
        return &mLowerBound;
    }

    /**
     * The (exclusive) upper bound of the split, as the same length as the primary key columns of the table.
     */
    const std::tr1::shared_ptr<PrimaryKey>& upperBound() const
    {
        return mUpperBound;
    }

    std::tr1::shared_ptr<PrimaryKey>* mutableUpperBound()
    {
        return &mUpperBound;
    }

    /**
     * A hint of the location where the split lies in.
     * If a location is not comfortable to be seen, it will keep empty.
     */
    const std::string location() const
    {
        return mLocation;
    }

    std::string* mutableLocation()
    {
        return &mLocation;
    }
    
private:
    std::tr1::shared_ptr<PrimaryKey> mLowerBound;
    std::tr1::shared_ptr<PrimaryKey> mUpperBound;
    std::string mLocation;
};


// conditions

class ColumnCondition
{
public:
    enum Type {
        SINGLE = 1,
        COMPOSITE
    };

    virtual ~ColumnCondition() {}
    virtual Type type() const =0;
    virtual void prettyPrint(std::string*) const =0;
    virtual util::Optional<Error> validate() const =0;
    virtual void reset() =0;
};

class SingleColumnCondition : public ColumnCondition
{
public:
    enum Relation {
        EQUAL = 1, 
        NOT_EQUAL,
        GREATER_THAN,
        GREATER_EQUAL,
        LESS_THAN, 
        LESS_EQUAL,
    };

public:
    explicit SingleColumnCondition()
      : mRelation(EQUAL),
        mPassIfMissing(false),
        mLatestVersionOnly(true)
    {}
    
    explicit SingleColumnCondition(
        const std::string& columnName,
        Relation rel,
        const AttributeValue& columnValue)
      : mColumnName(columnName),
        mRelation(rel),
        mColumnValue(columnValue),
        mPassIfMissing(false),
        mLatestVersionOnly(true)
    {}

    explicit SingleColumnCondition(
        const util::MoveHolder<SingleColumnCondition>& a)
    {
        *this = a;
    }

    SingleColumnCondition& operator=(
        const util::MoveHolder<SingleColumnCondition>&);

    ColumnCondition::Type type() const
    {
        return ColumnCondition::SINGLE;
    }

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    const std::string& columnName() const
    {
        return mColumnName;
    }

    std::string* mutableColumnName()
    {
        return &mColumnName;
    }

    Relation relation() const
    {
        return mRelation;
    }

    Relation* mutableRelation()
    {
        return &mRelation;
    }

    const AttributeValue& columnValue() const
    {
        return mColumnValue;
    }

    AttributeValue* mutableAttributeValue()
    {
        return &mColumnValue;
    }

    bool passIfMissing() const
    {
        return mPassIfMissing;
    }

    bool* mutablePassIfMissing()
    {
        return &mPassIfMissing;
    }

    bool latestVersionOnly() const
    {
        return mLatestVersionOnly;
    }

    bool* mutableLatestVersionOnly()
    {
        return &mLatestVersionOnly;
    }

private:
    std::string mColumnName;
    Relation mRelation;
    AttributeValue mColumnValue;
    bool mPassIfMissing;
    bool mLatestVersionOnly;
};

class CompositeColumnCondition : public ColumnCondition
{
public:
    enum Operator {
        NOT = 1,
        AND = 2, 
        OR  = 3
    };

public:
    explicit CompositeColumnCondition()
      : mOperator(AND)
    {}

    explicit CompositeColumnCondition(
        const util::MoveHolder<CompositeColumnCondition>& a)
    {
        *this = a;
    }

    CompositeColumnCondition& operator=(
        const util::MoveHolder<CompositeColumnCondition>&);
    
    Type type() const
    {
        return COMPOSITE;
    }

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    Operator op() const
    {
        return mOperator;
    }

    Operator* mutableOp()
    {
        return &mOperator;
    }

    const IVector<std::tr1::shared_ptr<ColumnCondition> >& children() const
    {
        return mChildren;
    }

    IVector<std::tr1::shared_ptr<ColumnCondition> >* mutableChildren()
    {
        return &mChildren;
    }

private:
    Operator mOperator;
    DequeBasedVector<std::tr1::shared_ptr<ColumnCondition> > mChildren;
};

class Condition
{
public:
    enum RowExistenceExpectation
    {
        IGNORE = 0,
        EXPECT_EXIST,
        EXPECT_NOT_EXIST,
    };

public:
    explicit Condition()
      : mRowCondition(IGNORE)
    {}

    explicit Condition(const util::MoveHolder<Condition>& a)
    {
        *this = a;
    }

    Condition& operator=(const util::MoveHolder<Condition>&);

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    RowExistenceExpectation rowCondition() const
    {
        return mRowCondition;
    }

    RowExistenceExpectation* mutableRowCondition()
    {
        return &mRowCondition;
    }

    const std::tr1::shared_ptr<ColumnCondition>& columnCondition() const
    {
        return mColumnCondition;
    }

    std::tr1::shared_ptr<ColumnCondition>* mutableColumnCondition()
    {
        return &mColumnCondition;
    }

private:
    RowExistenceExpectation mRowCondition;
    std::tr1::shared_ptr<ColumnCondition> mColumnCondition;
};

// row changes

class RowChange
{
public:
    enum ReturnType
    {
        RT_NONE,
        RT_PRIMARY_KEY,
    };
    
protected:
    explicit RowChange()
      : mReturnType(RT_NONE)
    {}

    void move(RowChange& a);
    virtual void prettyPrint(std::string*) const;
    
public:
    virtual ~RowChange() {}
    virtual util::Optional<Error> validate() const;
    virtual void reset();

    const std::string& table() const
    {
        return mTable;
    }

    std::string* mutableTable()
    {
        return &mTable;
    }

    const PrimaryKey& primaryKey() const
    {
        return mPrimaryKey;
    }

    PrimaryKey* mutablePrimaryKey()
    {
        return &mPrimaryKey;
    }

    const Condition& condition() const
    {
        return mCondition;
    }

    Condition* mutableCondition()
    {
        return &mCondition;
    }

    ReturnType returnType() const
    {
        return mReturnType;
    }

    ReturnType* mutableReturnType()
    {
        return &mReturnType;
    }

private:
    std::string mTable;
    PrimaryKey mPrimaryKey;
    Condition mCondition;
    ReturnType mReturnType;
};

class RowPutChange: public RowChange
{
public:
    explicit RowPutChange() {}
    explicit RowPutChange(const util::MoveHolder<RowPutChange>& a)
    {
        *this = a;
    }

    RowPutChange& operator=(const util::MoveHolder<RowPutChange>&);

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();
    
    const IVector<Attribute>& attributes() const
    {
        return mAttrs;
    }

    IVector<Attribute>* mutableAttributes()
    {
        return &mAttrs;
    }

private:
    DequeBasedVector<Attribute> mAttrs;
};

class RowUpdateChange : public RowChange
{
public:
    class Update
    {
    public:
        enum Type
        {
            /**
             * Overwrites a cell with a specific timestamp.
             * If the cell does not previously exist, insert it.
             */
            PUT,
            /**
             * Deletes a single cell with a specific timestamp.
             */
            DELETE,
            /**
             * Deletes all cells of a column
             */
            DELETE_ALL,
        };

    public:
        explicit Update()
          : mType(PUT)
        {}

        explicit Update(const util::MoveHolder<Update>& a)
        {
            *this = a;
        }

        Update& operator=(const util::MoveHolder<Update>&);
        void prettyPrint(std::string*) const;
        util::Optional<Error> validate() const;

        Type type() const
        {
            return mType;
        }

        Type* mutableType()
        {
            return &mType;
        }

        const std::string& attrName() const
        {
            return mAttrName;
        }

        std::string* mutableAttrName()
        {
            return &mAttrName;
        }

        const util::Optional<AttributeValue>& attrValue() const
        {
            return mAttrValue;
        }

        util::Optional<AttributeValue>* mutableAttrValue()
        {
            return &mAttrValue;
        }

        const util::Optional<util::UtcTime>& timestamp() const
        {
            return mTimestamp;
        }

        util::Optional<util::UtcTime>* mutableTimestamp()
        {
            return &mTimestamp;
        }

    private:
        Type mType;
        std::string mAttrName;
        util::Optional<AttributeValue> mAttrValue;
        util::Optional<util::UtcTime> mTimestamp;
    };
    
public:
    explicit RowUpdateChange() {}

    explicit RowUpdateChange(const util::MoveHolder<RowUpdateChange>& a)
    {
        *this = a;
    }

    RowUpdateChange& operator=(const util::MoveHolder<RowUpdateChange>&);
    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    const IVector<Update>& updates() const
    {
        return mUpdates;
    }

    IVector<Update>* mutableUpdates()
    {
        return &mUpdates;
    }

private:
    DequeBasedVector<Update> mUpdates;
};

class RowDeleteChange : public RowChange
{
public:
    explicit RowDeleteChange() {}

    explicit RowDeleteChange(const util::MoveHolder<RowDeleteChange>& a)
    {
        *this = a;
    }

    RowDeleteChange& operator=(const util::MoveHolder<RowDeleteChange>& a)
    {
        RowChange::move(*a);
        return *this;
    }

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();
};

/**
 * User data will pass from operations in requests to their results in responses.
 * It is an easy facility for users to identify operations and their results.
 */
template<class T>
class PairWithUserData
{
public:
    typedef T ValueType;

    explicit PairWithUserData()
      : mUserData(NULL)
    {}

    explicit PairWithUserData(const util::MoveHolder<PairWithUserData<T> >& a)
    {
        *this = a;
    }

    PairWithUserData<T>& operator=(
        const util::MoveHolder<PairWithUserData<T> >& a)
    {
        util::moveAssign(&mData, util::move(a->mData));
        mUserData = a->mUserData;
        return *this;
    }

    void prettyPrint(std::string* out) const
    {
        pp::prettyPrint(out, get());
    }

    const T& get() const
    {
        return mData;
    }

    T* mutableGet()
    {
        return &mData;
    }

    const void* userData() const
    {
        return mUserData;
    }

    void const** mutableUserData()
    {
        return &mUserData;
    }

private:
    T mData;
    const void* mUserData;
};

// query criteria

class QueryCriterion
{
protected:
    explicit QueryCriterion() {}
    QueryCriterion& operator=(const util::MoveHolder<QueryCriterion>&);

public:
    virtual ~QueryCriterion() {}
    virtual void prettyPrint(std::string*) const;
    virtual util::Optional<Error> validate() const;
    virtual void reset();
    
    const std::string& table() const
    {
        return mTable;
    }

    std::string* mutableTable()
    {
        return &mTable;
    }

    const IVector<std::string>& columnsToGet() const
    {
        return mColumnsToGet;
    }

    IVector<std::string>* mutableColumnsToGet()
    {
        return &mColumnsToGet;
    }

    const util::Optional<int64_t>& maxVersions() const
    {
        return mMaxVersions;
    }

    util::Optional<int64_t>* mutableMaxVersions()
    {
        return &mMaxVersions;
    }

    const util::Optional<TimeRange>& timeRange() const
    {
        return mTimeRange;
    }

    util::Optional<TimeRange>* mutableTimeRange()
    {
        return &mTimeRange;
    }

    const util::Optional<bool>& cacheBlocks() const
    {
        return mCacheBlocks;
    }

    util::Optional<bool>* mutableCacheBlocks()
    {
        return &mCacheBlocks;
    }

    const std::tr1::shared_ptr<ColumnCondition>& filter() const
    {
        return mFilter;
    }

    std::tr1::shared_ptr<ColumnCondition>* mutableFilter()
    {
        return &mFilter;
    }

private:
    std::string mTable;
    DequeBasedVector<std::string> mColumnsToGet;
    util::Optional<int64_t> mMaxVersions;
    util::Optional<TimeRange> mTimeRange;
    util::Optional<bool> mCacheBlocks;
    std::tr1::shared_ptr<ColumnCondition> mFilter;
};

class PointQueryCriterion : public QueryCriterion
{
public:
    explicit PointQueryCriterion() {}

    explicit PointQueryCriterion(
        const util::MoveHolder<PointQueryCriterion>& a)
    {
        *this = a;
    }

    PointQueryCriterion& operator=(const util::MoveHolder<PointQueryCriterion>&);

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    const PrimaryKey& primaryKey() const
    {
        return mPrimaryKey;
    }

    PrimaryKey* mutablePrimaryKey()
    {
        return &mPrimaryKey;
    }

private:
    PrimaryKey mPrimaryKey;
};

class RangeQueryCriterion : public QueryCriterion
{
public:
    enum Direction
    {
        FORWARD,
        BACKWARD
    };

public:
    explicit RangeQueryCriterion()
      : mDirection(FORWARD)
    {}

    explicit RangeQueryCriterion(const util::MoveHolder<RangeQueryCriterion>& a)
    {
        *this = a;
    }

    RangeQueryCriterion& operator=(const util::MoveHolder<RangeQueryCriterion>&);
    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    Direction direction() const
    {
        return mDirection;
    }

    Direction* mutableDirection()
    {
        return &mDirection;
    }

    const PrimaryKey& inclusiveStart() const
    {
        return mInclusiveStart;
    }

    PrimaryKey* mutableInclusiveStart()
    {
        return &mInclusiveStart;
    }

    const PrimaryKey& exclusiveEnd() const
    {
        return mExclusiveEnd;
    }

    PrimaryKey* mutableExclusiveEnd()
    {
        return &mExclusiveEnd;
    }

    const util::Optional<int64_t>& limit() const
    {
        return mLimit;
    }

    util::Optional<int64_t>* mutableLimit()
    {
        return &mLimit;
    }

private:
    Direction mDirection;
    PrimaryKey mInclusiveStart;
    PrimaryKey mExclusiveEnd;
    util::Optional<int64_t> mLimit;
};

class MultiPointQueryCriterion: public QueryCriterion
{
public:
    typedef PairWithUserData<PrimaryKey> RowKey;

public:
    explicit MultiPointQueryCriterion() {}

    explicit MultiPointQueryCriterion(
        const util::MoveHolder<MultiPointQueryCriterion>&);
    MultiPointQueryCriterion& operator=(
        const util::MoveHolder<MultiPointQueryCriterion>&);
    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    const IVector<RowKey>& rowKeys() const
    {
        return mRowKeys;
    }

    IVector<RowKey>* mutableRowKeys()
    {
        return &mRowKeys;
    }
    
private:
    DequeBasedVector<RowKey> mRowKeys;
};

// requests and responses

class Response
{
protected:
    Response& operator=(const util::MoveHolder<Response>&);
    void prettyPrint(std::string*) const;
    void reset();

public:
    const std::string& requestId() const
    {
        return mRequestId;
    }

    std::string* mutableRequestId()
    {
        return &mRequestId;
    }

    const std::string& traceId() const
    {
        return mTraceId;
    }

    std::string* mutableTraceId()
    {
        return &mTraceId;
    }

    
    /**
     * for internal usage only
     */
    IVector<std::string>* mutableMemHolder()
    {
        return &mMemHolder;
    }
    
private:
    std::string mRequestId;
    std::string mTraceId;
    DequeBasedVector<std::string> mMemHolder;
};

class CreateTableRequest
{
public:
    explicit CreateTableRequest();
    explicit CreateTableRequest(const util::MoveHolder<CreateTableRequest>& a)
    {
        *this = a;
    }

    CreateTableRequest& operator=(const util::MoveHolder<CreateTableRequest>&);

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    const TableMeta& meta() const
    {
        return mMeta;
    }

    TableMeta* mutableMeta()
    {
        return &mMeta;
    }

    const TableOptions& options() const
    {
        return mOptions;
    }

    TableOptions* mutableOptions()
    {
        return &mOptions;
    }

    /**
     * For now, each shard split point must contains exactly one primary 
     * key column which conforms to the table schema.
     */
    const IVector<PrimaryKey>& shardSplitPoints() const
    {
        return mShardSplitPoints;
    }

    IVector<PrimaryKey>* mutableShardSplitPoints()
    {
        return &mShardSplitPoints;
    }

private:
    TableMeta mMeta;
    TableOptions mOptions;
    DequeBasedVector<PrimaryKey> mShardSplitPoints;
};

class CreateTableResponse: public Response
{
public:
    explicit CreateTableResponse() {}
    explicit CreateTableResponse(const util::MoveHolder<CreateTableResponse>& a)
    {
        *this = a;
    }

    CreateTableResponse& operator=(const util::MoveHolder<CreateTableResponse>& a);

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();
};


class ListTableRequest
{
public:
    explicit ListTableRequest() {}
    explicit ListTableRequest(const util::MoveHolder<ListTableRequest>&) {}

    ListTableRequest& operator=(const util::MoveHolder<ListTableRequest>&)
    {
        return *this;
    }

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();
};

class ListTableResponse: public Response
{
public:
    explicit ListTableResponse() {}
    explicit ListTableResponse(const util::MoveHolder<ListTableResponse>&);
    ListTableResponse& operator=(const util::MoveHolder<ListTableResponse>&);

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();
    
    const IVector<std::string>& tables() const
    {
        return mTables;
    }

    IVector<std::string>* mutableTables()
    {
        return &mTables;
    }
    
private:
    DequeBasedVector<std::string> mTables;
};


class DeleteTableRequest
{
public:
    explicit DeleteTableRequest() {}
    explicit DeleteTableRequest(const util::MoveHolder<DeleteTableRequest>&);

    DeleteTableRequest& operator=(const util::MoveHolder<DeleteTableRequest>&);

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    const std::string& table() const
    {
        return mTable;
    }

    std::string* mutableTable()
    {
        return &mTable;
    }

private:
    std::string mTable;
};

class DeleteTableResponse: public Response
{
public:
    explicit DeleteTableResponse() {}
    explicit DeleteTableResponse(const util::MoveHolder<DeleteTableResponse>&);
    DeleteTableResponse& operator=(const util::MoveHolder<DeleteTableResponse>&);

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();
};

class DescribeTableRequest
{
public:
    explicit DescribeTableRequest() {}
    explicit DescribeTableRequest(const util::MoveHolder<DescribeTableRequest>&);
    DescribeTableRequest& operator=(const util::MoveHolder<DescribeTableRequest>&);

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    const std::string& table() const
    {
        return mTable;
    }

    std::string* mutableTable()
    {
        return &mTable;
    }

private:
    std::string mTable;
};

class DescribeTableResponse: public Response
{
public:
    explicit DescribeTableResponse()
      : mStatus(ACTIVE)
    {}
    explicit DescribeTableResponse(const util::MoveHolder<DescribeTableResponse>&);
    DescribeTableResponse& operator=(const util::MoveHolder<DescribeTableResponse>&);

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    const TableMeta& meta() const
    {
        return mMeta;
    }

    TableMeta* mutableMeta()
    {
        return &mMeta;
    }

    const TableOptions& options() const
    {
        return mOptions;
    }

    TableOptions* mutableOptions()
    {
        return &mOptions;
    }

    TableStatus status() const
    {
        return mStatus;
    }

    TableStatus* mutableStatus()
    {
        return &mStatus;
    }

    const IVector<PrimaryKey>& shardSplitPoints() const
    {
        return mShardSplitPoints;
    }

    IVector<PrimaryKey>* mutableShardSplitPoints()
    {
        return &mShardSplitPoints;
    }

private:
    TableMeta mMeta;
    TableOptions mOptions;
    TableStatus mStatus;
    DequeBasedVector<PrimaryKey> mShardSplitPoints;
};

class UpdateTableRequest
{
public:
    explicit UpdateTableRequest() {}
    explicit UpdateTableRequest(const util::MoveHolder<UpdateTableRequest>&);
    UpdateTableRequest& operator=(const util::MoveHolder<UpdateTableRequest>&);

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    const std::string& table() const
    {
        return mTable;
    }

    std::string* mutableTable()
    {
        return &mTable;
    }

    const TableOptions& options() const
    {
        return mOptions;
    }

    TableOptions* mutableOptions()
    {
        return &mOptions;
    }
    
private:
    std::string mTable;
    TableOptions mOptions;
};

class UpdateTableResponse: public Response
{
public:
    explicit UpdateTableResponse() {}
    explicit UpdateTableResponse(const util::MoveHolder<UpdateTableResponse>&);
    UpdateTableResponse& operator=(const util::MoveHolder<UpdateTableResponse>&);

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();
};

class ComputeSplitsBySizeRequest
{
    static const int64_t kDefaultSplitSize = 5; // 500MB

public:
    explicit ComputeSplitsBySizeRequest()
      : mSplitSize(kDefaultSplitSize)
    {}

    explicit ComputeSplitsBySizeRequest(const util::MoveHolder<ComputeSplitsBySizeRequest>&);
    ComputeSplitsBySizeRequest& operator=(const util::MoveHolder<ComputeSplitsBySizeRequest>&);

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    const std::string& table() const
    {
        return mTable;
    }

    std::string* mutableTable()
    {
        return &mTable;
    }

    int64_t splitSize() const
    {
        return mSplitSize;
    }

    int64_t* mutableSplitSize()
    {
        return &mSplitSize;
    }

private:
    std::string mTable;
    int64_t mSplitSize;
};

class ComputeSplitsBySizeResponse: public Response
{
public:
    explicit ComputeSplitsBySizeResponse() {}
    explicit ComputeSplitsBySizeResponse(
        const util::MoveHolder<ComputeSplitsBySizeResponse>&);
    ComputeSplitsBySizeResponse& operator=(
        const util::MoveHolder<ComputeSplitsBySizeResponse>&);

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    const CapacityUnit& consumedCapacity() const
    {
        return mConsumedCapacity;
    }

    CapacityUnit* mutableConsumedCapacity()
    {
        return &mConsumedCapacity;
    }

    const Schema& schema() const
    {
        return mSchema;
    }

    Schema* mutableSchema()
    {
        return &mSchema;
    }

    const IVector<Split>& splits() const
    {
        return mSplits;
    }

    IVector<Split>* mutableSplits()
    {
        return &mSplits;
    }

private:
    CapacityUnit mConsumedCapacity;
    Schema mSchema;
    DequeBasedVector<Split> mSplits;
};

class PutRowRequest
{
public:
    explicit PutRowRequest() {}
    explicit PutRowRequest(const util::MoveHolder<PutRowRequest>& a)
    {
        *this = a;
    }

    PutRowRequest& operator=(const util::MoveHolder<PutRowRequest>&);

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    const RowPutChange& rowChange() const
    {
        return mRowChange;
    }

    RowPutChange* mutableRowChange()
    {
        return &mRowChange;
    }
    
private:
    RowPutChange mRowChange;
};

class PutRowResponse: public Response
{
public:
    explicit PutRowResponse() {}
    explicit PutRowResponse(const util::MoveHolder<PutRowResponse>& a)
    {
        *this = a;
    }

    PutRowResponse& operator=(const util::MoveHolder<PutRowResponse>&);

    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();
    
    const CapacityUnit& consumedCapacity() const
    {
        return mConsumedCapacity;
    }

    CapacityUnit* mutableConsumedCapacity()
    {
        return &mConsumedCapacity;
    }

    const util::Optional<Row>& row() const
    {
        return mRow;
    }

    util::Optional<Row>* mutableRow()
    {
        return &mRow;
    }

private:
    CapacityUnit mConsumedCapacity;
    util::Optional<Row> mRow;
};

class GetRowRequest
{
public:
    explicit GetRowRequest() {}

    explicit GetRowRequest(const util::MoveHolder<GetRowRequest>& a)
    {
        *this = a;
    }

    GetRowRequest& operator=(const util::MoveHolder<GetRowRequest>&);
    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    const PointQueryCriterion& queryCriterion() const
    {
        return mQueryCriterion;
    }

    PointQueryCriterion* mutableQueryCriterion()
    {
        return &mQueryCriterion;
    }

private:
    PointQueryCriterion mQueryCriterion;
};

class GetRowResponse: public Response
{
public:
    explicit GetRowResponse() {}

    explicit GetRowResponse(const util::MoveHolder<GetRowResponse>& a)
    {
        *this = a;
    }

    GetRowResponse& operator=(const util::MoveHolder<GetRowResponse>& a);
    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    const CapacityUnit& consumedCapacity() const
    {
        return mConsumedCapacity;
    }

    CapacityUnit* mutableConsumedCapacity()
    {
        return &mConsumedCapacity;
    }

    const util::Optional<Row>& row() const
    {
        return mRow;
    }

    util::Optional<Row>* mutableRow()
    {
        return &mRow;
    }

private:
    CapacityUnit mConsumedCapacity;
    util::Optional<Row> mRow;
};

class GetRangeRequest
{
public:
    explicit GetRangeRequest() {}

    explicit GetRangeRequest(const util::MoveHolder<GetRangeRequest>& a)
    {
        *this = a;
    }

    GetRangeRequest& operator=(const util::MoveHolder<GetRangeRequest>&);
    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    const RangeQueryCriterion& queryCriterion() const
    {
        return mQueryCriterion;
    }

    RangeQueryCriterion* mutableQueryCriterion()
    {
        return &mQueryCriterion;
    }
    
private:
    RangeQueryCriterion mQueryCriterion;
};

class GetRangeResponse : public Response
{
public:
    explicit GetRangeResponse() {}

    explicit GetRangeResponse(const util::MoveHolder<GetRangeResponse>& a)
    {
        *this = a;
    }

    GetRangeResponse& operator=(const util::MoveHolder<GetRangeResponse>&);
    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    const CapacityUnit& consumedCapacity() const
    {
        return mConsumedCapacity;
    }

    CapacityUnit* mutableConsumedCapacity()
    {
        return &mConsumedCapacity;
    }

    const IVector<Row>& rows() const
    {
        return mRows;
    }

    IVector<Row>* mutableRows()
    {
        return &mRows;
    }

    const util::Optional<PrimaryKey>& nextStart() const
    {
        return mNextStart;
    }

    util::Optional<PrimaryKey>* mutableNextStart()
    {
        return &mNextStart;
    }

private:
    CapacityUnit mConsumedCapacity;
    DequeBasedVector<Row> mRows;
    util::Optional<PrimaryKey> mNextStart;
};

class UpdateRowRequest
{
public:
    explicit UpdateRowRequest() {}

    explicit UpdateRowRequest(const util::MoveHolder<UpdateRowRequest>& a)
    {
        *this = a;
    }

    UpdateRowRequest& operator=(const util::MoveHolder<UpdateRowRequest>&);
    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    const RowUpdateChange& rowChange() const
    {
        return mRowChange;
    }

    RowUpdateChange* mutableRowChange()
    {
        return &mRowChange;
    }

private:
    RowUpdateChange mRowChange;
};

class UpdateRowResponse: public Response
{
public:
    explicit UpdateRowResponse() {}

    explicit UpdateRowResponse(const util::MoveHolder<UpdateRowResponse>& a)
    {
        *this = a;
    }

    UpdateRowResponse& operator=(const util::MoveHolder<UpdateRowResponse>&);
    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    const CapacityUnit& consumedCapacity() const
    {
        return mConsumedCapacity;
    }

    CapacityUnit* mutableConsumedCapacity()
    {
        return &mConsumedCapacity;
    }

    const util::Optional<Row>& row() const
    {
        return mRow;
    }

    util::Optional<Row>* mutableRow()
    {
        return &mRow;
    }

private:
    CapacityUnit mConsumedCapacity;
    util::Optional<Row> mRow;
};

class DeleteRowRequest
{
public:
    explicit DeleteRowRequest() {}

    explicit DeleteRowRequest(const util::MoveHolder<DeleteRowRequest>& a)
    {
        *this = a;
    }

    DeleteRowRequest& operator=(const util::MoveHolder<DeleteRowRequest>& a);
    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    const RowDeleteChange& rowChange() const
    {
        return mRowChange;
    }

    RowDeleteChange* mutableRowChange()
    {
        return &mRowChange;
    }

private:
    RowDeleteChange mRowChange;
};

class DeleteRowResponse : public Response
{
public:
    explicit DeleteRowResponse() {}

    explicit DeleteRowResponse(const util::MoveHolder<DeleteRowResponse>& a)
    {
        *this = a;
    }

    DeleteRowResponse& operator=(const util::MoveHolder<DeleteRowResponse>&);
    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    const CapacityUnit& consumedCapacity() const
    {
        return mConsumedCapacity;
    }

    CapacityUnit* mutableConsumedCapacity()
    {
        return &mConsumedCapacity;
    }

    const util::Optional<Row>& row() const
    {
        return mRow;
    }

    util::Optional<Row>* mutableRow()
    {
        return &mRow;
    }

private:
    CapacityUnit mConsumedCapacity;
    util::Optional<Row> mRow;
};

class BatchGetRowRequest
{
public:
    explicit BatchGetRowRequest() {}
    explicit BatchGetRowRequest(const util::MoveHolder<BatchGetRowRequest>&);
    BatchGetRowRequest& operator=(const util::MoveHolder<BatchGetRowRequest>&);
    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    const IVector<MultiPointQueryCriterion>& criteria() const
    {
        return mCriteria;
    }

    IVector<MultiPointQueryCriterion>* mutableCriteria()
    {
        return &mCriteria;
    }

private:
    DequeBasedVector<MultiPointQueryCriterion> mCriteria;
};

class BatchGetRowResponse : public Response
{
public:
    typedef PairWithUserData<util::Result<util::Optional<Row>, Error> > Result;

public:
    explicit BatchGetRowResponse() {}
    explicit BatchGetRowResponse(const util::MoveHolder<BatchGetRowResponse>& a)
    {
        *this = a;
    }

    BatchGetRowResponse& operator=(const util::MoveHolder<BatchGetRowResponse>&);
    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    const CapacityUnit& consumedCapacity() const
    {
        return mConsumedCapacity;
    }

    CapacityUnit* mutableConsumedCapacity()
    {
        return &mConsumedCapacity;
    }
    
    const IVector<Result>& results() const
    {
        return mResults;
    }

    IVector<Result>* mutableResults()
    {
        return &mResults;
    }

private:
    CapacityUnit mConsumedCapacity; 
    DequeBasedVector<Result> mResults;
};

class BatchWriteRowRequest
{
public:
    typedef PairWithUserData<RowPutChange> Put;
    typedef PairWithUserData<RowUpdateChange> Update;
    typedef PairWithUserData<RowDeleteChange> Delete;

public:
    explicit BatchWriteRowRequest() {}
    explicit BatchWriteRowRequest(
        const util::MoveHolder<BatchWriteRowRequest>&);
    BatchWriteRowRequest& operator=(
        const util::MoveHolder<BatchWriteRowRequest>&);
    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    const IVector<Put>& puts() const
    {
        return mPuts;
    }

    IVector<Put>* mutablePuts()
    {
        return &mPuts;
    }

    const IVector<Update>& updates() const
    {
        return mUpdates;
    }

    IVector<Update>* mutableUpdates()
    {
        return &mUpdates;
    }

    const IVector<Delete>& deletes() const
    {
        return mDeletes;
    }

    IVector<Delete>* mutableDeletes()
    {
        return &mDeletes;
    }

private:
    DequeBasedVector<Put> mPuts;
    DequeBasedVector<Update> mUpdates;
    DequeBasedVector<Delete> mDeletes;
};

class BatchWriteRowResponse: public Response
{
public:
    typedef PairWithUserData<util::Result<util::Optional<Row>, Error> > Result;

public:
    explicit BatchWriteRowResponse() {}
    explicit BatchWriteRowResponse(
        const util::MoveHolder<BatchWriteRowResponse>&);
    BatchWriteRowResponse& operator=(
        const util::MoveHolder<BatchWriteRowResponse>&);
    void prettyPrint(std::string*) const;
    util::Optional<Error> validate() const;
    void reset();

    const CapacityUnit& consumedCapacity() const
    {
        return mConsumedCapacity;
    }

    CapacityUnit* mutableConsumedCapacity()
    {
        return &mConsumedCapacity;
    }

    const IVector<Result>& putResults() const
    {
        return mPutResults;
    }

    IVector<Result>* mutablePutResults()
    {
        return &mPutResults;
    }

    const IVector<Result>& updateResults() const
    {
        return mUpdateResults;
    }

    IVector<Result>* mutableUpdateResults()
    {
        return &mUpdateResults;
    }

    const IVector<Result>& deleteResults() const
    {
        return mDeleteResults;
    }

    IVector<Result>* mutableDeleteResults()
    {
        return &mDeleteResults;
    }

private:
    CapacityUnit mConsumedCapacity; 
    DequeBasedVector<Result> mPutResults;
    DequeBasedVector<Result> mUpdateResults;
    DequeBasedVector<Result> mDeleteResults;
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
struct PrettyPrinterCategory<T, typename mp::EnableIf<std::tr1::is_same<T, aliyun::tablestore::core::RowChange::ReturnType>::value, void>::Type>
{
    typedef typename aliyun::tablestore::core::RowChange::ReturnType Category;
};

template<>
struct PrettyPrinter<aliyun::tablestore::core::RowChange::ReturnType, aliyun::tablestore::core::RowChange::ReturnType>
{
    void operator()(std::string*, aliyun::tablestore::core::RowChange::ReturnType) const;
};

template<class T>
struct PrettyPrinterCategory<T, typename mp::EnableIf<std::tr1::is_same<T, aliyun::tablestore::core::AttributeValue::Category>::value, void>::Type>
{
    typedef typename aliyun::tablestore::core::AttributeValue::Category Category;
};

template<>
struct PrettyPrinter<aliyun::tablestore::core::AttributeValue::Category, aliyun::tablestore::core::AttributeValue::Category>
{
    void operator()(std::string*, aliyun::tablestore::core::AttributeValue::Category) const;
};

template<class T>
struct PrettyPrinterCategory<T, typename mp::EnableIf<std::tr1::is_same<T, aliyun::tablestore::core::Condition::RowExistenceExpectation>::value, void>::Type>
{
    typedef typename aliyun::tablestore::core::Condition::RowExistenceExpectation Category;
};

template<>
struct PrettyPrinter<aliyun::tablestore::core::Condition::RowExistenceExpectation, aliyun::tablestore::core::Condition::RowExistenceExpectation>
{
    void operator()(std::string*, aliyun::tablestore::core::Condition::RowExistenceExpectation) const;
};

template<class T>
struct PrettyPrinterCategory<T, typename mp::EnableIf<std::tr1::is_same<T, aliyun::tablestore::core::SingleColumnCondition::Relation>::value, void>::Type>
{
    typedef typename aliyun::tablestore::core::SingleColumnCondition::Relation Category;
};

template<>
struct PrettyPrinter<aliyun::tablestore::core::SingleColumnCondition::Relation, aliyun::tablestore::core::SingleColumnCondition::Relation>
{
    void operator()(std::string*, aliyun::tablestore::core::SingleColumnCondition::Relation) const;
};

template<class T>
struct PrettyPrinterCategory<T, typename mp::EnableIf<std::tr1::is_same<T, aliyun::tablestore::core::CompositeColumnCondition::Operator>::value, void>::Type>
{
    typedef typename aliyun::tablestore::core::CompositeColumnCondition::Operator Category;
};

template<>
struct PrettyPrinter<aliyun::tablestore::core::CompositeColumnCondition::Operator, aliyun::tablestore::core::CompositeColumnCondition::Operator>
{
    void operator()(std::string*, aliyun::tablestore::core::CompositeColumnCondition::Operator) const;
};

template<class T>
struct PrettyPrinterCategory<T, typename mp::EnableIf<std::tr1::is_same<T, aliyun::tablestore::core::ColumnCondition::Type>::value, void>::Type>
{
    typedef typename aliyun::tablestore::core::ColumnCondition::Type Category;
};

template<>
struct PrettyPrinter<aliyun::tablestore::core::ColumnCondition::Type, aliyun::tablestore::core::ColumnCondition::Type>
{
    void operator()(std::string*, aliyun::tablestore::core::ColumnCondition::Type) const;
};

template<class T>
struct PrettyPrinterCategory<T, typename mp::EnableIf<std::tr1::is_same<T, aliyun::tablestore::core::RangeQueryCriterion::Direction>::value, void>::Type>
{
    typedef typename aliyun::tablestore::core::RangeQueryCriterion::Direction Category;
};

template<>
struct PrettyPrinter<aliyun::tablestore::core::RangeQueryCriterion::Direction, aliyun::tablestore::core::RangeQueryCriterion::Direction>
{
    void operator()(std::string*, aliyun::tablestore::core::RangeQueryCriterion::Direction) const;
};

template<class T>
struct PrettyPrinterCategory<T, typename mp::EnableIf<std::tr1::is_same<T, aliyun::tablestore::core::RowUpdateChange::Update::Type>::value, void>::Type>
{
    typedef typename aliyun::tablestore::core::RowUpdateChange::Update::Type Category;
};

template<>
struct PrettyPrinter<aliyun::tablestore::core::RowUpdateChange::Update::Type, aliyun::tablestore::core::RowUpdateChange::Update::Type>
{
    void operator()(std::string*, aliyun::tablestore::core::RowUpdateChange::Update::Type) const;
};

template<class T>
struct PrettyPrinterCategory<
    T,
    typename mp::EnableIf<
        std::tr1::is_same<
            T,
            aliyun::tablestore::util::Result<
                aliyun::tablestore::util::Optional<aliyun::tablestore::core::Row>,
                aliyun::tablestore::core::Error> >::value,
        void>::Type>
{
    typedef typename aliyun::tablestore::util::Result<
        aliyun::tablestore::util::Optional<aliyun::tablestore::core::Row>,
        aliyun::tablestore::core::Error> Category;
};

template<>
struct PrettyPrinter<
    aliyun::tablestore::util::Result<
        aliyun::tablestore::util::Optional<aliyun::tablestore::core::Row>,
        aliyun::tablestore::core::Error>,
    aliyun::tablestore::util::Result<
        aliyun::tablestore::util::Optional<aliyun::tablestore::core::Row>,
        aliyun::tablestore::core::Error> >
{
    void operator()(
        std::string*,
        const aliyun::tablestore::util::Result<
          aliyun::tablestore::util::Optional<aliyun::tablestore::core::Row>,
          aliyun::tablestore::core::Error>&) const;
};

} // namespace impl
} // namespace pp
