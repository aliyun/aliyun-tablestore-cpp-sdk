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
#include "core/types.hpp"
#include "core/retry.hpp"
#include "util/random.hpp"
#include "util/try.hpp"
#include "util/assert.hpp"
#include <cmath>
#include <stdint.h>

using namespace std;
using namespace aliyun::tablestore::util;
using namespace aliyun::tablestore::core;

namespace pp {
namespace impl {

void PrettyPrinter<Action, Action>::operator()(string* out, Action act) const
{
    switch(act) {
    case API_CREATE_TABLE:
        out->append("CreateTable");
        break;
    case API_LIST_TABLE:
        out->append("ListTable");
        break;
    case API_DESCRIBE_TABLE:
        out->append("DescribeTable");
        break;
    case API_DELETE_TABLE:
        out->append("DeleteTable");
        break;
    case API_UPDATE_TABLE:
        out->append("UpdateTable");
        break;
    case API_GET_ROW:
        out->append("GetRow");
        break;
    case API_PUT_ROW:
        out->append("PutRow");
        break;
    case API_UPDATE_ROW:
        out->append("UpdateRow");
        break;
    case API_DELETE_ROW:
        out->append("DeleteRow");
        break;
    case API_BATCH_GET_ROW:
        out->append("BatchGetRow");
        break;
    case API_BATCH_WRITE_ROW:
        out->append("BatchWriteRow");
        break;
    case API_GET_RANGE:
        out->append("GetRange");
        break;
    case API_COMPUTE_SPLIT_POINTS_BY_SIZE:
        out->append("ComputeSplitPointsBySize");
        break;
    }
}

void PrettyPrinter<PrimaryKeyType, PrimaryKeyType>::operator()(string* out, PrimaryKeyType pkt) const
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

void PrettyPrinter<PrimaryKeyOption, PrimaryKeyOption>::operator()(string* out, PrimaryKeyOption pko) const
{
    switch(pko) {
    case PKO_NONE:
        out->append("PKO_NONE");
        break;
    case PKO_AUTO_INCREMENT:
        out->append("PKO_AUTO_INCREMENT");
        break;
    }
}

void PrettyPrinter<BloomFilterType, BloomFilterType>::operator()(string* out, BloomFilterType bft) const
{
    switch(bft) {
    case BFT_NONE:
        out->append("BFT_NONE");
        break;
    case BFT_CELL:
        out->append("BFT_CELL");
        break;
    case BFT_ROW:
        out->append("BFT_ROW");
        break;
    }
}

void PrettyPrinter<PrimaryKeyValue::Category, PrimaryKeyValue::Category>::operator()(string* out, PrimaryKeyValue::Category cat) const
{
    switch(cat) {
    case PrimaryKeyValue::NONE:
        out->append("NONE");
        break;
    case PrimaryKeyValue::INF_MIN:
        out->append("-INF");
        break;
    case PrimaryKeyValue::INF_MAX:
        out->append("+INF");
        break;
    case PrimaryKeyValue::AUTO_INCR:
        out->append("AUTO_INCR");
        break;
    case PrimaryKeyValue::INTEGER:
        out->append("INTEGER");
        break;
    case PrimaryKeyValue::STRING:
        out->append("STRING");
        break;
    case PrimaryKeyValue::BINARY:
        out->append("BINARY");
        break;
    }
}

void PrettyPrinter<CompareResult, CompareResult>::operator()(string* out, CompareResult cr) const
{
    switch(cr) {
    case UNCOMPARABLE:
        out->append("UNCOMPARABLE");
        break;
    case EQUAL_TO:
        out->append("EQUAL_TO");
        break;
    case LESS_THAN:
        out->append("LESS_THAN");
        break;
    case GREATER_THAN:
        out->append("GREATER_THAN");
        break;
    }
}

void PrettyPrinter<TableStatus, TableStatus>::operator()(string* out, TableStatus ts) const
{
    switch(ts) {
    case ACTIVE:
        out->append("ACTIVE");
        break;
    case INACTIVE:
        out->append("INACTIVE");
        break;
    case LOADING:
        out->append("LOADING");
        break;
    case UNLOADING:
        out->append("UNLOADING");
        break;
    case UPDATING:
        out->append("UPDATING");
        break;
    }
}

void PrettyPrinter<ColumnType, ColumnType>::operator()(string* out, ColumnType ts) const
{
    switch(ts) {
    case CT_STRING:
        out->append("CT_STRING");
        break;
    case CT_INTEGER:
        out->append("CT_INTEGER");
        break;
    case CT_BINARY:
        out->append("CT_BINARY");
        break;
    case CT_BOOLEAN:
        out->append("CT_BOOLEAN");
        break;
    case CT_DOUBLE:
        out->append("CT_DOUBLE");
        break;
    }
}

void PrettyPrinter<ReturnType, ReturnType>::operator()(string* out, ReturnType ts) const
{
    switch(ts) {
    case RT_NONE:
        out->append("RT_NONE");
        break;
    case RT_PRIMARY_KEY:
        out->append("RT_PRIMARY_KEY");
        break;
    }
}

void PrettyPrinter<ColumnValue::Category, ColumnValue::Category>::operator()(string* out, ColumnValue::Category cat) const
{
    switch(cat) {
    case ColumnValue::NONE:
        out->append("NONE");
        break;
    case ColumnValue::STRING:
        out->append("STRING");
        break;
    case ColumnValue::INTEGER:
        out->append("INTEGER");
        break;
    case ColumnValue::BINARY:
        out->append("BINARY");
        break;
    case ColumnValue::BOOLEAN:
        out->append("BOOLEAN");
        break;
    case ColumnValue::FLOATING_POINT:
        out->append("FLOATING_POINT");
        break;
    }
}

} // namespace impl
} // namespace pp

namespace aliyun {
namespace tablestore {
namespace core {

Endpoint::Endpoint(const string& endpoint, const string& inst)
  : mEndpoint(endpoint),
    mInstanceName(inst)
{}

Endpoint::Endpoint(const MoveHolder<Endpoint>& a)
{
    *this = a;
}

Endpoint& Endpoint::operator=(const MoveHolder<Endpoint>& a)
{
    moveAssign(&mEndpoint, util::move(a->mEndpoint));
    moveAssign(&mInstanceName, util::move(a->mInstanceName));
    return *this;
}

void Endpoint::prettyPrint(string* out) const
{
    out->append("{\"Endpoint\":");
    pp::prettyPrint(out, mEndpoint);
    out->append(",\"InstanceName\":");
    pp::prettyPrint(out, mInstanceName);
    out->push_back('}');
}

Optional<Error> Endpoint::validate() const
{
    if (mEndpoint.empty()) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Endpoint must be nonempty."));
    }
    if (!MemPiece::from(mEndpoint).startsWith(MemPiece::from("http://"))
        && !MemPiece::from(mEndpoint).startsWith(MemPiece::from("https://"))) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Endpoint must starts with either \"http://\" or \"https://\"."));
    }
    if (mInstanceName.empty()) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Instance name must be nonempty."));
    }
    return Optional<Error>();
}

Credential::Credential(const MoveHolder<Credential>& a)
{
    *this = a;
}

Credential& Credential::operator=(const MoveHolder<Credential>& a)
{
    moveAssign(&mAccessKeyId, util::move(a->mAccessKeyId));
    moveAssign(&mAccessKeySecret, util::move(a->mAccessKeySecret));
    moveAssign(&mSecurityToken, util::move(a->mSecurityToken));
    return *this;
}

void Credential::prettyPrint(string* out) const
{
    out->append("{\"AccessKeyId\":");
    pp::prettyPrint(out, mAccessKeyId);
    out->append(",\"AccessKeySecret\":");
    pp::prettyPrint(out, mAccessKeySecret);
    if (!mSecurityToken.empty()) {
        out->append(",\"SecurityToken\":");
        pp::prettyPrint(out, mSecurityToken);
    }
    out->push_back('}');
}

Optional<Error> Credential::validate() const
{
    if (mAccessKeyId.empty()) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Access-key id must be nonempty."));
    }
    if (mAccessKeySecret.empty()) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Access-key secret must be nonempty."));
    }
    return Optional<Error>();
}


ClientOptions::ClientOptions()
  : mRandom(random::newDefault()),
    mMaxConnections(5000),
    mConnectTimeout(Duration::fromSec(2)),
    mRequestTimeout(Duration::fromSec(10)),
    mTraceThreshold(Duration::fromMsec(100)),
    mCheckResponseDigest(false),
    mRetryStrategy(new DefaultRetryStrategy(mRandom.get(), mRequestTimeout))
{}

ClientOptions::ClientOptions(const MoveHolder<ClientOptions>& a)
{
    *this = a;
}

ClientOptions& ClientOptions::operator=(const MoveHolder<ClientOptions>& a)
{
    moveAssign(&mRandom, util::move(a->mRandom));
    moveAssign(&mMaxConnections, util::move(a->mMaxConnections));
    moveAssign(&mConnectTimeout, util::move(a->mConnectTimeout));
    moveAssign(&mRequestTimeout, util::move(a->mRequestTimeout));
    moveAssign(&mTraceThreshold, util::move(a->mTraceThreshold));
    moveAssign(&mCheckResponseDigest, util::move(a->mCheckResponseDigest));
    moveAssign(&mRetryStrategy, util::move(a->mRetryStrategy));
    return *this;
}

void ClientOptions::prettyPrint(string* out) const
{
    out->append("{\"MaxConnections\":");
    pp::prettyPrint(out, maxConnections());
    out->append(",\"ConnectTimeout\":");
    pp::prettyPrint(out, connectTimeout());
    out->append(",\"RequestTimeout\":");
    pp::prettyPrint(out, requestTimeout());
    out->append(",\"TraceThreshold\":");
    pp::prettyPrint(out, traceThreshold());
    out->append(",\"CheckResponseDigest\":");
    pp::prettyPrint(out, checkResponesDigest());
    out->append(",\"RetryStrategy\":");
    pp::prettyPrint(out, string(typeid(retryStrategy().get()).name()));
    out->push_back('}');
}

Optional<Error> PrimaryKeyColumnSchema::validate() const
{
    if (name().empty()) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "\"name\" is required."));
    }
    if (option() == PKO_AUTO_INCREMENT && type() != PKT_INTEGER) {
        string msg("PKO_AUTO_INCREMENT can only be applied on PKT_INTEGER, for primary key \"");
        msg.append(name());
        msg.append("\".");
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                msg));
    }
    return Optional<Error>();
}

void PrimaryKeyColumnSchema::prettyPrint(string* out) const
{
    out->push_back('{');
    pp::prettyPrint(out, mName);
    out->push_back(':');
    pp::prettyPrint(out, mType);
    if (mOption != PKO_NONE) {
        out->push_back('+');
        pp::prettyPrint(out, mOption);
    }
    out->push_back('}');
}

void PrimaryKeySchema::prettyPrint(string* out) const
{
    pp::prettyPrint(out, mColumns);
}

Optional<Error> PrimaryKeySchema::validate() const
{
    if (size() == 0) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Table schema must be nonempty."));
    }
    for(int64_t i = 0, sz = size(); i < sz; ++i) {
        OTS_TRY((*this)[i].validate());
    }
    return Optional<Error>();
}

PrimaryKeyType PrimaryKeyValue::toPrimaryKeyType(Category cat)
{
    switch(cat) {
    case INTEGER: return PKT_INTEGER;
    case STRING: return PKT_STRING;
    case BINARY: return PKT_BINARY;
    case NONE: case INF_MIN: case INF_MAX: case AUTO_INCR:
        OTS_ASSERT(false)(cat);
    }
    return PKT_INTEGER;
}


PrimaryKeyValue::PrimaryKeyValue()
  : mCategory(NONE),
    mIntValue(0)
{}

PrimaryKeyValue::PrimaryKeyValue(const MoveHolder<PrimaryKeyValue>& a)
  : mCategory(a->mCategory),
    mIntValue(a->mIntValue)
{
    moveAssign(&mStrBlobValue, util::move(a->mStrBlobValue));
}

PrimaryKeyValue& PrimaryKeyValue::operator=(const MoveHolder<PrimaryKeyValue>& a)
{
    mCategory = a->mCategory;
    mIntValue = a->mIntValue;
    moveAssign(&mStrBlobValue, util::move(a->mStrBlobValue));
    return *this;
}

PrimaryKeyValue::Category PrimaryKeyValue::category() const
{
    return mCategory;
}

void PrimaryKeyValue::prettyPrint(string* out) const
{
    switch(category()) {
    case NONE:
        out->append("none");
        break;
    case INF_MIN:
        out->append("-inf");
        break;
    case INF_MAX:
        out->append("+inf");
        break;
    case AUTO_INCR:
        out->append("auto-incr");
        break;
    case INTEGER:
        pp::prettyPrint(out, integer());
        break;
    case STRING:
        pp::prettyPrint(out, str().toStr());
        break;
    case BINARY:
        pp::prettyPrint(out, blob());
        break;
    }
}

Optional<Error> PrimaryKeyValue::validate() const
{
    if (category() == NONE) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "value is not set."));
    }
    return Optional<Error>();
}

CompareResult PrimaryKeyValue::compare(const PrimaryKeyValue& b) const
{
    if (category() == NONE) {
        if (b.category() == NONE) {
            return EQUAL_TO;
        } else {
            return UNCOMPARABLE;
        }
    } else if (b.category() == NONE) {
        return UNCOMPARABLE;
    }
    if (category() == INF_MIN) {
        if (b.category() == INF_MIN) {
            return UNCOMPARABLE;
        } else {
            return LESS_THAN;
        }
    } else if (b.category() == INF_MIN) {
        return GREATER_THAN;
    }
    if (category() == INF_MAX) {
        if (b.category() == INF_MAX) {
            return UNCOMPARABLE;
        } else {
            return GREATER_THAN;
        }
    } else if (b.category() == INF_MAX) {
        return LESS_THAN;
    }
    if (category() == AUTO_INCR) {
        return UNCOMPARABLE;
    } else if (b.category() == AUTO_INCR) {
        return UNCOMPARABLE;
    }
    if (category() != b.category()) {
        return UNCOMPARABLE;
    }

    switch(category()) {
    case INTEGER:
        if (integer() < b.integer()) {
            return LESS_THAN;
        } else if (integer() > b.integer()) {
            return GREATER_THAN;
        } else {
            return EQUAL_TO;
        }
    case STRING: {
        int c = lexicographicOrder(str(), b.str());
        if (c < 0) {
            return LESS_THAN;
        } else if (c > 0) {
            return GREATER_THAN;
        } else {
            return EQUAL_TO;
        }
    }
    case BINARY: {
        int c = lexicographicOrder(blob(), b.blob());
        if (c < 0) {
            return LESS_THAN;
        } else if (c > 0) {
            return GREATER_THAN;
        } else {
            return EQUAL_TO;
        }
    }
    case NONE: case INF_MIN: case INF_MAX: case AUTO_INCR:
        OTS_ASSERT(false)(category());
    }
    return UNCOMPARABLE;
}

PrimaryKeyValue::PrimaryKeyValue(int64_t x)
  : mCategory(INTEGER),
    mIntValue(x)
{}

PrimaryKeyValue PrimaryKeyValue::toInteger(int64_t x)
{
    return PrimaryKeyValue(x);
}

int64_t PrimaryKeyValue::integer() const
{
    OTS_ASSERT(category() == INTEGER)(category());
    return mIntValue;
}

int64_t& PrimaryKeyValue::mutableInteger()
{
    PrimaryKeyValue empty;
    moveAssign(this, util::move(empty));
    mCategory = INTEGER;
    return mIntValue;
}

PrimaryKeyValue::PrimaryKeyValue(PrimaryKeyType tp, const MemPiece& s)
  : mCategory(NONE),
    mIntValue(0),
    mStrBlobValue(s)
{
    switch(tp) {
    case PKT_STRING:
        mCategory = STRING;
        break;
    case PKT_BINARY:
        mCategory = BINARY;
        break;
    default:
        OTS_ASSERT(false)(tp);
    }
}

PrimaryKeyValue PrimaryKeyValue::toStr(const MemPiece& s)
{
    return PrimaryKeyValue(PKT_STRING, s);
}

const MemPiece& PrimaryKeyValue::str() const
{
    OTS_ASSERT(category() == STRING)(category());
    return mStrBlobValue;
}

MemPiece& PrimaryKeyValue::mutableStr()
{
    PrimaryKeyValue empty;
    moveAssign(this, util::move(empty));
    mCategory = STRING;
    return mStrBlobValue;
}

PrimaryKeyValue PrimaryKeyValue::toBlob(const MemPiece& b)
{
    return PrimaryKeyValue(PKT_BINARY, b);
}

const MemPiece& PrimaryKeyValue::blob() const
{
    OTS_ASSERT(category() == BINARY)(category());
    return mStrBlobValue;
}

MemPiece& PrimaryKeyValue::mutableBlob()
{
    PrimaryKeyValue empty;
    moveAssign(this, util::move(empty));
    mCategory = BINARY;
    return mStrBlobValue;
}

PrimaryKeyValue::PrimaryKeyValue(InfMax)
  : mCategory(INF_MAX),
    mIntValue(0)
{}

PrimaryKeyValue PrimaryKeyValue::toInfMax()
{
    return PrimaryKeyValue(InfMax());
}

bool PrimaryKeyValue::isInfMax() const
{
    return category() == INF_MAX;
}

void PrimaryKeyValue::setInfMax()
{
    PrimaryKeyValue to = PrimaryKeyValue::toInfMax();
    moveAssign(this, util::move(to));
}

PrimaryKeyValue::PrimaryKeyValue(InfMin)
  : mCategory(INF_MIN),
    mIntValue(0)
{}

PrimaryKeyValue PrimaryKeyValue::toInfMin()
{
    return PrimaryKeyValue(InfMin());
}

bool PrimaryKeyValue::isInfMin() const
{
    return category() == INF_MIN;
}

void PrimaryKeyValue::setInfMin()
{
    PrimaryKeyValue to = PrimaryKeyValue::toInfMin();
    moveAssign(this, util::move(to));
}

PrimaryKeyValue::PrimaryKeyValue(AutoIncrement)
  : mCategory(AUTO_INCR),
    mIntValue(0)
{}

PrimaryKeyValue PrimaryKeyValue::toAutoIncrement()
{
    return PrimaryKeyValue(AutoIncrement());
}

bool PrimaryKeyValue::isAutoIncrement() const
{
    return category() == AUTO_INCR;
}

void PrimaryKeyValue::setAutoIncrement()
{
    PrimaryKeyValue to = PrimaryKeyValue::toAutoIncrement();
    moveAssign(this, util::move(to));
}

Optional<Error> PrimaryKeyColumn::validate() const
{
    if (mName.empty()) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "name of primary-key column is required."));
    }
    Optional<Error> err = value().validate();
    if (err.present()) {
        string msg("For primary-key column \"");
        msg.append(mName);
        msg.append("\", ");
        msg.append(err->message());
        err->mutableMessage() = msg;
        return err;
    }
    return Optional<Error>();
}

PrimaryKeyColumn::PrimaryKeyColumn()
{}

PrimaryKeyColumn::PrimaryKeyColumn(const string& name, const PrimaryKeyValue& v)
  : mName(name),
    mValue(v)
{}

PrimaryKeyColumn& PrimaryKeyColumn::operator=(const MoveHolder<PrimaryKeyColumn>& a)
{
    moveAssign(&mName, util::move(a->mName));
    moveAssign(&mValue, util::move(a->mValue));
    return *this;
}

void PrimaryKeyColumn::prettyPrint(string* out) const
{
    pp::prettyPrint(out, mName);
    out->push_back(':');
    pp::prettyPrint(out, mValue);
}

void PrimaryKey::prettyPrint(string* out) const
{
    if (mColumns.size() == 0) {
        out->append("{}");
        return;
    }
    out->push_back('{');
    pp::prettyPrint(out, mColumns[0]);
    for(int64_t i = 1, sz = mColumns.size(); i < sz; ++i) {
        out->push_back(',');
        pp::prettyPrint(out, mColumns[i]);
    }
    out->push_back('}');
}

Optional<Error> PrimaryKey::validate() const
{
    if (size() == 0) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Primary key must be nonempty."));
    }
    for(int64_t i = 0, sz = size(); i < sz; ++i) {
        OTS_TRY((*this)[i].validate());
    }
    return Optional<Error>();
}

CompareResult PrimaryKey::compare(const PrimaryKey& b) const
{
    int64_t asz = size();
    int64_t bsz = b.size();
    if (asz != bsz) {
        return UNCOMPARABLE;
    }
    for(int64_t i = 0; i < asz; ++i) {
        CompareResult c = (*this)[i].value().compare(b[i].value());
        if (c != EQUAL_TO) {
            return c;
        }
    }
    return EQUAL_TO;
}

void TableMeta::prettyPrint(string* out) const
{
    out->append("{\"TableName\":");
    pp::prettyPrint(out, mTableName);
    out->append(",\"Schema\":");
    pp::prettyPrint(out, mSchema);
    out->push_back('}');
}

Optional<Error> TableMeta::validate() const
{
    if (tableName().empty()) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Table name is required."));
    }
    OTS_TRY(schema().validate());
    return Optional<Error>();
}

TableOptions& TableOptions::operator=(const MoveHolder<TableOptions>& a)
{
    util::moveAssign(&mReservedThroughput, util::move(a->mReservedThroughput));
    util::moveAssign(&mTimeToLive, util::move(a->mTimeToLive));
    util::moveAssign(&mMaxVersions, util::move(a->mMaxVersions));
    util::moveAssign(&mBloomFilterType, util::move(a->mBloomFilterType));
    util::moveAssign(&mBlockSize, util::move(a->mBlockSize));
    util::moveAssign(&mMaxTimeDeviation, util::move(a->mMaxTimeDeviation));
    return *this;
}

void TableOptions::clear()
{
    mReservedThroughput = CapacityUnit(0, 0);
    mTimeToLive.clear();
    mMaxVersions.clear();
    mBloomFilterType.clear();
    mBlockSize.clear();
    mMaxTimeDeviation.clear();
}

void TableOptions::prettyPrint(string* out) const
{
    out->push_back('{');
    {
        out->append("\"ReservedThroughput\":");
        pp::prettyPrint(out, mReservedThroughput);
    }
    if (mTimeToLive.present()) {
        out->push_back(',');
        out->append("\"TimeToLive\":");
        pp::prettyPrint(out, mTimeToLive->toSec());
    }
    if (mMaxVersions.present()) {
        out->push_back(',');
        out->append("\"MaxVersions\":");
        pp::prettyPrint(out, *mMaxVersions);
    }
    if (mBloomFilterType.present()) {
        out->push_back(',');
        out->append("\"BloomFilterType\":");
        pp::prettyPrint(out, *mBloomFilterType);
    }
    if (mBlockSize.present()) {
        out->push_back(',');
        out->append("\"BlockSize\":");
        pp::prettyPrint(out, *mBlockSize);
    }
    if (mMaxTimeDeviation.present()) {
        out->push_back(',');
        out->append("\"MaxTimeDeviation\":");
        pp::prettyPrint(out, mMaxTimeDeviation->toSec());
    }
    out->push_back('}');
}

Optional<Error> TableOptions::validate() const
{
    OTS_TRY(reservedThroughput().validate());
    if (!reservedThroughput().read().present()) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Read reserved throughput must be set."));
    }
    if (!reservedThroughput().write().present()) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Write reserved throughput must be set."));
    }
    if (mTimeToLive.present()) {
        if (mTimeToLive->toUsec() % kUsecPerSec != 0) {
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    "TimeToLive must be integral multiple of seconds."));
        }
        if (mTimeToLive->toUsec() <= 0) {
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    "TimeToLive must be positive."));
        }
    }
    if (mMaxVersions.present()) {
        if (*mMaxVersions <= 0) {
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    "MaxVersions must be positive."));
        }
    }
    if (mBlockSize.present()) {
        if (*mBlockSize <= 0) {
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    "BlockSize must be positive."));
        }
    }
    if (mMaxTimeDeviation.present()) {
        if (mMaxTimeDeviation->toUsec() % kUsecPerSec != 0) {
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    "MaxTimeDeviation must be integral multiple of seconds."));
        }
        if (mMaxTimeDeviation->toUsec() <= 0) {
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    "MaxTimeDeviation must be positive."));
        }
    }
    return Optional<Error>();
}

Optional<Error> CapacityUnit::validate() const
{
    if (mRead.present()) {
        if (*mRead < 0) {
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    "read capacity unit must be positive."));
        }
    }
    if (mWrite.present()) {
        if (*mWrite < 0) {
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    "write capacity unit must be positive."));
        }
    }
    return Optional<Error>();
}

void CapacityUnit::prettyPrint(string* out) const
{
    out->push_back('{');
    bool first = true;
    if (mRead.present()) {
        first = false;
        out->append("\"Read\":");
        pp::prettyPrint(out, *mRead);
    }
    if (mWrite.present()) {
        if (first) {
            first = false;
        } else {
            out->push_back(',');
        }
        out->append("\"Write\":");
        pp::prettyPrint(out, *mWrite);
    }
    out->push_back('}');
}


ColumnValue::ColumnValue()
  : mCategory(NONE),
    mIntValue(0),
    mBoolValue(false),
    mFloatingValue(0)
{}

ColumnValue& ColumnValue::operator=(const MoveHolder<ColumnValue>& a)
{
    mCategory = a->category();
    mIntValue = a->mIntValue;
    mBoolValue = a->mBoolValue;
    mFloatingValue = a->mFloatingValue;
    util::moveAssign(&mStrBlobValue, util::move(a->mStrBlobValue));
    return *this;
}

ColumnType ColumnValue::toColumnType(Category cat)
{
    switch(cat) {
    case ColumnValue::STRING: return CT_STRING;
    case ColumnValue::INTEGER: return CT_INTEGER;
    case ColumnValue::BINARY: return CT_BINARY;
    case ColumnValue::BOOLEAN: return CT_BOOLEAN;
    case ColumnValue::FLOATING_POINT: return CT_DOUBLE;
    case ColumnValue::NONE:
        break;
    }
    OTS_ASSERT(false)((int) cat);
    return CT_STRING; // whatever
}

void ColumnValue::prettyPrint(string* out) const
{
    switch(category()) {
    case NONE:
        out->append("none");
        break;
    case STRING:
        pp::prettyPrint(out, str().toStr());
        break;
    case BINARY:
        pp::prettyPrint(out, blob());
        break;
    case INTEGER:
        pp::prettyPrint(out, integer());
        break;
    case BOOLEAN:
        if (boolean()) {
            out->append("true");
        } else {
            out->append("false");
        }
        break;
    case FLOATING_POINT:
        pp::prettyPrint(out, floatPoint());
        break;
    }
}

Optional<Error> ColumnValue::validate() const
{
    if (category() == NONE) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "value is not set."));
    }
    if (category() == FLOATING_POINT) {
        double v = floatPoint();
        if (isinf(v)) {
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    "value cannot be set to infinity."));
        }
        if (isnan(v)) {
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    "value cannot be set to NaN."));
        }
    }
    return Optional<Error>();
}

ColumnValue::ColumnValue(Category cat, const MemPiece& strblob)
  : mCategory(cat),
    mIntValue(0),
    mStrBlobValue(strblob),
    mBoolValue(false),
    mFloatingValue(0)
{}

ColumnValue ColumnValue::toStr(const MemPiece& a)
{
    return ColumnValue(STRING, a);
}

const MemPiece& ColumnValue::str() const
{
    OTS_ASSERT(category() == STRING)(category());
    return mStrBlobValue;
}

MemPiece& ColumnValue::mutableStr()
{
    ColumnValue empty;
    moveAssign(this, util::move(empty));
    mCategory = STRING;
    return mStrBlobValue;
}

ColumnValue ColumnValue::toBlob(const MemPiece& a)
{
    return ColumnValue(BINARY, a);
}

const MemPiece& ColumnValue::blob() const
{
    OTS_ASSERT(category() == BINARY)(category());
    return mStrBlobValue;
}

MemPiece& ColumnValue::mutableBlob()
{
    ColumnValue empty;
    moveAssign(this, util::move(empty));
    mCategory = BINARY;
    return mStrBlobValue;
}

ColumnValue::ColumnValue(int64_t v)
  : mCategory(INTEGER),
    mIntValue(v),
    mBoolValue(false),
    mFloatingValue(0)
{}

ColumnValue ColumnValue::toInteger(int64_t v)
{
    return ColumnValue(v);
}

int64_t ColumnValue::integer() const
{
    OTS_ASSERT(category() == INTEGER)(category());
    return mIntValue;
}

int64_t& ColumnValue::mutableInteger()
{
    ColumnValue empty;
    moveAssign(this, util::move(empty));
    mCategory = INTEGER;
    return mIntValue;
}

ColumnValue::ColumnValue(double v)
  : mCategory(FLOATING_POINT),
    mIntValue(0),
    mBoolValue(false),
    mFloatingValue(v)
{}

ColumnValue ColumnValue::toFloatPoint(double v)
{
    return ColumnValue(v);
}

double ColumnValue::floatPoint() const
{
    OTS_ASSERT(category() == FLOATING_POINT)(category());
    return mFloatingValue;
}

double& ColumnValue::mutableFloatPoint()
{
    ColumnValue empty;
    moveAssign(this, util::move(empty));
    mCategory = FLOATING_POINT;
    return mFloatingValue;
}

ColumnValue::ColumnValue(bool v)
  : mCategory(BOOLEAN),
    mIntValue(0),
    mBoolValue(v),
    mFloatingValue(0)
{}

ColumnValue ColumnValue::toBoolean(bool v)
{
    return ColumnValue(v);
}

bool ColumnValue::boolean() const
{
    OTS_ASSERT(category() == BOOLEAN)(category());
    return mBoolValue;
}

bool& ColumnValue::mutableBoolean()
{
    ColumnValue empty;
    moveAssign(this, util::move(empty));
    mCategory = BOOLEAN;
    return mBoolValue;
}


Column::Column(const string& name, const ColumnValue& val)
  : mName(name),
    mValue(val)
{}

Column::Column(const string& name, const ColumnValue& val, int64_t ts)
  : mName(name),
    mValue(val),
    mTimestamp(ts)
{}

void Column::prettyPrint(string* out) const
{
    out->append("{\"Name\":");
    pp::prettyPrint(out, mName);
    out->append(",\"Value\":");
    pp::prettyPrint(out, mValue);
    if (mTimestamp.present()) {
        out->append(",\"Timestamp\":");
        pp::prettyPrint(out, *mTimestamp);
    }
    out->push_back('}');
}

Optional<Error> Column::validate() const
{
    if (name().empty()) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Column name must be nonempty."));
    }
    {
        Optional<Error> err = value().validate();
        if (err.present()) {
            string msg("For column ");
            pp::prettyPrint(&msg, name());
            msg.append(", ");
            msg.append(err->message());
            err->mutableMessage() = msg;
            return err;
        }
    }
    if (timestamp().present()) {
        if (*timestamp() < 0) {
            string msg("Timestamp of column ");
            pp::prettyPrint(&msg, name());
            msg.append(" must be positive.");
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    msg));
        }
    }
    return Optional<Error>();
}


Row& Row::operator=(const MoveHolder<Row>& a)
{
    moveAssign(&mPkey, util::move(a->mPkey));
    moveAssign(&mAttrs, util::move(a->mAttrs));
    return *this;
}

void Row::prettyPrint(string* out) const
{
    out->append("{\"PrimaryKey\":");
    pp::prettyPrint(out, mPkey);
    out->append(",\"Attributes\":");
    pp::prettyPrint(out, mAttrs);
    out->push_back('}');
}

Optional<Error> Row::validate() const
{
    OTS_TRY(mPkey.validate());
    for(int64_t i = 0, sz = mAttrs.size(); i < sz; ++i) {
        OTS_TRY(mAttrs[i].validate());
    }
    return Optional<Error>();
}


TimeRange& TimeRange::operator=(const MoveHolder<TimeRange>& a)
{
    moveAssign(&mStart, util::move(a->mStart));
    moveAssign(&mEnd, util::move(a->mEnd));
    return *this;
}

void TimeRange::prettyPrint(string* out) const
{
    out->push_back('[');
    pp::prettyPrint(out, mStart);
    out->push_back(',');
    pp::prettyPrint(out, mEnd);
    out->push_back(')');
}

Optional<Error> TimeRange::validate() const
{
    if (mStart.toUsec() % kUsecPerMsec != 0) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Start of time ranges must be integral multiple of milliseconds."));
    }
    if (mEnd.toUsec() % kUsecPerMsec != 0) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "End of time ranges must be integral multiple of milliseconds."));
    }
    if (mStart > mEnd) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Start of time ranges must be in advance of their ends."));
    }
    return Optional<Error>();
}

Split& Split::operator=(const MoveHolder<Split>& a)
{
    moveAssign(&mLowerBound, util::move(a->mLowerBound));
    moveAssign(&mUpperBound, util::move(a->mUpperBound));
    moveAssign(&mLocation, util::move(a->mLocation));
    return *this;
}

void Split::prettyPrint(string* out) const
{
    out->append("{\"Location\":");
    pp::prettyPrint(out, mLocation);
    if (mLowerBound.get() != NULL) {
        out->append(",\"LowerBound\":");
        pp::prettyPrint(out, *mLowerBound);
    }
    if (mUpperBound.get() != NULL) {
        out->append(",\"UpperBound\":");
        pp::prettyPrint(out, *mUpperBound);
    }
    out->push_back('}');
}

Optional<Error> Split::validate() const
{
    if (mLowerBound.get() == NULL) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Lower bound of a split must be nonnull."));
    }
    if (mUpperBound.get() == NULL) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Upper bound of a split must be nonnull."));
    }
    OTS_TRY(mLowerBound->validate());
    OTS_TRY(mUpperBound->validate());
    if (mLowerBound->size() != mUpperBound->size()) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Lower bound of a split must be of the same length of the upper bound of that split."));
    }
    for(int64_t i = 0, sz = mLowerBound->size(); i < sz; ++i) {
        const PrimaryKeyColumn& lower = (*mLowerBound)[i];
        const PrimaryKeyColumn& upper = (*mUpperBound)[i];
        if (lower.name() != upper.name()) {
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    "Lower bound of a split must have the same names of the upper bound of that split."));
        }
        if (lower.value().category() != upper.value().category()) {
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    "Lower bound of a split must have the same types of the upper bound of that split."));
        }
    }
    {
        CompareResult c = mLowerBound->compare(*mUpperBound);
        if (c == GREATER_THAN || c == EQUAL_TO) {
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    "Lower bound of a split must be smaller than the upper bound of that split."));
        } else if (c == UNCOMPARABLE) {
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    "Lower bound of a split must be comparable with the upper bound."));
        }
    }
    return Optional<Error>();
}


Response& Response::operator=(const MoveHolder<Response>& a)
{
    moveAssign(&mRequestId, util::move(a->mRequestId));
    moveAssign(&mTraceId, util::move(a->mTraceId));
    return *this;
}

CreateTableRequest& CreateTableRequest::operator=(const MoveHolder<CreateTableRequest>& a)
{
    moveAssign(&mMeta, util::move(a->mMeta));
    moveAssign(&mOptions, util::move(a->mOptions));
    moveAssign(&mPartitionSplitPoints, util::move(a->mPartitionSplitPoints));
    return *this;
}

void CreateTableRequest::prettyPrint(string* out) const
{
    out->append("{\"Meta\":");
    pp::prettyPrint(out, mMeta);
    out->append(",\"Options\":");
    pp::prettyPrint(out, mOptions);
    out->append(",\"PartitionSplitPoints\":");
    pp::prettyPrint(out, mPartitionSplitPoints);
    out->push_back('}');
}

Optional<Error> CreateTableRequest::validate() const
{
    OTS_TRY(mMeta.validate());
    OTS_TRY(mOptions.validate());
    for(int64_t i = 0, sz = mPartitionSplitPoints.size(); i < sz; ++i) {
        const PrimaryKey& pk = mPartitionSplitPoints[i];
        OTS_TRY(pk.validate());
    }
    for(int64_t i = 0, sz = mPartitionSplitPoints.size(); i < sz; ++i) {
        const PrimaryKey& pk = mPartitionSplitPoints[i];
        if (pk.size() != 1) {
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    "Length of partition split points must be exactly one."));
        }
    }
    for(int64_t i = 0, sz = mPartitionSplitPoints.size(); i < sz; ++i) {
        const PrimaryKey& pk = mPartitionSplitPoints[i];
        OTS_ASSERT(pk.size() >= 1)(pk.size());
        const PrimaryKeyColumn& pkc = pk[0];
        if (!pkc.value().isReal()) {
            string msg = "Partition split points contains an unreal value type ";
            pp::prettyPrint(&msg, pkc.value().category());
            msg.push_back('.');
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    msg));
        }
        const PrimaryKeySchema& schema = mMeta.schema();
        OTS_ASSERT(schema.size() >= 1)
            (schema.size());
        const PrimaryKeyColumnSchema& colSchema = schema[0];
        if (MemPiece::from(pkc.name()) != MemPiece::from(colSchema.name())) {
            string msg = "Partition split points contains ";
            pp::prettyPrint(&msg, pkc.name());
            msg.append(", which is different with that in the schema.");
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    msg));
        }
        if (PrimaryKeyValue::toPrimaryKeyType(pkc.value().category()) != colSchema.type()) {
            string msg = "Type of primary-key column ";
            pp::prettyPrint(&msg, pkc.name());
            msg.append(" mismatches that in schema.");
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    msg));
        }
    }
    return Optional<Error>();
}

CreateTableResponse& CreateTableResponse::operator=(
    const MoveHolder<CreateTableResponse>& a)
{
    Response::operator=(util::move(static_cast<Response&>(*a)));
    return *this;
}

} // namespace core
} // namespace tablestore
} // namespace aliyun

