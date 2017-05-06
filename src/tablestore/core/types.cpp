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
#include "tablestore/core/types.hpp"
#include "tablestore/core/retry.hpp"
#include "tablestore/util/random.hpp"
#include "tablestore/util/try.hpp"
#include "tablestore/util/assert.hpp"
#include <cmath>
#include <stdint.h>
#include <cstdio>

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

void PrettyPrinter<PrimaryKeyType, PrimaryKeyType>::operator()(
    string* out, PrimaryKeyType pkt) const
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

void PrettyPrinter<PrimaryKeyOption, PrimaryKeyOption>::operator()(
    string* out, PrimaryKeyOption pko) const
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

void PrettyPrinter<BloomFilterType, BloomFilterType>::operator()(
    string* out, BloomFilterType bft) const
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

void PrettyPrinter<
    PrimaryKeyValue::Category, PrimaryKeyValue::Category>::operator()
    (string* out, PrimaryKeyValue::Category cat) const
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

void PrettyPrinter<CompareResult, CompareResult>::operator()(
    string* out, CompareResult cr) const
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

void PrettyPrinter<TableStatus, TableStatus>::operator()(
    string* out, TableStatus ts) const
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

void PrettyPrinter<RowChange::ReturnType, RowChange::ReturnType>::operator()(
    string* out, RowChange::ReturnType ts) const
{
    switch(ts) {
    case RowChange::RT_NONE:
        out->append("RT_NONE");
        break;
    case RowChange::RT_PRIMARY_KEY:
        out->append("RT_PRIMARY_KEY");
        break;
    }
}

void PrettyPrinter<
    AttributeValue::Category, AttributeValue::Category>::operator()
    (string* out, AttributeValue::Category cat) const
{
    switch(cat) {
    case AttributeValue::NONE:
        out->append("NONE");
        break;
    case AttributeValue::STRING:
        out->append("STRING");
        break;
    case AttributeValue::INTEGER:
        out->append("INTEGER");
        break;
    case AttributeValue::BINARY:
        out->append("BINARY");
        break;
    case AttributeValue::BOOLEAN:
        out->append("BOOLEAN");
        break;
    case AttributeValue::FLOATING_POINT:
        out->append("FLOATING_POINT");
        break;
    }
}

void PrettyPrinter<
    Condition::RowExistenceExpectation,
    Condition::RowExistenceExpectation>::operator()
    (string* out, Condition::RowExistenceExpectation exp) const
{
    switch(exp) {
    case Condition::IGNORE:
        out->append("IGNORE");
        break;
    case Condition::EXPECT_EXIST:
        out->append("EXPECT_EXIST");
        break;
    case Condition::EXPECT_NOT_EXIST:
        out->append("EXPECT_NOT_EXIST");
        break;
    }
}

void PrettyPrinter<
    SingleColumnCondition::Relation,
    SingleColumnCondition::Relation>::operator()
    (string* out, SingleColumnCondition::Relation exp) const
{
    switch(exp) {
    case SingleColumnCondition::EQUAL: 
        out->append("EQUAL");
        break;
    case SingleColumnCondition::NOT_EQUAL:
        out->append("NOT_EQUAL");
        break;
    case SingleColumnCondition::GREATER_THAN:
        out->append("GREATER_THAN");
        break;
    case SingleColumnCondition::GREATER_EQUAL:
        out->append("GREATER_EQUAL");
        break;
    case SingleColumnCondition::LESS_THAN: 
        out->append("LESS_THAN");
        break;
    case SingleColumnCondition::LESS_EQUAL:
        out->append("LESS_EQUAL");
        break;
    }
}

void PrettyPrinter<
    CompositeColumnCondition::Operator,
    CompositeColumnCondition::Operator>::operator()
    (string* out, CompositeColumnCondition::Operator op) const
{
    switch(op) {
    case CompositeColumnCondition::NOT: 
        out->append("NOT");
        break;
    case CompositeColumnCondition::AND:
        out->append("AND");
        break;
    case CompositeColumnCondition::OR:
        out->append("OR");
        break;
    }
}

void PrettyPrinter<ColumnCondition::Type, ColumnCondition::Type>::operator()
    (string* out, ColumnCondition::Type cct) const
{
    switch(cct) {
    case ColumnCondition::SINGLE: 
        out->append("SINGLE");
        break;
    case ColumnCondition::COMPOSITE:
        out->append("COMPOSITE");
        break;
    }
}

void PrettyPrinter<RangeQueryCriterion::Direction, RangeQueryCriterion::Direction>::operator()
    (string* out, RangeQueryCriterion::Direction dir) const
{
    switch(dir) {
    case RangeQueryCriterion::FORWARD: 
        out->append("FORWARD");
        break;
    case RangeQueryCriterion::BACKWARD:
        out->append("BACKWARD");
        break;
    }
}

void PrettyPrinter<RowUpdateChange::Update::Type, RowUpdateChange::Update::Type>::operator()
    (string* out, RowUpdateChange::Update::Type type) const
{
    switch(type) {
    case RowUpdateChange::Update::PUT: 
        out->append("PUT");
        break;
    case RowUpdateChange::Update::DELETE:
        out->append("DELETE");
        break;
    case RowUpdateChange::Update::DELETE_ALL:
        out->append("DELETE_ALL");
        break;
    }
}

void PrettyPrinter<
    Result<Optional<Row>, Error>, Result<Optional<Row>, Error> >::operator()(
        string* out, const Result<Optional<Row>, Error>& res) const
{
    if (res.ok()) {
        out->append("{\"Ok\":");
        if (!res.okValue().present()) {
            out->append("null}");
        } else {
            pp::prettyPrint(out, *res.okValue());
            out->push_back('}');
        }
    } else {
        out->append("{\"Error\":");
        pp::prettyPrint(out, res.errValue());
        out->push_back('}');
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

void Endpoint::reset()
{
    mEndpoint.clear();
    mInstanceName.clear();
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

void Credential::reset()
{
    mAccessKeyId.clear();
    mAccessKeySecret.clear();
    mSecurityToken.clear();
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

namespace {

bool ContainsCrlf(const string& s)
{
    return s.find('\n') != string::npos || s.find('\r') != string::npos;
}

} // namespace

Optional<Error> Credential::validate() const
{
    if (mAccessKeyId.empty()) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Access-key id must be nonempty."));
    }
    if (ContainsCrlf(mAccessKeyId)) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Access-key id should not contain CRLF."));
    }
    if (mAccessKeySecret.empty()) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Access-key secret must be nonempty."));
    }
    if (ContainsCrlf(mAccessKeySecret)) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Access-key secret should not contain CRLF."));
    }
    if (ContainsCrlf(mSecurityToken)) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Security token should not contian CRLF."));
    }
    return Optional<Error>();
}


ClientOptions::ClientOptions()
{
    reset();
}

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

void ClientOptions::reset()
{
    mMaxConnections = 5000;
    mConnectTimeout = Duration::fromSec(2);
    mRequestTimeout = Duration::fromMsec(300);
    mTraceThreshold = Duration::fromMsec(100);
    mCheckResponseDigest = false;
    mRetryStrategy.reset(new DefaultRetryStrategy(mRandom.get(), Duration::fromSec(10)));
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
    pp::prettyPrint(out, checkResponseDigest());
    out->append(",\"RetryStrategy\":");
    pp::prettyPrint(out, string(typeid(retryStrategy().get()).name()));
    out->push_back('}');
}

Optional<Error> ClientOptions::validate() const
{
    if (maxConnections() <= 0) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "MaxConnections must be positive."));
    }
    if (connectTimeout() < Duration::fromMsec(1)) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "ConnectTimeout must be greater than 1 msec. Recommends 2 secs."));
    }
    if (requestTimeout() < Duration::fromMsec(1)) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "RequestTimeout must be greater than 1 msec. Recommends 10 secs."));
    }
    if (traceThreshold() < Duration::fromMsec(1)) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "TraceThreshold must be greater than 1 msec. Recommends 100 msecs."));
    }
    if (retryStrategy().get() == NULL) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "RetryStrategy is required."));
    }
    return Optional<Error>();
}


void PrimaryKeyColumnSchema::reset()
{
    mName.clear();
    mType = PKT_INTEGER;
    mOption = PKO_NONE;
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

void Schema::prettyPrint(string* out) const
{
    pp::prettyPrint(out, mColumns);
}

Optional<Error> Schema::validate() const
{
    if (size() == 0) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Table schema must be nonempty."));
    }
    for(int64_t i = 0, sz = size(); i < sz; ++i) {
        TRY((*this)[i].validate());
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
                "value is required."));
    }
    return Optional<Error>();
}

void PrimaryKeyValue::reset()
{
    mCategory = NONE;
    mIntValue = 0;
    mStrBlobValue = MemPiece();
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

int64_t* PrimaryKeyValue::mutableInteger()
{
    PrimaryKeyValue empty;
    moveAssign(this, util::move(empty));
    mCategory = INTEGER;
    return &mIntValue;
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

MemPiece* PrimaryKeyValue::mutableStr()
{
    PrimaryKeyValue empty;
    moveAssign(this, util::move(empty));
    mCategory = STRING;
    return &mStrBlobValue;
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

MemPiece* PrimaryKeyValue::mutableBlob()
{
    PrimaryKeyValue empty;
    moveAssign(this, util::move(empty));
    mCategory = BINARY;
    return &mStrBlobValue;
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
        *err->mutableMessage() = msg;
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

void PrimaryKeyColumn::reset()
{
    mName.clear();
    mValue.reset();
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
                "Primary key is required."));
    }
    for(int64_t i = 0, sz = size(); i < sz; ++i) {
        TRY((*this)[i].validate());
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


void TableMeta::reset()
{
    mTableName.clear();
    mSchema.reset();
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
    TRY(schema().validate());
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

void TableOptions::reset()
{
    mReservedThroughput.reset();
    mTimeToLive.reset();
    mMaxVersions.reset();
    mBloomFilterType.reset();
    mBlockSize.reset();
    mMaxTimeDeviation.reset();
}

void TableOptions::prettyPrint(string* out) const
{
    out->push_back('{');
    bool first = true;
    if (mReservedThroughput.present()) {
        first = false;
        out->append("\"ReservedThroughput\":");
        pp::prettyPrint(out, *mReservedThroughput);
    }
    if (mTimeToLive.present()) {
        if (first) {
            first = false;
        } else {
            out->push_back(',');
        }
        out->append("\"TimeToLive\":");
        pp::prettyPrint(out, mTimeToLive->toSec());
    }
    if (mMaxVersions.present()) {
        if (first) {
            first = false;
        } else {
            out->push_back(',');
        }
        out->append("\"MaxVersions\":");
        pp::prettyPrint(out, *mMaxVersions);
    }
    if (mBloomFilterType.present()) {
        if (first) {
            first = false;
        } else {
            out->push_back(',');
        }
        out->append("\"BloomFilterType\":");
        pp::prettyPrint(out, *mBloomFilterType);
    }
    if (mBlockSize.present()) {
        if (first) {
            first = false;
        } else {
            out->push_back(',');
        }
        out->append("\"BlockSize\":");
        pp::prettyPrint(out, *mBlockSize);
    }
    if (mMaxTimeDeviation.present()) {
        if (first) {
            first = false;
        } else {
            out->push_back(',');
        }
        out->append("\"MaxTimeDeviation\":");
        pp::prettyPrint(out, mMaxTimeDeviation->toSec());
    }
    out->push_back('}');
}

Optional<Error> TableOptions::validate() const
{
    if (reservedThroughput().present()) {
        TRY(reservedThroughput()->validate());
        if (!reservedThroughput()->read().present()) {
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    "Read reserved throughput is required."));
        }
        if (!reservedThroughput()->write().present()) {
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    "Write reserved throughput is required."));
        }
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


void CapacityUnit::reset()
{
    mRead.reset();
    mWrite.reset();
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


AttributeValue::AttributeValue()
  : mCategory(NONE),
    mIntValue(0),
    mBoolValue(false),
    mFloatingValue(0)
{}

AttributeValue& AttributeValue::operator=(const MoveHolder<AttributeValue>& a)
{
    mCategory = a->category();
    mIntValue = a->mIntValue;
    mBoolValue = a->mBoolValue;
    mFloatingValue = a->mFloatingValue;
    util::moveAssign(&mStrBlobValue, util::move(a->mStrBlobValue));
    return *this;
}

void AttributeValue::reset()
{
    mCategory = NONE;
    mIntValue = 0;
    mStrBlobValue = MemPiece();
    mBoolValue = false;
    mFloatingValue = 0;
}

void AttributeValue::prettyPrint(string* out) const
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

Optional<Error> AttributeValue::validate() const
{
    if (category() == NONE) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "value is required."));
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

CompareResult AttributeValue::compare(const AttributeValue& b) const
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
    case BOOLEAN: {
        if (boolean() == b.boolean()) {
            return EQUAL_TO;
        } else if (boolean()) {
            return GREATER_THAN;
        } else {
            return LESS_THAN;
        }
    }
    case FLOATING_POINT: {
        if (floatPoint() == b.floatPoint()) {
            return EQUAL_TO;
        } else if (floatPoint() < b.floatPoint()) {
            return LESS_THAN;
        } else {
            return GREATER_THAN;
        }
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
    case NONE:
        OTS_ASSERT(false)(category());
    }
    return UNCOMPARABLE;
}

AttributeValue::AttributeValue(Category cat, const MemPiece& strblob)
  : mCategory(cat),
    mIntValue(0),
    mStrBlobValue(strblob),
    mBoolValue(false),
    mFloatingValue(0)
{}

AttributeValue AttributeValue::toStr(const MemPiece& a)
{
    return AttributeValue(STRING, a);
}

const MemPiece& AttributeValue::str() const
{
    OTS_ASSERT(category() == STRING)(category());
    return mStrBlobValue;
}

MemPiece* AttributeValue::mutableStr()
{
    AttributeValue empty;
    moveAssign(this, util::move(empty));
    mCategory = STRING;
    return &mStrBlobValue;
}

AttributeValue AttributeValue::toBlob(const MemPiece& a)
{
    return AttributeValue(BINARY, a);
}

const MemPiece& AttributeValue::blob() const
{
    OTS_ASSERT(category() == BINARY)(category());
    return mStrBlobValue;
}

MemPiece* AttributeValue::mutableBlob()
{
    AttributeValue empty;
    moveAssign(this, util::move(empty));
    mCategory = BINARY;
    return &mStrBlobValue;
}

AttributeValue::AttributeValue(int64_t v)
  : mCategory(INTEGER),
    mIntValue(v),
    mBoolValue(false),
    mFloatingValue(0)
{}

AttributeValue AttributeValue::toInteger(int64_t v)
{
    return AttributeValue(v);
}

int64_t AttributeValue::integer() const
{
    OTS_ASSERT(category() == INTEGER)(category());
    return mIntValue;
}

int64_t* AttributeValue::mutableInteger()
{
    AttributeValue empty;
    moveAssign(this, util::move(empty));
    mCategory = INTEGER;
    return &mIntValue;
}

AttributeValue::AttributeValue(double v)
  : mCategory(FLOATING_POINT),
    mIntValue(0),
    mBoolValue(false),
    mFloatingValue(v)
{}

AttributeValue AttributeValue::toFloatPoint(double v)
{
    return AttributeValue(v);
}

double AttributeValue::floatPoint() const
{
    OTS_ASSERT(category() == FLOATING_POINT)(category());
    return mFloatingValue;
}

double* AttributeValue::mutableFloatPoint()
{
    AttributeValue empty;
    moveAssign(this, util::move(empty));
    mCategory = FLOATING_POINT;
    return &mFloatingValue;
}

AttributeValue::AttributeValue(bool v)
  : mCategory(BOOLEAN),
    mIntValue(0),
    mBoolValue(v),
    mFloatingValue(0)
{}

AttributeValue AttributeValue::toBoolean(bool v)
{
    return AttributeValue(v);
}

bool AttributeValue::boolean() const
{
    OTS_ASSERT(category() == BOOLEAN)(category());
    return mBoolValue;
}

bool* AttributeValue::mutableBoolean()
{
    AttributeValue empty;
    moveAssign(this, util::move(empty));
    mCategory = BOOLEAN;
    return &mBoolValue;
}


Attribute::Attribute(const string& name, const AttributeValue& val)
  : mName(name),
    mValue(val)
{}

Attribute::Attribute(const string& name, const AttributeValue& val, UtcTime ts)
  : mName(name),
    mValue(val),
    mTimestamp(ts)
{}

Attribute& Attribute::operator=(const MoveHolder<Attribute>& a)
{
    moveAssign(&mName, util::move(a->mName));
    moveAssign(&mValue, util::move(a->mValue));
    moveAssign(&mTimestamp, util::move(a->mTimestamp));
    return *this;
}

void Attribute::reset()
{
    mName.clear();
    mValue.reset();
    mTimestamp.reset();
}

void Attribute::prettyPrint(string* out) const
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

Optional<Error> Attribute::validate() const
{
    if (name().empty()) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Attribute name must be nonempty."));
    }
    {
        Optional<Error> err = value().validate();
        if (err.present()) {
            string msg("For column ");
            pp::prettyPrint(&msg, name());
            msg.append(", ");
            msg.append(err->message());
            *err->mutableMessage() = msg;
            return err;
        }
    }
    if (timestamp().present()) {
        if (timestamp()->toMsec() < 0) {
            string msg("Timestamp of column ");
            pp::prettyPrint(&msg, name());
            msg.append(" must be positive.");
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    msg));
        }
        if (timestamp()->toUsec() % kUsecPerMsec != 0) {
            string msg("Timestamp of column ");
            pp::prettyPrint(&msg, name());
            msg.append(" must be multiple of milliseconds.");
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

void Row::reset()
{
    mPkey.reset();
    mAttrs.reset();
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
    TRY(mPkey.validate());
    for(int64_t i = 0, sz = mAttrs.size(); i < sz; ++i) {
        TRY(mAttrs[i].validate());
    }
    return Optional<Error>();
}


TimeRange& TimeRange::operator=(const MoveHolder<TimeRange>& a)
{
    moveAssign(&mStart, util::move(a->mStart));
    moveAssign(&mEnd, util::move(a->mEnd));
    return *this;
}

void TimeRange::reset()
{
    mStart = UtcTime();
    mEnd = UtcTime();
}

void TimeRange::prettyPrint(string* out) const
{
    out->push_back('[');
    pp::prettyPrint(out, mStart);
    out->push_back(',');
    pp::prettyPrint(out, mEnd);
    out->push_back(']');
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

void Split::reset()
{
    mLowerBound.reset();
    mUpperBound.reset();
    mLocation.clear();
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
    TRY(mLowerBound->validate());
    TRY(mUpperBound->validate());
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
    moveAssign(&mMemHolder, util::move(a->mMemHolder));
    return *this;
}

void Response::reset()
{
    mRequestId.clear();
    mTraceId.clear();
    mMemHolder.reset();
}

void Response::prettyPrint(string* out) const
{
    if (!mRequestId.empty()) {
        out->append(",\"RequestId\":");
        pp::prettyPrint(out, mRequestId);
    }
    if (!mTraceId.empty()) {
        out->append(",\"TraceId\":");
        pp::prettyPrint(out, mTraceId);
    }
}


CreateTableRequest::CreateTableRequest()
  : mMeta(),
    mOptions(),
    mShardSplitPoints()
{
    CapacityUnit tmp(0, 0);
    *mutableOptions()->mutableReservedThroughput() = util::move(tmp);
    *mutableOptions()->mutableMaxVersions() = 1;
}

CreateTableRequest& CreateTableRequest::operator=(
    const MoveHolder<CreateTableRequest>& a)
{
    moveAssign(&mMeta, util::move(a->mMeta));
    moveAssign(&mOptions, util::move(a->mOptions));
    moveAssign(&mShardSplitPoints, util::move(a->mShardSplitPoints));
    return *this;
}

void CreateTableRequest::reset()
{
    mMeta.reset();
    mOptions.reset();
    mShardSplitPoints.reset();
}

void CreateTableRequest::prettyPrint(string* out) const
{
    out->append("{\"API\":\"CreateTableRequest\",\"Meta\":");
    pp::prettyPrint(out, mMeta);
    out->append(",\"Options\":");
    pp::prettyPrint(out, mOptions);
    out->append(",\"ShardSplitPoints\":");
    pp::prettyPrint(out, mShardSplitPoints);
    out->push_back('}');
}

Optional<Error> CreateTableRequest::validate() const
{
    TRY(meta().validate());
    TRY(options().validate());
    if (!options().reservedThroughput().present()) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Both read and write capacity units are required."));
    }
    if (!options().reservedThroughput()->read().present()) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Both read and write capacity units are required."));
    }
    if (!options().reservedThroughput()->write().present()) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Both read and write capacity units are required."));
    }
    if (*options().reservedThroughput()->read() < 0) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Read capacity units must be positive."));
    }
    if (*options().reservedThroughput()->write() < 0) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Write capacity units must be positive."));
    }
    if (!options().maxVersions().present()) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "MaxVersions is missing while creating table."));
    }
    for(int64_t i = 0, sz = mShardSplitPoints.size(); i < sz; ++i) {
        const PrimaryKey& pk = mShardSplitPoints[i];
        TRY(pk.validate());
    }
    for(int64_t i = 0, sz = mShardSplitPoints.size(); i < sz; ++i) {
        const PrimaryKey& pk = mShardSplitPoints[i];
        if (pk.size() != 1) {
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    "Length of shard split points must be exactly one."));
        }
    }
    for(int64_t i = 0, sz = mShardSplitPoints.size(); i < sz; ++i) {
        const PrimaryKey& pk = mShardSplitPoints[i];
        OTS_ASSERT(pk.size() >= 1)(pk.size());
        const PrimaryKeyColumn& pkc = pk[0];
        if (!pkc.value().isReal()) {
            string msg = "Shard split points contains an unreal value type ";
            pp::prettyPrint(&msg, pkc.value().category());
            msg.push_back('.');
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    msg));
        }
        const Schema& schema = mMeta.schema();
        OTS_ASSERT(schema.size() >= 1)
            (schema.size());
        const PrimaryKeyColumnSchema& colSchema = schema[0];
        if (MemPiece::from(pkc.name()) != MemPiece::from(colSchema.name())) {
            string msg = "Shard split points contains ";
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

void CreateTableResponse::reset()
{
    Response::reset();
}

void CreateTableResponse::prettyPrint(string* out) const
{
    out->append("{\"API\":\"CreateTableResponse\"");
    Response::prettyPrint(out);
    out->push_back('}');
}

Optional<Error> CreateTableResponse::validate() const
{
    return Optional<Error>();
}


void ListTableRequest::reset()
{
}

void ListTableRequest::prettyPrint(string* out) const
{
    out->append("{\"API\":\"ListTableRequest\"}");
}

Optional<Error> ListTableRequest::validate() const
{
    return Optional<Error>();
}


ListTableResponse::ListTableResponse(const MoveHolder<ListTableResponse>& a)
{
    *this = a;
}

ListTableResponse& ListTableResponse::operator=(const MoveHolder<ListTableResponse>& a)
{
    moveAssign(&mTables, util::move(a->mTables));
    Response::operator=(util::move(static_cast<Response&>(*a)));
    return *this;
}

void ListTableResponse::reset()
{
    Response::reset();
    mTables.reset();
}

void ListTableResponse::prettyPrint(string* out) const
{
    out->append("{\"API\":\"ListTableResponse\",\"Tables\":");
    pp::prettyPrint(out, mTables);
    Response::prettyPrint(out);
    out->push_back('}');
}

Optional<Error> ListTableResponse::validate() const
{
    return Optional<Error>();
}


DeleteTableRequest::DeleteTableRequest(const MoveHolder<DeleteTableRequest>& a)
{
    *this = a;
}

DeleteTableRequest& DeleteTableRequest::operator=(const MoveHolder<DeleteTableRequest>& a)
{
    moveAssign(&mTable, util::move(a->mTable));
    return *this;
}

void DeleteTableRequest::reset()
{
    mTable.clear();
}

void DeleteTableRequest::prettyPrint(string* out) const
{
    out->append("{\"API\":\"DeleteTableRequest\",\"Table\":");
    pp::prettyPrint(out, mTable);
    out->push_back('}');
}

Optional<Error> DeleteTableRequest::validate() const
{
    if (table().empty()) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Table name must be nonempty."));
    }
    return Optional<Error>();
}

DeleteTableResponse::DeleteTableResponse(const MoveHolder<DeleteTableResponse>& a)
{
    *this = a;
}

DeleteTableResponse& DeleteTableResponse::operator=(const MoveHolder<DeleteTableResponse>& a)
{
    Response::operator=(util::move(static_cast<Response&>(*a)));
    return *this;
}

void DeleteTableResponse::reset()
{
    Response::reset();
}

void DeleteTableResponse::prettyPrint(string* out) const
{
    out->append("{\"API\":\"DeleteTableResponse\"");
    Response::prettyPrint(out);
    out->push_back('}');
}

Optional<Error> DeleteTableResponse::validate() const
{
    return Optional<Error>();
}


DescribeTableRequest::DescribeTableRequest(
    const MoveHolder<DescribeTableRequest>& a)
{
    *this = a;
}

DescribeTableRequest& DescribeTableRequest::operator=(
    const MoveHolder<DescribeTableRequest>& a)
{
    moveAssign(&mTable, util::move(a->mTable));
    return *this;
}

void DescribeTableRequest::reset()
{
    mTable.clear();
}

void DescribeTableRequest::prettyPrint(string* out) const
{
    out->append("{\"API\":\"DescribeTableRequest\",\"Table\":");
    pp::prettyPrint(out, table());
    out->push_back('}');
}

Optional<Error> DescribeTableRequest::validate() const
{
    if (table().empty()) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Table name must be nonempty."));
    }
    return Optional<Error>();
}


DescribeTableResponse::DescribeTableResponse(
    const MoveHolder<DescribeTableResponse>& a)
{
    *this = a;
}

DescribeTableResponse& DescribeTableResponse::operator=(
    const MoveHolder<DescribeTableResponse>& a)
{
    moveAssign(&mMeta, util::move(a->mMeta));
    moveAssign(&mOptions, util::move(a->mOptions));
    moveAssign(&mStatus, util::move(a->mStatus));
    moveAssign(&mShardSplitPoints, util::move(a->mShardSplitPoints));
    Response::operator=(util::move(static_cast<Response&>(*a)));
    return *this;
}

void DescribeTableResponse::reset()
{
    Response::reset();
    mMeta.reset();
    mOptions.reset();
    mStatus = ACTIVE;
    mShardSplitPoints.reset();
}

void DescribeTableResponse::prettyPrint(string* out) const
{
    out->append("{\"API\":\"DescribeTableResponse\",\"TableMeta\":");
    pp::prettyPrint(out, mMeta);
    out->append(",\"TableOptions\":");
    pp::prettyPrint(out, mOptions);
    out->append(",\"TableStatus\":");
    pp::prettyPrint(out, mStatus);
    out->append(",\"ShardSplitPoints\":");
    pp::prettyPrint(out, mShardSplitPoints);
    Response::prettyPrint(out);
    out->push_back('}');
}

Optional<Error> DescribeTableResponse::validate() const
{
    TRY(mMeta.validate());
    TRY(mOptions.validate());
    for(int64_t i = 0, sz = mShardSplitPoints.size(); i < sz; ++i) {
        TRY(mShardSplitPoints[i].validate());
    }
    return Optional<Error>();
}


UpdateTableRequest::UpdateTableRequest(const MoveHolder<UpdateTableRequest>& a)
{
    *this = a;
}

UpdateTableRequest& UpdateTableRequest::operator=(const MoveHolder<UpdateTableRequest>& a)
{
    moveAssign(&mTable, util::move(a->mTable));
    moveAssign(&mOptions, util::move(a->mOptions));
    return *this;
}

void UpdateTableRequest::reset()
{
    mTable.clear();
    mOptions.reset();
}

void UpdateTableRequest::prettyPrint(string* out) const
{
    out->append("{\"API\":\"UpdateTableRequest\",\"TableName\":");
    pp::prettyPrint(out, mTable);
    out->append(",\"TableOptions\":");
    pp::prettyPrint(out, mOptions);
    out->push_back('}');
}

Optional<Error> UpdateTableRequest::validate() const
{
    if (mTable.empty()) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Table name is required."));
    }
    TRY(mOptions.validate());
    return Optional<Error>();
}


UpdateTableResponse::UpdateTableResponse(const MoveHolder<UpdateTableResponse>& a)
{
    *this = a;
}

UpdateTableResponse& UpdateTableResponse::operator=(const MoveHolder<UpdateTableResponse>& a)
{
    Response::operator=(util::move(static_cast<Response&>(*a)));
    return *this;
}

void UpdateTableResponse::reset()
{
    Response::reset();
}

void UpdateTableResponse::prettyPrint(string* out) const
{
    out->append("{\"API\":\"UpdateTableResponse\"");
    Response::prettyPrint(out);
    out->push_back('}');
}

Optional<Error> UpdateTableResponse::validate() const
{
    return Optional<Error>();
}


ComputeSplitsBySizeRequest::ComputeSplitsBySizeRequest(
    const MoveHolder<ComputeSplitsBySizeRequest>& a)
{
    *this = a;
}

ComputeSplitsBySizeRequest& ComputeSplitsBySizeRequest::operator=(
    const MoveHolder<ComputeSplitsBySizeRequest>& a)
{
    moveAssign(&mTable, util::move(a->mTable));
    moveAssign(&mSplitSize, util::move(a->mSplitSize));
    return *this;
}

void ComputeSplitsBySizeRequest::reset()
{
    mTable.clear();
    mSplitSize = kDefaultSplitSize;
}

void ComputeSplitsBySizeRequest::prettyPrint(string* out) const
{
    out->append("{\"API\":\"ComputeSplitsBySizeRequest\",\"TableName\":");
    pp::prettyPrint(out, mTable);
    out->append(",\"SplitSize\":");
    pp::prettyPrint(out, mSplitSize);
    out->push_back('}');
}

Optional<Error> ComputeSplitsBySizeRequest::validate() const
{
    if (mTable.empty()) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Table name must be nonempty."));
    }
    if (mSplitSize <= 0) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Split size must be positive."));
    }
    return Optional<Error>();
}


ComputeSplitsBySizeResponse::ComputeSplitsBySizeResponse(
    const MoveHolder<ComputeSplitsBySizeResponse>& a)
{
    *this = a;
}

ComputeSplitsBySizeResponse& ComputeSplitsBySizeResponse::operator=(
    const MoveHolder<ComputeSplitsBySizeResponse>& a)
{
    moveAssign(&mConsumedCapacity, util::move(a->mConsumedCapacity));
    moveAssign(&mSchema, util::move(a->mSchema));
    moveAssign(&mSplits, util::move(a->mSplits));
    Response::operator=(util::move(static_cast<Response&>(*a)));
    return *this;
}

void ComputeSplitsBySizeResponse::reset()
{
    Response::reset();
    mConsumedCapacity.reset();
    mSchema.reset();
    mSplits.reset();
}

void ComputeSplitsBySizeResponse::prettyPrint(string* out) const
{
    out->append("{\"API\":\"ComputeSplitsBySizeResponse\",\"ConsumedCapcityUnit\":");
    pp::prettyPrint(out, mConsumedCapacity);
    out->append(",\"Schema\":");
    pp::prettyPrint(out, mSchema);
    out->append(",\"Splits\":");
    pp::prettyPrint(out, mSplits);
    Response::prettyPrint(out);
    out->push_back('}');
}

Optional<Error> ComputeSplitsBySizeResponse::validate() const
{
    TRY(mConsumedCapacity.validate());
    TRY(mSchema.validate());
    for(int64_t i = 0, sz = mSplits.size(); i < sz; ++i) {
        TRY(mSplits[i].validate());
    }
    return Optional<Error>();
}


Condition& Condition::operator=(const MoveHolder<Condition>& a)
{
    mRowCondition = a->mRowCondition;
    return *this;
}

void Condition::reset()
{
    mRowCondition = IGNORE;
    mColumnCondition.reset();
}

void Condition::prettyPrint(string* out) const
{
    out->append("{\"RowCondition\":");
    pp::prettyPrint(out, mRowCondition);
    if (mColumnCondition.get() != NULL) {
        out->append(",\"ColumnCondition\":");
        pp::prettyPrint(out, *mColumnCondition);
    }
    out->push_back('}');
}

Optional<Error> Condition::validate() const
{
    if (mColumnCondition.get() != NULL) {
        TRY(mColumnCondition->validate());
    }
    return Optional<Error>();
}

SingleColumnCondition& SingleColumnCondition::operator=(
    const MoveHolder<SingleColumnCondition>& a)
{
    moveAssign(&mColumnName, util::move(a->mColumnName));
    mRelation = a->mRelation;
    moveAssign(&mColumnValue, util::move(a->mColumnValue));
    mPassIfMissing = a->mPassIfMissing;
    mLatestVersionOnly = a->mLatestVersionOnly;
    return *this;
}

void SingleColumnCondition::reset()
{
    mColumnName.clear();
    mRelation = EQUAL;
    mColumnValue.reset();
    mPassIfMissing = false;
    mLatestVersionOnly = true;
}

void SingleColumnCondition::prettyPrint(string* out) const
{
    out->append("{\"Relation\":");
    pp::prettyPrint(out, mRelation);
    out->append(",\"ColumnName\":");
    pp::prettyPrint(out, mColumnName);
    out->append(",\"ColumnValue\":");
    pp::prettyPrint(out, mColumnValue);
    out->append(",\"PassIfMissing\":");
    pp::prettyPrint(out, mPassIfMissing);
    out->append(",\"LatestVersionOnly\":");
    pp::prettyPrint(out, mLatestVersionOnly);
    out->push_back('}');
}

Optional<Error> SingleColumnCondition::validate() const
{
    if (mColumnName.empty()) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Column name is required."));
    }
    TRY(mColumnValue.validate());
    if (mColumnValue.category() == AttributeValue::NONE) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Column value is required."));
    }
    return Optional<Error>();
}


CompositeColumnCondition& CompositeColumnCondition::operator=(
    const MoveHolder<CompositeColumnCondition>& a)
{
    mOperator = a->mOperator;
    moveAssign(&mChildren, util::move(a->mChildren));
    return *this;
}

void CompositeColumnCondition::reset()
{
    mOperator = AND;
    mChildren.reset();
}

void CompositeColumnCondition::prettyPrint(string* out) const
{
    out->append("{\"Operator\":");
    pp::prettyPrint(out, mOperator);
    out->append(",\"Children\":");
    pp::prettyPrint(out, mChildren);
    out->push_back('}');
}

Optional<Error> CompositeColumnCondition::validate() const
{
    for(int64_t i = 0, sz = mChildren.size(); i < sz; ++i) {
        if (mChildren[i].get() == NULL) {
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    "Children of a composite column condition must be nonnull."));
        }
        TRY(mChildren[i]->validate());
    }
    return Optional<Error>();
}

void RowChange::move(RowChange& a)
{
    moveAssign(&mTable, util::move(a.mTable));
    moveAssign(&mPrimaryKey, util::move(a.mPrimaryKey));
    moveAssign(&mCondition, util::move(a.mCondition));
    mReturnType = a.mReturnType;
}

void RowChange::reset()
{
    mTable.clear();
    mPrimaryKey.reset();
    mCondition.reset();
    mReturnType = RT_NONE;
}

void RowChange::prettyPrint(string* out) const
{
    out->append("\"TableName\":");
    pp::prettyPrint(out, mTable);
    out->append(",\"PrimaryKey\":");
    pp::prettyPrint(out, mPrimaryKey);
    out->append(",\"Condition\":");
    pp::prettyPrint(out, mCondition);
    out->append(",\"ReturnType\":");
    pp::prettyPrint(out, mReturnType);
}

Optional<Error> RowChange::validate() const
{
    if (mTable.empty()) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Table name is required."));
    }
    TRY(mPrimaryKey.validate());
    for(int64_t i = 0, sz = mPrimaryKey.size(); i < sz; ++i) {
        if (mPrimaryKey[i].value().isInfinity()) {
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    "Infinity is not allowed in GetRange."));
        }
    }
    TRY(mCondition.validate());
    return Optional<Error>();
}

RowPutChange& RowPutChange::operator=(const MoveHolder<RowPutChange>& a)
{
    moveAssign(&mAttrs, util::move(a->mAttrs));
    RowChange::move(*a);
    return *this;
}

void RowPutChange::reset()
{
    RowChange::reset();
    mAttrs.reset();
}

void RowPutChange::prettyPrint(string* out) const
{
    out->append("{\"ChangeType\":\"RowPutChange\",");
    RowChange::prettyPrint(out);
    out->append(",\"Columns\":");
    pp::prettyPrint(out, mAttrs);
    out->push_back('}');
}

Optional<Error> RowPutChange::validate() const
{
    TRY(RowChange::validate());
    for(int64_t i = 0, sz = mAttrs.size(); i < sz; ++i) {
        TRY(mAttrs[i].validate());
    }
    return Optional<Error>();
}

PutRowRequest& PutRowRequest::operator=(const MoveHolder<PutRowRequest>& a)
{
    moveAssign(&mRowChange, util::move(a->mRowChange));
    return *this;
}

void PutRowRequest::reset()
{
    mRowChange.reset();
}

void PutRowRequest::prettyPrint(string* out) const
{
    out->append("{\"API\":\"PutRowRequest\",\"RowChange\":");
    pp::prettyPrint(out, mRowChange);
    out->push_back('}');
}

Optional<Error> PutRowRequest::validate() const
{
    return mRowChange.validate();
}


PutRowResponse& PutRowResponse::operator=(const MoveHolder<PutRowResponse>& a)
{
    moveAssign(&mConsumedCapacity, util::move(a->mConsumedCapacity));
    moveAssign(&mRow, util::move(a->mRow));
    Response::operator=(util::move(static_cast<Response&>(*a)));
    return *this;
}

void PutRowResponse::reset()
{
    Response::reset();
    mConsumedCapacity.reset();
    mRow.reset();
}

void PutRowResponse::prettyPrint(string* out) const
{
    out->append("{\"API\":\"PutRowResponse\",\"ConsumedCapacity\":");
    pp::prettyPrint(out, mConsumedCapacity);
    if (mRow.present()) {
        out->append(",\"Row\":");
        pp::prettyPrint(out, *mRow);
    }
    Response::prettyPrint(out);
    out->push_back('}');
}

Optional<Error> PutRowResponse::validate() const
{
    TRY(mConsumedCapacity.validate());
    if (mRow.present()) {
        TRY(mRow->validate());
    }
    return Optional<Error>();
}


QueryCriterion& QueryCriterion::operator=(const MoveHolder<QueryCriterion>& a)
{
    moveAssign(&mTable, util::move(a->mTable));
    moveAssign(&mColumnsToGet, util::move(a->mColumnsToGet));
    moveAssign(&mMaxVersions, util::move(a->mMaxVersions));
    moveAssign(&mTimeRange, util::move(a->mTimeRange));
    moveAssign(&mCacheBlocks, util::move(a->mCacheBlocks));
    moveAssign(&mFilter, util::move(a->mFilter));
    return *this;
}

void QueryCriterion::reset()
{
    mTable.clear();
    mColumnsToGet.reset();
    mMaxVersions.reset();
    mTimeRange.reset();
    mCacheBlocks.reset();
    mFilter.reset();
}

void QueryCriterion::prettyPrint(string* out) const
{
    out->append("\"TableName\":");
    pp::prettyPrint(out, mTable);
    out->append(",\"ColumnsToGet\":");
    pp::prettyPrint(out, mColumnsToGet);
    if (mMaxVersions.present()) {
        out->append(",\"MaxVersions\":");
        pp::prettyPrint(out, *mMaxVersions);
    }
    if (mTimeRange.present()) {
        out->append(",\"TimeRange\":");
        pp::prettyPrint(out, *mTimeRange);
    }
    if (mCacheBlocks.present()) {
        out->append(",\"CacheBlocks\":");
        pp::prettyPrint(out, *mCacheBlocks);
    }
    if (mFilter.get() != NULL) {
        out->append(",\"Filter\":");
        pp::prettyPrint(out, *mFilter);
    }
}

Optional<Error> QueryCriterion::validate() const
{
    if (mTable.empty()) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Table name is required."));
    }
    for(int64_t i = 0, sz = mColumnsToGet.size(); i < sz; ++i) {
        if (mColumnsToGet[i].empty()) {
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    "Columns in ColumnsToGet must be nonempty."));
        }
    }
    if (!mMaxVersions.present() && !mTimeRange.present()) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Either MaxVersions or TimeRange is required."));
    }
    if (mMaxVersions.present() && *mMaxVersions <= 0) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "MaxVersions must be positive."));
    }
    if (mTimeRange.present()) {
        TRY(mTimeRange->validate());
    }
    if (mFilter.get() != NULL) {
        TRY(mFilter->validate());
    }
    return Optional<Error>();
}

PointQueryCriterion& PointQueryCriterion::operator=(
    const MoveHolder<PointQueryCriterion>& a)
{
    QueryCriterion::operator=(util::move(static_cast<QueryCriterion&>(*a)));
    moveAssign(&mPrimaryKey, util::move(a->mPrimaryKey));
    return *this;
}

void PointQueryCriterion::reset()
{
    QueryCriterion::reset();
    mPrimaryKey.reset();
}

void PointQueryCriterion::prettyPrint(string* out) const
{
    out->push_back('{');
    QueryCriterion::prettyPrint(out);
    out->append(",\"PrimaryKey\":");
    pp::prettyPrint(out, mPrimaryKey);
    out->push_back('}');
}

Optional<Error> PointQueryCriterion::validate() const
{
    TRY(QueryCriterion::validate());
    TRY(mPrimaryKey.validate());
    return Optional<Error>();
}


GetRowRequest& GetRowRequest::operator=(const MoveHolder<GetRowRequest>& a)
{
    moveAssign(&mQueryCriterion, util::move(a->mQueryCriterion));
    return *this;
}

void GetRowRequest::reset()
{
    mQueryCriterion.reset();
}

void GetRowRequest::prettyPrint(string* out) const
{
    out->append("{\"API\":\"GetRowRequest\",\"QueryCriterion\":");
    pp::prettyPrint(out, mQueryCriterion);
    out->push_back('}');
}

Optional<Error> GetRowRequest::validate() const
{
    return mQueryCriterion.validate();
}


GetRowResponse& GetRowResponse::operator=(const MoveHolder<GetRowResponse>& a)
{
    moveAssign(&mConsumedCapacity, util::move(a->mConsumedCapacity));
    moveAssign(&mRow, util::move(a->mRow));
    return *this;
}

void GetRowResponse::reset()
{
    Response::reset();
    mConsumedCapacity.reset();
    mRow.reset();
}

void GetRowResponse::prettyPrint(string* out) const
{
    out->append("{\"API\":\"GetRowResponse\",\"ConsumedCapacity\":");
    pp::prettyPrint(out, mConsumedCapacity);
    if (mRow.present()) {
        out->append(",\"Row\":");
        pp::prettyPrint(out, *mRow);
    }
    Response::prettyPrint(out);
    out->push_back('}');
}

Optional<Error> GetRowResponse::validate() const
{
    TRY(mConsumedCapacity.validate());
    if (mRow.present()) {
        TRY(mRow->validate());
    }
    return Optional<Error>();
}

RangeQueryCriterion& RangeQueryCriterion::operator=(
    const MoveHolder<RangeQueryCriterion>& a)
{
    moveAssign(&mDirection, util::move(a->mDirection));
    moveAssign(&mInclusiveStart, util::move(a->mInclusiveStart));
    moveAssign(&mExclusiveEnd, util::move(a->mExclusiveEnd));
    moveAssign(&mLimit, util::move(a->mLimit));
    QueryCriterion::operator=(util::move(static_cast<QueryCriterion&>(*a)));
    return *this;
}

void RangeQueryCriterion::reset()
{
    QueryCriterion::reset();
    mDirection = FORWARD;
    mInclusiveStart.reset();
    mExclusiveEnd.reset();
    mLimit.reset();
}

void RangeQueryCriterion::prettyPrint(string* out) const
{
    out->append("{\"Direction\":");
    pp::prettyPrint(out, mDirection);
    out->append(",\"Start\":");
    pp::prettyPrint(out, mInclusiveStart);
    out->append(",\"End\":");
    pp::prettyPrint(out, mExclusiveEnd);
    if (mLimit.present()) {
        out->append(",\"Limit\":");
        pp::prettyPrint(out, *mLimit);
    }
    out->push_back('}');
}

Optional<Error> RangeQueryCriterion::validate() const
{
    TRY(QueryCriterion::validate());
    TRY(mInclusiveStart.validate());
    TRY(mExclusiveEnd.validate());
    if (mInclusiveStart.size() == 0) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Start primary key is required."));
    }
    if (mExclusiveEnd.size() == 0) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "End primary key is required."));
    }
    if (mInclusiveStart.size() != mExclusiveEnd.size()) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Start primary key must be of the same length of that of the end."));
    }
    {
        CompareResult r = mInclusiveStart.compare(mExclusiveEnd);
        if (mDirection == FORWARD) {
            if (r == UNCOMPARABLE || r == GREATER_THAN) {
                return Optional<Error>(Error(
                        -1,
                        "OTSParameterInvalid",
                        "Start primary key should be less than or equals to the end in a forward range."));
            }
        } else {
            if (r == UNCOMPARABLE || r == LESS_THAN) {
                return Optional<Error>(Error(
                        -1,
                        "OTSParameterInvalid",
                        "Start primary key should be greater than or equals to the end in a backward range."));
            }
        }
    }
    if (mLimit.present()) {
        if (*mLimit <= 0) {
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    "Limit of GetRange must be positive."));
        }
    }
    return Optional<Error>();
}


GetRangeRequest& GetRangeRequest::operator=(
    const MoveHolder<GetRangeRequest>& a)
{
    moveAssign(&mQueryCriterion, util::move(a->mQueryCriterion));
    return *this;
}

void GetRangeRequest::reset()
{
    mQueryCriterion.reset();
}

void GetRangeRequest::prettyPrint(string* out) const
{
    out->append("{\"API\":\"GetRangeRequest\",\"QueryCriterion\":");
    pp::prettyPrint(out, mQueryCriterion);
    out->push_back('}');
}

Optional<Error> GetRangeRequest::validate() const
{
    TRY(mQueryCriterion.validate());
    return Optional<Error>();
}


GetRangeResponse& GetRangeResponse::operator=(
    const MoveHolder<GetRangeResponse>& a)
{
    moveAssign(&mConsumedCapacity, util::move(a->mConsumedCapacity));
    moveAssign(&mRows, util::move(a->mRows));
    moveAssign(&mNextStart, util::move(a->mNextStart));
    Response::operator=(util::move(static_cast<Response&>(*a)));
    return *this;
}

void GetRangeResponse::reset()
{
    Response::reset();
    mConsumedCapacity.reset();
    mRows.reset();
    mNextStart.reset();
}

void GetRangeResponse::prettyPrint(string* out) const
{
    out->append("{\"API\":\"GetRangeResponse\",\"ConsumedCapacity\":");
    pp::prettyPrint(out, mConsumedCapacity);
    out->append(",\"Rows\":");
    pp::prettyPrint(out, mRows);
    if (mNextStart.present()) {
        out->append(",\"NextStart\":");
        pp::prettyPrint(out, *mNextStart);
    }
    Response::prettyPrint(out);
    out->push_back('}');
}

Optional<Error> GetRangeResponse::validate() const
{
    TRY(mConsumedCapacity.validate());
    for(int64_t i = 0, sz = mRows.size(); i < sz; ++i) {
        TRY(mRows[i].validate());
    }
    if (mNextStart.present()) {
        TRY(mNextStart->validate());
    }
    return Optional<Error>();
}

RowUpdateChange::Update& RowUpdateChange::Update::operator=(
    const MoveHolder<RowUpdateChange::Update>& a)
{
    moveAssign(&mType, util::move(a->mType));
    moveAssign(&mAttrName, util::move(a->mAttrName));
    moveAssign(&mAttrValue, util::move(a->mAttrValue));
    moveAssign(&mTimestamp, util::move(a->mTimestamp));
    return *this;
}

void RowUpdateChange::reset()
{
    RowChange::reset();
    mUpdates.reset();
}

void RowUpdateChange::Update::prettyPrint(string* out) const
{
    out->append("{\"UpdateType\":");
    pp::prettyPrint(out, mType);
    out->append(",\"AttrName\":");
    pp::prettyPrint(out, mAttrName);
    if (mAttrValue.present()) {
        out->append(",\"AttrValue\":");
        pp::prettyPrint(out, *mAttrValue);
    }
    if (mTimestamp.present()) {
        out->append(",\"Timestamp\":");
        pp::prettyPrint(out, *mTimestamp);
    }
    out->push_back('}');
}

Optional<Error> RowUpdateChange::Update::validate() const
{
    if (mAttrName.empty()) {
        return Optional<Error>(Error(
                -1,
                "OTSParameterInvalid",
                "Attribute name is required."));
    }
    if (mAttrValue.present()) {
        TRY(mAttrValue->validate());
    }
    switch(mType) {
    case PUT:
        if (!mAttrValue.present()) {
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    "Attribute value is required for PUT update."));
        }
        break;
    case DELETE: {
        if (mAttrValue.present()) {
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    "Attribute value should not be specified for DELETE update."));
        }
        break;
    }
    case DELETE_ALL: {
        if (mAttrValue.present()) {
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    "Attribute value should not be specified for DELETE_ALL update."));
        }
        if (mTimestamp.present()) {
            return Optional<Error>(Error(
                    -1,
                    "OTSParameterInvalid",
                    "Timestamp should not be specified for DELETE_ALL update."));
        }
        break;
    }
    }
    return Optional<Error>();
}

RowUpdateChange& RowUpdateChange::operator=(const MoveHolder<RowUpdateChange>& a)
{
    moveAssign(&mUpdates, util::move(a->mUpdates));
    RowChange::move(*a);
    return *this;
}

void RowUpdateChange::prettyPrint(string* out) const
{
    out->append("{\"ChangeType\":\"RowUpdateChange\",");
    RowChange::prettyPrint(out);
    out->append(",\"Update\":");
    pp::prettyPrint(out, mUpdates);
    out->push_back('}');
}

Optional<Error> RowUpdateChange::validate() const
{
    TRY(RowChange::validate());
    for(int64_t i = 0, sz = mUpdates.size(); i < sz; ++i) {
        TRY(mUpdates[i].validate());
    }
    return Optional<Error>();
}


UpdateRowRequest& UpdateRowRequest::operator=(const MoveHolder<UpdateRowRequest>& a)
{
    moveAssign(&mRowChange, util::move(a->mRowChange));
    return *this;
}

void UpdateRowRequest::reset()
{
    mRowChange.reset();
}

void UpdateRowRequest::prettyPrint(string* out) const
{
    out->append("{\"API\":\"UpdateRowRequest\",\"RowChange\":");
    pp::prettyPrint(out, mRowChange);
    out->push_back('}');
}

Optional<Error> UpdateRowRequest::validate() const
{
    TRY(mRowChange.validate());
    return Optional<Error>();
}

UpdateRowResponse& UpdateRowResponse::operator=(
    const MoveHolder<UpdateRowResponse>& a)
{
    moveAssign(&mConsumedCapacity, util::move(a->mConsumedCapacity));
    moveAssign(&mRow, util::move(a->mRow));
    Response::operator=(util::move(static_cast<Response&>(*a)));
    return *this;
}

void UpdateRowResponse::reset()
{
    Response::reset();
    mConsumedCapacity.reset();
    mRow.reset();
}

void UpdateRowResponse::prettyPrint(string* out) const
{
    out->append("{\"API\":\"UpdateRowResponse\",\"ConsumedCapacity\":");
    pp::prettyPrint(out, mConsumedCapacity);
    if (mRow.present()) {
        out->append(",\"Row\":");
        pp::prettyPrint(out, *mRow);
    }
    Response::prettyPrint(out);
    out->push_back('}');
}

Optional<Error> UpdateRowResponse::validate() const
{
    TRY(mConsumedCapacity.validate());
    if (mRow.present()) {
        TRY(mRow->validate());
    }
    return Optional<Error>();
}


void RowDeleteChange::reset()
{
    RowChange::reset();
}

void RowDeleteChange::prettyPrint(string* out) const
{
    out->append("{\"ChangeType\":\"RowPutChange\",");
    RowChange::prettyPrint(out);
    out->push_back('}');
}

Optional<Error> RowDeleteChange::validate() const
{
    TRY(RowChange::validate());
    return Optional<Error>();
}


DeleteRowRequest& DeleteRowRequest::operator=(
    const MoveHolder<DeleteRowRequest>& a)
{
    moveAssign(&mRowChange, util::move(a->mRowChange));
    return *this;
}

void DeleteRowRequest::reset()
{
    mRowChange.reset();
}

void DeleteRowRequest::prettyPrint(string* out) const
{
    out->append("{\"API\":\"DeleteRowRequest\",\"RowChange\":");
    pp::prettyPrint(out, mRowChange);
    out->push_back('}');
}

Optional<Error> DeleteRowRequest::validate() const
{
    TRY(mRowChange.validate());
    return Optional<Error>();
}


DeleteRowResponse& DeleteRowResponse::operator=(
    const MoveHolder<DeleteRowResponse>& a)
{
    moveAssign(&mConsumedCapacity, util::move(a->mConsumedCapacity));
    moveAssign(&mRow, util::move(a->mRow));
    Response::operator=(util::move(static_cast<Response&>(*a)));
    return *this;
}

void DeleteRowResponse::reset()
{
    Response::reset();
    mConsumedCapacity.reset();
    mRow.reset();
}

void DeleteRowResponse::prettyPrint(string* out) const
{
    out->append("{\"API\":\"DeleteRowResponse\"");
    Response::prettyPrint(out);
    out->append(",\"ConsumedCapacity\":");
    pp::prettyPrint(out, mConsumedCapacity);
    if (mRow.present()) {
        out->append(",\"Row\":");
        pp::prettyPrint(out, *mRow);
    }
    out->push_back('}');
}

Optional<Error> DeleteRowResponse::validate() const
{
    TRY(mConsumedCapacity.validate());
    if (mRow.present()) {
        TRY(mRow->validate());
    }
    return Optional<Error>();
}


MultiPointQueryCriterion::MultiPointQueryCriterion(
    const MoveHolder<MultiPointQueryCriterion>& a)
{
    *this = a;
}

MultiPointQueryCriterion& MultiPointQueryCriterion::operator=(
    const MoveHolder<MultiPointQueryCriterion>& a)
{
    QueryCriterion::operator=(util::move(static_cast<QueryCriterion&>(*a)));
    moveAssign(&mRowKeys, util::move(a->mRowKeys));
    return *this;
}

void MultiPointQueryCriterion::reset()
{
    QueryCriterion::reset();
    mRowKeys.reset();
}

void MultiPointQueryCriterion::prettyPrint(string* out) const
{
    out->append("{");
    QueryCriterion::prettyPrint(out);
    out->append(",\"RowKeys\":");
    pp::prettyPrint(out, mRowKeys);
    out->push_back('}');
}

Optional<Error> MultiPointQueryCriterion::validate() const
{
    TRY(QueryCriterion::validate());
    for(int64_t i = 0, sz = mRowKeys.size(); i < sz; ++i) {
        TRY(mRowKeys[i].get().validate());
    }
    return Optional<Error>();
}


BatchGetRowRequest::BatchGetRowRequest(const MoveHolder<BatchGetRowRequest>& a)
{
    *this = a;
}

BatchGetRowRequest& BatchGetRowRequest::operator=(
    const MoveHolder<BatchGetRowRequest>& a)
{
    moveAssign(&mCriteria, util::move(a->mCriteria));
    return *this;
}

void BatchGetRowRequest::reset()
{
    mCriteria.reset();
}

void BatchGetRowRequest::prettyPrint(string* out) const
{
    out->append("{\"API\":\"BatchGetRowRequest\",\"Criteria\":");
    pp::prettyPrint(out, mCriteria);
    out->push_back('}');
}

Optional<Error> BatchGetRowRequest::validate() const
{
    for(int64_t i = 0, sz = criteria().size(); i < sz; ++i) {
        TRY(criteria()[i].validate());
    }
    return Optional<Error>();
}


BatchGetRowResponse& BatchGetRowResponse::operator=(
    const MoveHolder<BatchGetRowResponse>& a)
{
    moveAssign(&mConsumedCapacity, util::move(a->mConsumedCapacity));
    moveAssign(&mResults, util::move(a->mResults));
    Response::operator=(util::move(static_cast<Response&>(*a)));
    return *this;
}

void BatchGetRowResponse::reset()
{
    Response::reset();
    mConsumedCapacity.reset();
    mResults.reset();
}

void BatchGetRowResponse::prettyPrint(string* out) const
{
    out->append("{\"API\":\"BatchGetRowResponse\",\"ConsumedCapacity\":");
    pp::prettyPrint(out, mConsumedCapacity);
    out->append(",\"Results\":");
    pp::prettyPrint(out, mResults);
    Response::prettyPrint(out);
    out->append("}");
}

Optional<Error> BatchGetRowResponse::validate() const
{
    TRY(mConsumedCapacity.validate());
    for(int64_t i = 0, sz = mResults.size(); i < sz; ++i) {
        const util::Result<Optional<Row>, Error>& result = mResults[i].get();
        if (result.ok()) {
            if (result.okValue().present()) {
                TRY(result.okValue()->validate());
            }
        }
    }
    return Optional<Error>();
}


BatchWriteRowRequest::BatchWriteRowRequest(
    const MoveHolder<BatchWriteRowRequest>& a)
{
    *this = a;
}

void BatchWriteRowRequest::reset()
{
    mPuts.reset();
    mUpdates.reset();
    mDeletes.reset();
}

BatchWriteRowRequest& BatchWriteRowRequest::operator=(
    const MoveHolder<BatchWriteRowRequest>& a)
{
    moveAssign(&mPuts, util::move(a->mPuts));
    moveAssign(&mUpdates, util::move(a->mUpdates));
    moveAssign(&mDeletes, util::move(a->mDeletes));
    return *this;
}

void BatchWriteRowRequest::prettyPrint(string* out) const
{
    out->append("{\"API\":\"BatchWriteRequest\",\"Puts\":");
    pp::prettyPrint(out, mPuts);
    out->append(",\"Updates\":");
    pp::prettyPrint(out, mUpdates);
    out->append(",\"Deletes\":");
    pp::prettyPrint(out, mDeletes);
    out->push_back('}');
}

Optional<Error> BatchWriteRowRequest::validate() const
{
    for(int64_t i = 0, sz = mPuts.size(); i < sz; ++i) {
        TRY(mPuts[i].get().validate());
    }
    for(int64_t i = 0, sz = mUpdates.size(); i < sz; ++i) {
        TRY(mUpdates[i].get().validate());
    }
    for(int64_t i = 0, sz = mDeletes.size(); i < sz; ++i) {
        TRY(mDeletes[i].get().validate());
    }
    return Optional<Error>();
}


BatchWriteRowResponse::BatchWriteRowResponse(
    const MoveHolder<BatchWriteRowResponse>& a)
{
    *this = a;
}

void BatchWriteRowResponse::reset()
{
    Response::reset();
    mConsumedCapacity.reset();
    mPutResults.reset();
    mUpdateResults.reset();
    mDeleteResults.reset();
}

BatchWriteRowResponse& BatchWriteRowResponse::operator=(
    const MoveHolder<BatchWriteRowResponse>& a)
{
    moveAssign(&mConsumedCapacity, util::move(a->mConsumedCapacity));
    moveAssign(&mPutResults, util::move(a->mPutResults));
    moveAssign(&mUpdateResults, util::move(a->mUpdateResults));
    moveAssign(&mDeleteResults, util::move(a->mDeleteResults));
    Response::operator=(util::move(static_cast<Response&>(*a)));
    return *this;
}

void BatchWriteRowResponse::prettyPrint(string* out) const
{
    out->append("{\"API\":\"BatchWriteResponse\",\"ConsumedCapacity\":");
    pp::prettyPrint(out, mConsumedCapacity);
    out->append(",\"PutResults\":");
    pp::prettyPrint(out, mPutResults);
    out->append(",\"UpdateResults\":");
    pp::prettyPrint(out, mUpdateResults);
    out->append(",\"DeleteResults\":");
    pp::prettyPrint(out, mDeleteResults);
    Response::prettyPrint(out);
    out->push_back('}');
}

Optional<Error> BatchWriteRowResponse::validate() const
{
    TRY(mConsumedCapacity.validate());
    for(int64_t i = 0, sz = putResults().size(); i < sz; ++i) {
        const util::Result<Optional<Row>, Error>& res = putResults()[i].get();
        if (res.ok() && res.okValue().present()) {
            TRY(res.okValue()->validate());
        }
    }
    for(int64_t i = 0, sz = updateResults().size(); i < sz; ++i) {
        const util::Result<Optional<Row>, Error>& res = updateResults()[i].get();
        if (res.ok() && res.okValue().present()) {
            TRY(res.okValue()->validate());
        }
    }
    for(int64_t i = 0, sz = deleteResults().size(); i < sz; ++i) {
        const util::Result<Optional<Row>, Error>& res = deleteResults()[i].get();
        if (res.ok() && res.okValue().present()) {
            TRY(res.okValue()->validate());
        }
    }
    return Optional<Error>();
}



} // namespace core
} // namespace tablestore
} // namespace aliyun

