#include "type_delegates.h"
#include "logging_assert.h"
#include "foreach.h"
#include "jsoncpp/json/writer.h"
#include <limits>

using namespace ::std;
using namespace ::std::tr1;

namespace static_index {

void Value::Init(Logger* logger, Type type)
{
    mLogger = logger;
    mType = type;
    mIntValue = 0;
    mBoolValue = false;
    mDblValue = 0;
}

int64_t Value::Int() const
{
    OTS_ASSERT(mLogger, mType == INTEGER)((int) mType);
    return mIntValue;
}

string Value::Str() const
{
    OTS_ASSERT(mLogger, mType == STRING)((int) mType);
    return mStrValue;
}

bool Value::Bool() const
{
    OTS_ASSERT(mLogger, mType == BOOLEAN)((int) mType);
    return mBoolValue;
}

double Value::Double() const
{
    OTS_ASSERT(mLogger, mType == DOUBLE)((int) mType);
    return mDblValue;
}

void Value::SetInfMin()
{
    Init(mLogger, INF_MIN);
}

void Value::SetInfMax()
{
    Init(mLogger, INF_MAX);
}

void Value::SetInt(int64_t x)
{
    Init(mLogger, INTEGER);
    mIntValue = x;
}

void Value::SetStr(const string& x)
{
    Init(mLogger, STRING);
    mStrValue = x;
}

void Value::SetBool(bool x)
{
    Init(mLogger, BOOLEAN);
    mBoolValue = x;
}

void Value::SetDouble(double x)
{
    Init(mLogger, DOUBLE);
    mDblValue = x;
}

Value Value::Next() const
{
    switch(GetType()) {
    case INF_MIN: case INF_MAX: return *this;
    case INTEGER: {
        int64_t x = Int();
        if (x == numeric_limits<int64_t>::max()) {
            return Value(mLogger, InfMax());
        } else {
            return Value(mLogger, x + 1);
        }
    }
    case STRING: {
        string n = Str();
        n.push_back('\0');
        return Value(mLogger, n);
    }
    case BOOLEAN: {
        if (Bool() == false) {
            return Value(mLogger, (bool) true);
        } else {
            return Value(mLogger, InfMax());
        }
    }
    case DOUBLE:
        return Value(mLogger, Double() + numeric_limits<double>::min());
    default:
        OTS_ASSERT(mLogger, false)((int) GetType()).What("Value has no next value");
        return *this;
    }
}

namespace {

template<typename T>
Value::ComparisonResult _Compare(T a, T b)
{
    if (a < b) {
        return Value::SMALLER;
    } else if (a == b) {
        return Value::EQUIV;
    } else {
        return Value::LARGER;
    }
}

template<>
Value::ComparisonResult _Compare<string>(
    string a,
    string b)
{
    int r = ToSlice(a).LexicographicOrder(ToSlice(b));
    if (r < 0) {
        return Value::SMALLER;
    } else if (r == 0) {
        return Value::EQUIV;
    } else {
        return Value::LARGER;
    }
}

} // namespace

Value::ComparisonResult Value::Compare(const Value& b) const
{
    OTS_ASSERT(mLogger, GetType() != INVALID);
    OTS_ASSERT(mLogger, b.GetType() != INVALID);
    if (IsInfMin()) {
        if (!b.IsInfMin()) {
            return SMALLER;
        } else {
            return TYPE_MISMATCH;
        }
    }
    if (b.IsInfMin()) {
        if (!IsInfMin()) {
            return LARGER;
        } else {
            return TYPE_MISMATCH;
        }
    }
    if (IsInfMax()) {
        if (!b.IsInfMax()) {
            return LARGER;
        } else {
            return TYPE_MISMATCH;
        }
    }
    if (b.IsInfMax()) {
        if (!IsInfMax()) {
            return SMALLER;
        } else {
            return TYPE_MISMATCH;
        }
    }
    if (GetType() != b.GetType()) {
        return TYPE_MISMATCH;
    }
    switch(GetType()) {
    case Value::INTEGER: return _Compare(Int(), b.Int());
    case Value::STRING: return _Compare(Str(), b.Str());
    case Value::BOOLEAN: return _Compare(Bool(), b.Bool());
    case Value::DOUBLE: return _Compare(Double(), b.Double());
    default:
        OTS_ASSERT(mLogger, false)
            ((int) GetType())
            .What("unsupported value type");
    }
    return TYPE_MISMATCH;
}

bool operator==(const Value& a, const Value& b)
{
    if (a.GetType() != b.GetType()) {
        return false;
    }
    switch(a.GetType()) {
    case Value::INTEGER: return a.Int() == b.Int();
    case Value::STRING:
        return ToSlice(a.Str()).QuasiLexicographicOrder(ToSlice(b.Str())) == 0;
    case Value::BOOLEAN: return a.Bool() == b.Bool();
    case Value::DOUBLE: return a.Double() == b.Double();
    default:
        OTS_ASSERT(a.mLogger, false)
            ((int) a.GetType())
            .What("unsupported value type");
    }
    return false;
}

size_t ValueHasher::operator()(const Value& v) const
{
    const size_t kGoldenPrime = 11400087837552502789ull;
    size_t res = v.GetType();
    res *= kGoldenPrime;
    if (v.GetType() == Value::INTEGER) {
        res += v.Int();
        res *= kGoldenPrime;
    } else if (v.GetType() == Value::STRING) {
        const string& s = v.Str();
        res += s.size();
        res *= kGoldenPrime;
        if (s.size() >= 1) {
            res += s[0];
            res *= kGoldenPrime;
        }
        if (s.size() >= 2) {
            res += s[s.size() - 1];
            res *= kGoldenPrime;
        }
        for(int i = 0; i < 5; ++i) {
            size_t idx = res % s.size();
            res += s[idx];
            res *= kGoldenPrime;
        }
    } else if (v.GetType() == Value::BOOLEAN) {
        res += v.Bool() ? 2654289839 : 40499;
        res *= kGoldenPrime;
    } else if (v.GetType() == Value::DOUBLE) {
        double d = v.Double();
        uint8_t* p = (uint8_t*) &d;
        uint8_t* q = p + sizeof(d);
        for(; p < q; ++p) {
            res += *p;
            res *= kGoldenPrime;
        }
    }
    return res;
}

size_t ColumnsHasher::operator()(const deque<Column>& cs) const
{
    const size_t kGoldenPrime = 11400087837552502789ull;
    ValueHasher vhash;
    size_t res = 0;
    FOREACH_ITER(i, cs) {
        res *= kGoldenPrime;
        res += vhash(i->mValue);
    }
    return res;
}

namespace details {

Json::Value Jsonizer<PutRowRequest>::operator()(const PutRowRequest& val) const
{
    Json::Value res = Json::objectValue;
    res["TableName"] = val.mTableName;

    res["Pkey"] = Json::arrayValue;
    FOREACH_ITER(i, val.mPrimaryKey) {
        const Column& col = *i;
        Json::Value c = Json::objectValue;
        c[col.mName] = Jsonize(mLogger, col.mValue);
        res["Pkey"].append(c);
    }

    res["Attrs"] = Json::objectValue;
    FOREACH_ITER(i, val.mAttributes) {
        const Column& col = *i;
        if (ToSlice(col.mName) != ToSlice("_track")) {
            res["Attrs"][col.mName] =
                Jsonize(mLogger, col.mValue);
        }
    }

    return res;
}

Json::Value Jsonizer<DeleteRowRequest>::operator()(const DeleteRowRequest& val) const
{
    Json::Value res = Json::objectValue;
    res["Tracker"] = val.mTracker;
    res["TableName"] = val.mTableName;
    res["PrimaryKey"] = Jsonize(mLogger, val.mPrimaryKey);
    return res;
}

Json::Value Jsonizer<BatchWriteRequest>::operator()(
    const BatchWriteRequest& val) const
{
    Json::Value res = Json::objectValue;
    res["Tracker"] = val.mTracker;
    if (!val.mPutRows.empty()) {
        res["PutRows"] = Json::arrayValue;
        Json::Value& puts = res["PutRows"];
        FOREACH_ITER(i, val.mPutRows) {
            puts.append(Jsonize(mLogger, get<0>(*i)));
        }
    }
    if (!val.mDelRows.empty()) {
        res["DelRows"] = Json::arrayValue;
        Json::Value& dels = res["DelRows"];
        FOREACH_ITER(i, val.mDelRows) {
            dels.append(Jsonize(mLogger, get<0>(*i)));
        }
    }
    return res;
}

Json::Value Jsonizer<BatchWriteResponse>::operator()(
    const BatchWriteResponse& val) const
{
    Json::Value res = Json::objectValue;
    if (!val.mPutRows.empty()) {
        res["PutRows"] = Json::arrayValue;
        Json::Value& puts = res["PutRows"];
        FOREACH_ITER(i, val.mPutRows) {
            puts.append(Jsonize(mLogger, get<0>(*i)));
        }
    }
    if (!val.mDelRows.empty()) {
        res["DelRows"] = Json::arrayValue;
        Json::Value& dels = res["DelRows"];
        FOREACH_ITER(i, val.mDelRows) {
            dels.append(Jsonize(mLogger, get<0>(*i)));
        }
    }
    return res;
}

void Unjsonizer<BatchWriteResponse>::operator()(
    BatchWriteResponse* out,
    const Json::Value& val) const
{
    OTS_ASSERT(mLogger, val.isObject());

    if (val.isMember("PutRows")) {
        const Json::Value& jPuts = val["PutRows"];
        OTS_ASSERT(mLogger, jPuts.isArray());
        for(int i = 0, sz = jPuts.size(); i < sz; ++i) {
            const Json::Value& jRow = jPuts[i];
            Exceptional ex;
            Unjsonize(mLogger, &ex, jRow);
            out->mPutRows.push_back(tuple<Exceptional, const void*>(ex, NULL));
        }
    }

    if (val.isMember("DelRows")) {
        const Json::Value& jDels = val["DelRows"];
        OTS_ASSERT(mLogger, jDels.isArray());
        for(int i = 0, sz = jDels.size(); i < sz; ++i) {
            const Json::Value& jRow = jDels[i];
            Exceptional ex;
            Unjsonize(mLogger, &ex, jRow);
            out->mDelRows.push_back(tuple<Exceptional, const void*>(ex, NULL));
        }
    }
}

Json::Value Jsonizer<GetRangeRequest>::operator()(const GetRangeRequest& val) const
{
    Json::Value res = Json::objectValue;
    res["TableName"] = val.mTableName;
    res["Limit"] = val.mLimit;
    if (!val.mWantedColumns.empty()) {
        Json::Value wc = Json::arrayValue;
        FOREACH_ITER(i, val.mWantedColumns) {
            wc.append(Json::Value(*i));
        }
        res["Wanted"] = wc;
    }
    {
        Json::Value pkey = Json::arrayValue;
        FOREACH_ITER(i, val.mStartPkey) {
            const Column& c = *i;
            pkey.append(Jsonize(mLogger, c));
        }
        res["Start"] = pkey;
    }
    {
        Json::Value pkey = Json::arrayValue;
        FOREACH_ITER(i, val.mStopPkey) {
            const Column& c = *i;
            pkey.append(Jsonize(mLogger, c));
        }
        res["Stop"] = pkey;
    }
    return res;
}

Json::Value Jsonizer<GetRangeResponse>::operator()(
    const GetRangeResponse& val) const
{
    Json::Value res = Json::objectValue;

    Json::Value& jRows = res["Rows"];
    jRows = Json::arrayValue;
    FOREACH_ITER(i, val.mRows) {
        jRows.append(Jsonize(mLogger, **i));
    }

    if (!val.mNextPkey.empty()) {
        res["Next"] = Jsonize(mLogger, val.mNextPkey);
    }

    return res;
}

void Unjsonizer<GetRangeResponse>::operator()(
    GetRangeResponse* out,
    const Json::Value& val) const
{
    OTS_ASSERT(mLogger, val.isObject());
    const Json::Value& jRows = val["Rows"];
    OTS_ASSERT(mLogger, jRows.isArray());
    for(int i = 0, sz = jRows.size(); i < sz; ++i) {
        const Json::Value& jRow = jRows[i];
        OTS_ASSERT(mLogger, jRow.isObject());
        out->mRows.push_back(shared_ptr<Row>(new Row()));

        const Json::Value& jPkey = jRow["PrimaryKeys"];
        Unjsonize(mLogger, &(out->mRows.back()->mPrimaryKey), jPkey);

        const Json::Value& jAttr = jRow["Attributes"];
        if (!jAttr.isNull()) {
            Unjsonize(mLogger, &(out->mRows.back()->mAttributes), jAttr);
        }
    }

    const Json::Value& jNext = val["Next"];
    OTS_ASSERT(mLogger, jNext.isNull() || jNext.isArray());
    if (jNext.isArray()) {
        Unjsonize(mLogger, &(out->mNextPkey), jNext);
    }
}

Json::Value Jsonizer<BatchGetRowRequest>::operator()(const BatchGetRowRequest& val) const
{
    Json::Value res = Json::objectValue;
    res["Tracker"] = val.mTracker;
    res["GetRows"] = Json::arrayValue;
    FOREACH_ITER(i, val.mGetRows) {
        Json::Value jRow = Jsonize(mLogger, get<0>(*i));
        res["GetRows"].append(jRow);
    }
    return res;
}

Json::Value Jsonizer<BatchGetRowResponse>::operator()(const BatchGetRowResponse& val) const
{
    Json::Value res = Json::objectValue;
    res["Tracker"] = val.mTracker;
    res["GetRows"] = Json::arrayValue;
    FOREACH_ITER(i, val.mGetRows) {
        const Exceptional& ex = get<0>(*i);
        const GetRowResponse& resp = get<1>(*i);
        Json::Value jRow = Json::objectValue;
        if (ex.GetCode() == Exceptional::OTS_OK) {
            jRow = Jsonize(mLogger, resp);
        } else {
            jRow = Jsonize(mLogger, ex);
        }
        res["GetRows"].append(jRow);
    }
    return res;
}

void Unjsonizer<BatchGetRowResponse>::operator()(
    BatchGetRowResponse* out,
    const Json::Value& val) const
{
    OTS_ASSERT(mLogger, val.isObject())(ToString(val));
    const Json::Value& jRows = val["Rows"];
    OTS_ASSERT(mLogger, jRows.isArray())(ToString(val));
    for(int i = 0, sz = jRows.size(); i < sz; ++i) {
        const Json::Value& jRow = jRows[i];
        OTS_ASSERT(mLogger, jRow.isObject());

        tuple<Exceptional, GetRowResponse, const void*> row(
            Exceptional(), GetRowResponse(), NULL);

        Unjsonize(mLogger, &(get<0>(row)), jRow);
        if (get<0>(row).GetCode() == Exceptional::OTS_OK) {
            Unjsonize(mLogger, &(get<1>(row)), jRow);
        }
        
        out->mGetRows.push_back(row);
    }
}

Json::Value Jsonizer<GetRowRequest>::operator()(const GetRowRequest& val) const
{
    Json::Value res = Json::objectValue;
    res["TableName"] = val.mTableName;
    res["Tracker"] = val.mTracker;
    res["PrimaryKey"] = Jsonize(mLogger, val.mPrimaryKey);
    return res;
}

Json::Value Jsonizer<GetRowResponse>::operator()(const GetRowResponse& val) const
{
    Json::Value res = Jsonize(mLogger, *val.mRow);
    res["Tracker"] = val.mTracker;
    return res;
}

void Unjsonizer<GetRowResponse>::operator()(
    GetRowResponse* out,
    const Json::Value& val) const
{
    OTS_ASSERT(mLogger, val.isObject())(ToString(val));
    Row row;
    Unjsonize(mLogger, &row, val);
    out->mRow.reset(new Row(row));

    const Json::Value& jTracker = val["Tracker"];
    if (!jTracker.isNull()) {
        OTS_ASSERT(mLogger, jTracker.isString())(ToString(val));
        out->mTracker = jTracker.asString();
    }
}

Json::Value Jsonizer<Row>::operator()(const Row& val) const
{
    Json::Value res = Json::objectValue;
    res["PrimaryKeys"] = Jsonize(mLogger, val.mPrimaryKey);
    if (!val.mAttributes.empty()) {
        res["Attributes"] = Jsonize(mLogger, val.mAttributes);
    }
    return res;
}

void Unjsonizer<Row>::operator()(
    Row* out,
    const Json::Value& val) const
{
    OTS_ASSERT(mLogger, val.isObject())(ToString(val));

    const Json::Value& jPkey = val["PrimaryKeys"];
    OTS_ASSERT(mLogger, jPkey.isArray())(ToString(val));
    Unjsonize(mLogger, &(out->mPrimaryKey), jPkey);

    const Json::Value& jAttrs = val["Attributes"];
    if (!jAttrs.isNull()) {
        OTS_ASSERT(mLogger, jAttrs.isArray())(ToString(val));
        Unjsonize(mLogger, &(out->mAttributes), jAttrs);
    }
}

Json::Value Jsonizer< deque<Column> >::operator()(
    const deque<Column>& val) const
{
    Json::Value res = Json::arrayValue;
    FOREACH_ITER(i, val) {
        res.append(Jsonize(mLogger, *i));
    }
    return res;
}

void Unjsonizer< deque<Column> >::operator()(
    deque<Column>* out,
    const Json::Value& val) const
{
    OTS_ASSERT(mLogger, val.isArray())(val.toStyledString());
    for(int i = 0, sz = val.size(); i < sz; ++i) {
        const Json::Value& jAttr = val[i];
        OTS_ASSERT(mLogger, jAttr.isObject());
        out->push_back(Column(mLogger));

        Unjsonize(mLogger, &(out->back()), jAttr);
    }
}

Json::Value Jsonizer<Column>::operator()(const Column& val) const
{
    Json::Value jCol = Json::objectValue;
    jCol["Name"] = val.mName;
    jCol["Value"] = Jsonize(mLogger, val.mValue);
    return jCol;
}

void Unjsonizer<Column>::operator()(
    Column* out,
    const Json::Value& val) const
{
    OTS_ASSERT(mLogger, val.isObject())(val.toStyledString());

    const Json::Value& jName = val["Name"];
    OTS_ASSERT(mLogger, jName.isString());
    out->mName = jName.asString();

    const Json::Value& jVal = val["Value"];
    Unjsonize(mLogger, &(out->mValue), jVal);
}

Json::Value Jsonizer<Value>::operator()(const Value& val) const
{
    switch(val.GetType()) {
    case Value::INF_MIN: return Json::Value("-inf");
    case Value::INF_MAX: return Json::Value("+inf");
    case Value::INTEGER: return Json::Value((Json::Int64) val.Int());
    case Value::STRING: return Json::Value(val.Str());
    case Value::BOOLEAN: return Json::Value(val.Bool());
    case Value::DOUBLE: return Json::Value(val.Double());
    default:
        OTS_ASSERT(mLogger, false).What("unknown value");
    }
    return Json::nullValue;
}

void Unjsonizer<Value>::operator()(
    Value* out,
    const Json::Value& val) const
{
    if (val.isInt()) {
        out->SetInt(val.asInt64());
    } else if (val.isBool()) {
        out->SetBool(val.asBool());
    } else if (val.isDouble()) {
        out->SetDouble(val.asDouble());
    } else if (val.isString()) {
        out->SetStr(val.asString());
    } else {
        OTS_ASSERT(mLogger, false)(ToString(val));
    }
}

Json::Value Jsonizer<Exceptional>::operator()(const Exceptional& val) const
{
    Json::Value res = Json::objectValue;
    if (val.GetCode() != Exceptional::OTS_OK) {
        res["ErrorMessage"] = val.GetErrorMessage();
        res["ErrorCode"] = val.GetErrorCode();
        if (!val.GetRequestId().empty()) {
            res["RequestId"] = val.GetRequestId();
        }
    }
    return res;
}

void Unjsonizer<Exceptional>::operator()(
    Exceptional* out,
    const Json::Value& val) const
{
    OTS_ASSERT(mLogger, val.isObject())(ToString(val));
    if (val.isMember("ErrorCode")) {
        string ec = val["ErrorCode"].asString();
        string msg = val["ErrorMessage"].asString();
        if (val.isMember("RequestId")) {
            *out = Exceptional(
                Exceptional::OTS_SERVICE_EXCEPTION,
                ec,
                msg,
                val["RequestId"].asString());
        } else {
            *out = Exceptional(
                Exceptional::OTS_CLIENT_EXCEPTION,
                ec,
                msg);
        }
    } else {
        *out = Exceptional();
    }
}

} // namespace details

} // namespace static_index

