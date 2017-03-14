#include "condition_observers.h"
#include "def_ast.h"
#include "security.h"
#include "arithmetic.h"

using namespace ::std;
using namespace ::std::tr1;

#define OBS_TRY(err) \
    if (err.GetCode() != ::static_index::Exceptional::OTS_OK) { \
        NotifyError(err);                                       \
        return;                                                 \
    }

namespace static_index {

Boundary::Boundary(const string& name, const Value& lower, const Value& upper)
  : mName(name),
    mLower(lower),
    mUpper(upper)
{}


PickAttrLimiter::PickAttrLimiter(
    Logger* logger,
    const string& name)
  : LimitObservableObserver(logger),
    mLogger(logger),
    mName(name)
{}
void PickAttrLimiter::OnInnerNext(const Boundary& v)
{
    if (ToSlice(v.mName) == ToSlice(mName)) {
        NotifyNext(v);
    }
}
void PickAttrLimiter::OnInnerCompletion()
{
    NotifyCompletion();
}
void PickAttrLimiter::OnInnerError(const Exceptional& err)
{
    NotifyError(err);
}


HexLimiter::HexLimiter(
    Logger* logger,
    const string& name)
  : LimitObservableObserver(logger),
    mLogger(logger),
    mName(name),
    mLower(0),
    mUpper(numeric_limits<uint64_t>::max())
{}
void HexLimiter::OnInnerNext(const Boundary& v)
{
    const Value& vmin = v.mLower;
    const Value& vmax = v.mUpper;
    uint64_t umin = 0;
    uint64_t umax = numeric_limits<uint64_t>::max();
    if (vmin.IsInfMin()) {
        // do nothing
    } else {
        umin = vmin.Int();
    }
    if (vmax.IsInfMax()) {
        // do nothing
    } else {
        umax = vmax.Int();
    }
    mLower = std::max(mLower, umin);
    mUpper = std::min(mUpper, umax);
}
void HexLimiter::OnInnerCompletion()
{
    Value lower(mLogger, Hex(mLower));
    Value upper(mLogger, Hex(mUpper));
    NotifyNext(Boundary(mName, lower, upper));
    NotifyCompletion();
}
void HexLimiter::OnInnerError(const Exceptional& err)
{
    NotifyError(err);
}


ShiftToUint64Limiter::ShiftToUint64Limiter(
    Logger* logger,
    const string& name)
  : LimitObservableObserver(logger),
    mLogger(logger),
    mName(name),
    mLower(numeric_limits<int64_t>::min()),
    mUpper(numeric_limits<int64_t>::max())
{}
void ShiftToUint64Limiter::OnInnerNext(const Boundary& v)
{
    const Value& vmin = v.mLower;
    const Value& vmax = v.mUpper;
    int64_t umin = numeric_limits<int64_t>::min();
    int64_t umax = numeric_limits<int64_t>::max();
    if (vmin.IsInfMin()) {
        // do nothing
    } else {
        umin = vmin.Int();
    }
    if (vmax.IsInfMax()) {
        // do nothing
    } else {
        umax = vmax.Int();
    }
    mLower = std::max(mLower, umin);
    mUpper = std::min(mUpper, umax);
}
void ShiftToUint64Limiter::OnInnerCompletion()
{
    Value lower(mLogger, (int64_t) ShiftToUint64(mLower));
    Value upper(mLogger, (int64_t) ShiftToUint64(mUpper));
    NotifyNext(Boundary(mName, lower, upper));
    NotifyCompletion();
}
void ShiftToUint64Limiter::OnInnerError(const Exceptional& err)
{
    NotifyError(err);
}


Crc64Limiter::Crc64Limiter(
    Logger* logger,
    const string& name)
  : LimitObservableObserver(logger),
    mLogger(logger),
    mName(name)
{
}
void Crc64Limiter::OnInnerNext(const Boundary& v)
{
}
void Crc64Limiter::OnInnerCompletion()
{
    Value lower(mLogger, numeric_limits<int64_t>::min());
    Value upper(mLogger, numeric_limits<int64_t>::max());
    NotifyNext(Boundary(mName, lower, upper));
    NotifyCompletion();
}
void Crc64Limiter::OnInnerError(const Exceptional& err)
{
    NotifyError(err);
}


namespace {

Exceptional Max(Value* inout, const Value& cmp)
{
    Value::ComparisonResult res = inout->Compare(cmp);
    if (res == Value::SMALLER) {
        *inout = cmp;
    } else if (res == Value::TYPE_MISMATCH) {
        return Exceptional("type mismatch in comparison");
    }
    return Exceptional();
}

Exceptional Min(Value* inout, const Value& cmp)
{
    Value::ComparisonResult res = inout->Compare(cmp);
    if (res == Value::LARGER) {
        *inout = cmp;
    } else if (res == Value::TYPE_MISMATCH) {
        return Exceptional("type mismatch in comparison");
    }
    return Exceptional();
}

} // namespace

ConcatLimiter::ConcatLimiter(
    Logger* logger,
    const ast::Node& node)
  : LimitObservableObserver(logger),
    mLogger(logger),
    mName(node.mName)
{
    Value min(mLogger, Value::InfMin());
    Value max(mLogger, Value::InfMax());
    int64_t loc = 0;
    FOREACH_ITER(i, node.mChildren) {
        const ast::Node& n = **i;
        mBoundaries.push_back(make_tuple(min, max));
        bool r = mLocations.insert(make_pair(n.mName, loc)).second;
        OTS_ASSERT(mLogger, r)(n.mName);
        ++loc;
    }
}
void ConcatLimiter::OnInnerNext(const Boundary& v)
{
    const string& name = v.mName;
    const Value& lower = v.mLower;
    const Value& upper = v.mUpper;
    QloMap<string, int64_t>::const_iterator it = mLocations.find(name);
    OTS_ASSERT(mLogger, it != mLocations.end())(name);
    tuple<Value, Value>& vv = mBoundaries.at(it->second);
    {
        const Exceptional& ex = Max(&(get<0>(vv)), lower);
        if (ex.GetCode() != Exceptional::OTS_OK) {
            OTS_LOG_ERROR(mLogger)
                (name)
                (ToString(Jsonize(mLogger, get<0>(vv))))
                (ToString(Jsonize(mLogger, lower)))
                .What("type mismatch");
            OBS_TRY(ex);
        }
    }
    {
        const Exceptional& ex = Min(&(get<1>(vv)), upper);
        if (ex.GetCode() != Exceptional::OTS_OK) {
            OTS_LOG_ERROR(mLogger)
                (name)
                (ToString(Jsonize(mLogger, get<1>(vv))))
                (ToString(Jsonize(mLogger, upper)))
                .What("type mismatch");
            OBS_TRY(ex);
        }
    }
}
void ConcatLimiter::OnInnerCompletion()
{
    const Value& lower = Lower();
    const Value& upper = Upper();
    NotifyNext(Boundary(mName, lower, upper));
    NotifyCompletion();
}
void ConcatLimiter::OnInnerError(const Exceptional& err)
{
    NotifyError(err);
}
Value ConcatLimiter::Lower() const
{
    string res;
    FOREACH_ITER(i, mBoundaries) {
        const Value& v = get<0>(*i);
        if (v.IsInfMin()) {
            break;
        }
        if (i != mBoundaries.begin()) {
            res.push_back('|');
        }
        OTS_ASSERT(mLogger, v.GetType() == Value::STRING)((int) v.GetType());
        res.append(v.Str());
    }
    return Value(mLogger, res);
}
Value ConcatLimiter::Upper() const
{
    string res;
    FOREACH_ITER(i, mBoundaries) {
        const Value& v = get<1>(*i);
        if (v.IsInfMax()) {
            return v;
        }
        if (i != mBoundaries.begin()) {
            res.push_back('|');
        }
        OTS_ASSERT(mLogger, v.GetType() == Value::STRING)((int) v.GetType());
        res.append(v.Str());
    }
    return Value(mLogger, res);
}


LimitAggregator::LimitAggregator(
    Logger* logger)
  : LimitObserver(logger),
    mLogger(logger)
{
}
void LimitAggregator::OnInnerNext(const Boundary& v)
{
    QloMap<string, Boundary>::iterator it = mBoundaries.find(v.mName);
    if (it == mBoundaries.end()) {
        bool r = mBoundaries.insert(make_pair(v.mName, v)).second;
        OTS_ASSERT(mLogger, r);
    } else {
        Max(&(it->second.mLower), v.mLower);
        Min(&(it->second.mUpper), v.mUpper);
    }
}
void LimitAggregator::OnInnerCompletion()
{
}
void LimitAggregator::OnInnerError(const Exceptional& err)
{
    mExcept = err;
}


void ConditionIssuer::Go(const QloMap<string, Json::Value>& condition)
{
    FOREACH_ITER(i, condition) {
        const string& name = i->first;
        const Json::Value& jCond = i->second;
        if (!jCond.isObject()) {
            Value v(mLogger);
            Unjsonize(mLogger, &v, jCond);
            NotifyNext(Boundary(name, v, v));
        } else {
            Value lower(mLogger, Value::InfMin());
            Value upper(mLogger, Value::InfMax());
            const vector<string>& ops = jCond.getMemberNames();
            FOREACH_ITER(i, ops) {
                const string& op = *i;
                if (ToSlice(op) == ToSlice("$eq")) {
                    Value v(mLogger);
                    Unjsonize(mLogger, &v, jCond[op]);
                    OBS_TRY(Max(&lower, v));
                    OBS_TRY(Min(&upper, v));
                } else if (ToSlice(op) == ToSlice("$gt") || ToSlice(op) == ToSlice("$gte")) {
                    Value v(mLogger);
                    Unjsonize(mLogger, &v, jCond[op]);
                    OBS_TRY(Max(&lower, v));
                } else if (ToSlice(op) == ToSlice("$lt") || ToSlice(op) == ToSlice("$lte")) {
                    Value v(mLogger);
                    Unjsonize(mLogger, &v, jCond[op]);
                    OBS_TRY(Min(&upper, v));
                } else {
                    OTS_LOG_DEBUG(mLogger)
                        (name)
                        (op)
                        .What("unknown operator, just ignore");
                    continue;
                }
            }
            Value::ComparisonResult cr = lower.Compare(upper);
            if (cr == Value::LARGER) {
                OBS_TRY(
                    Exceptional("The lower bound in condition is greater than the upper bound.")) ;
            } else if (cr == Value::TYPE_MISMATCH) {
                OBS_TRY(
                    Exceptional("Different types of values in condition.")) ;
            }
            NotifyNext(Boundary(name, lower, upper));
        }
    }
    NotifyCompletion();
}


OptionAggregator::OptionAggregator(Logger* logger)
  : ColumnObserver(logger)
{}
void OptionAggregator::OnInnerNext(const Column& c)
{
    mOptions[c.mName].insert(c.mValue);
}
void OptionAggregator::OnInnerCompletion()
{
}
void OptionAggregator::OnInnerError(const Exceptional& err)
{
    mExcept = err;
}


OptionExtractor::OptionExtractor(Logger* logger)
  : mLogger(logger)
{}
void OptionExtractor::Go(const QloMap<string, Json::Value>& condition)
{
    FOREACH_ITER(i, condition) {
        const string& name = i->first;
        const Json::Value& jCond = i->second;
        if (!jCond.isObject()) {
            Column c(mLogger);
            c.mName = name;
            Unjsonize(mLogger, &(c.mValue), jCond);
            NotifyNext(c);
        } else {
            const vector<string>& ops = jCond.getMemberNames();
            FOREACH_ITER(i, ops) {
                const string& op = *i;
                if (ToSlice(op) == ToSlice("$eq")) {
                    Column c(mLogger);
                    c.mName = name;
                    Unjsonize(mLogger, &(c.mValue), jCond[op]);
                    NotifyNext(c);
                } else if (ToSlice(op) == ToSlice("$in")) {
                    const Json::Value& jOptions = jCond[op];
                    if (!jOptions.isArray()) {
                        OTS_LOG_ERROR(mLogger)
                            (name)
                            (ToString(jCond));
                        OBS_TRY(Exceptional("Value to $in must be an array")) ;
                    }
                    for(int i = 0, sz = jOptions.size(); i < sz; ++i) {
                        const Json::Value& jVal = jOptions[i];
                        Column c(mLogger);
                        c.mName = name;
                        Unjsonize(mLogger, &(c.mValue), jVal);
                        NotifyNext(c);
                    }
                } else {
                    OTS_LOG_DEBUG(mLogger)
                        (name)
                        (op)
                        .What("unknown operator, just ignore");
                    continue;
                }
            }
        }
    }
    NotifyCompletion();
}

} // namespace static_index

#undef OBS_TRY
