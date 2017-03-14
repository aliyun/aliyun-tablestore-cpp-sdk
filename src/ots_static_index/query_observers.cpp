#include "query_observers.h"
#include "def_ast.h"
#include "foreach.h"

using namespace ::std;
using namespace ::std::tr1;

#define OBS_TRY(err) \
    if (err.GetCode() != ::static_index::Exceptional::OTS_OK) { \
        NotifyError(err);                                       \
        return;                                                 \
    }

namespace static_index {

Latcher::Latcher(Logger* logger)
  : RowObservableObserver(logger),
    mLogger(logger),
    mMutex(logger)
{}
void Latcher::OnInnerNext(const shared_ptr<AssociatedRow>& row)
{
    Scoped<Mutex> g(&mMutex);
    NotifyNext(row);
}
void Latcher::OnInnerCompletion()
{
    Scoped<Mutex> g(&mMutex);
    NotifyCompletion();
}
void Latcher::OnInnerError(const Exceptional& err)
{
    Scoped<Mutex> g(&mMutex);
    NotifyError(err);
}


ExactMatcher::ExactMatcher(
    Logger* logger,
    const string& name,
    const Value& value)
  : RowObservableObserver(logger),
    mLogger(logger),
    mName(name),
    mValue(value)
{
}
void ExactMatcher::OnInnerNext(const shared_ptr<AssociatedRow>& row)
{
    FOREACH_ITER(i, *row) {
        const string& name = i->first;
        const Value& val = i->second;
        if (ToSlice(name) == ToSlice(mName) && val == mValue) {
            NotifyNext(row);
        }
    }
}
void ExactMatcher::OnInnerCompletion()
{
    NotifyCompletion();
}
void ExactMatcher::OnInnerError(const Exceptional& err)
{
    NotifyError(err);
}


RangeMatcher::RangeMatcher(
    Logger* logger,
    const string& name,
    const Value& value,
    const string& op)
  : RowObservableObserver(logger),
    mLogger(logger),
    mName(name),
    mValue(value),
    mRelation(MongoOpToRelation(op))
{}
void RangeMatcher::OnInnerNext(const shared_ptr<AssociatedRow>& row)
{
    FOREACH_ITER(i, *row) {
        const string& name = i->first;
        const Value& val = i->second;
        if (ToSlice(name) == ToSlice(mName)) {
            Value::ComparisonResult res = val.Compare(mValue);
            if (res & mRelation) {
                NotifyNext(row);
            }
        }
    }
}
void RangeMatcher::OnInnerCompletion()
{
    NotifyCompletion();
}
void RangeMatcher::OnInnerError(const Exceptional& err)
{
    NotifyError(err);
}
uint64_t RangeMatcher::MongoOpToRelation(const string& op)
{
    if (ToSlice(op) == ToSlice("$lt")) {
        return Value::SMALLER;
    } else if (ToSlice(op) == ToSlice("$gt")) {
        return Value::LARGER;
    } else if (ToSlice(op) == ToSlice("$gte")) {
        return Value::LARGER | Value::EQUIV;
    } else if (ToSlice(op) == ToSlice("$lte")) {
        return Value::SMALLER | Value::EQUIV;
    } else if (ToSlice(op) == ToSlice("$ne")) {
        return Value::SMALLER | Value::LARGER;
    } else {
        OTS_ASSERT(mLogger, false)(op);
    }
    return 0;
}


InsideMatcher::InsideMatcher(
    Logger* logger,
    const string& name,
    const Json::Value& values)
  : RowObservableObserver(logger),
    mLogger(logger),
    mName(name)
{
    OTS_ASSERT(mLogger, values.isArray());
    for(int i = 0, sz = values.size(); i < sz; ++i) {
        const Json::Value& jVal = values[i];
        static_index::Value val(mLogger);
        Unjsonize(mLogger, &val, jVal);
        mValues.insert(val);
    }
}
void InsideMatcher::OnInnerNext(const shared_ptr<AssociatedRow>& row)
{
    FOREACH_ITER(i, *row) {
        const string& name = i->first;
        const Value& val = i->second;
        if (ToSlice(name) == ToSlice(mName)) {
            if (Hit(val)) {
                NotifyNext(row);
            }
        }
    }
}
void InsideMatcher::OnInnerCompletion()
{
    NotifyCompletion();
}
void InsideMatcher::OnInnerError(const Exceptional& err)
{
    NotifyError(err);
}
bool InsideMatcher::Hit(const Value& v) const
{
    return mValues.find(v) != mValues.end();
}


Sorter::Sorter(
    Logger* logger,
    const string& field,
    int order)
  : RowObservableObserver(logger),
    mLogger(logger),
    mOrderField(field),
    mOrder(order),
    mSorted(ValueComparison(order))
{
    OTS_ASSERT(mLogger, order == 1 || order == -1);
}
void Sorter::OnInnerNext(const shared_ptr<AssociatedRow>& row)
{
    Value v = SearchField(*row);
    if (v.GetType() == Value::INVALID) {
        mUnrelated.push_back(row);
    } else {
        mSorted.insert(make_pair(v, row));
    }
}
void Sorter::OnInnerCompletion()
{
    FOREACH_ITER(i, mSorted) {
        NotifyNext(i->second);
    }
    FOREACH_ITER(i, mUnrelated) {
        NotifyNext(*i);
    }
    NotifyCompletion();
}

void Sorter::OnInnerError(const Exceptional& err)
{
    NotifyError(err);
}
Value Sorter::SearchField(const AssociatedRow& row)
{
    AssociatedRow::const_iterator it = row.find(mOrderField);
    if (it != row.end()) {
        return it->second;
    } else {
        return Value(mLogger);
    }
}
bool Sorter::ValueComparison::operator()(const Value& a, const Value& b) const
{
    int natural = CompareValueInMongoOrder(a, b);
    return mOrder * natural < 0;
}
int Sorter::ValueComparison::CompareValueInMongoOrder(const Value& a, const Value& b) const
{
    Value::ComparisonResult res = a.Compare(b);
    if (res == Value::TYPE_MISMATCH) {
        Value::Type ta = a.GetType();
        Value::Type tb = b.GetType();
        if (ta == Value::BOOLEAN) {
            return 1;
        }
        if (tb == Value::BOOLEAN) {
            return -1;
        }
        if (ta == Value::STRING) {
            return 1;
        }
        if (tb == Value::STRING) {
            return -1;
        }
        double da = ta == Value::INTEGER
            ? static_cast<double>(a.Int())
            : a.Double();
        double db = tb == Value::INTEGER
            ? static_cast<double>(b.Int())
            : b.Double();
        if (da < db) {
            return -1;
        } else if (da > db) {
            return 1;
        } else {
            return 0;
        }
    } else if (res == Value::SMALLER) {
        return -1;
    } else if (res == Value::LARGER) {
        return 1;
    } else {
        return 0;
    }
}


SliceWindow::SliceWindow(
    Logger* logger,
    bool* quickQuit,
    int64_t start,
    int64_t limit)
  : RowObservableObserver(logger),
    mLogger(logger),
    mStart(start),
    mLimit(limit == 0 ? numeric_limits<int64_t>::max() : limit),
    mQuickQuit(quickQuit)
{}
void SliceWindow::OnInnerNext(const shared_ptr<AssociatedRow>& row)
{
    if (mStart > 0) {
        --mStart;
    } else if (mLimit > 0) {
        --mLimit;
        NotifyNext(row);
        if (mLimit == 0) {
            __atomic_store_1(mQuickQuit, true, __ATOMIC_RELEASE);
        }
    }
}
void SliceWindow::OnInnerCompletion()
{
    NotifyCompletion();
}
void SliceWindow::OnInnerError(const Exceptional& err)
{
    NotifyError(err);
}


RowToJsonVector::RowToJsonVector(
    Logger* logger,
    vector<Json::Value>* out,
    const TableMeta& schema,
    const Json::Value& projection)
  : RowObserver(logger),
    mLogger(logger),
    mOut(out),
    mSem(new Semaphore(logger, 0))
{
    FOREACH_ITER(i, schema.mPrimaryKey) {
        const shared_ptr<ast::Node>& root = *i;
        if (root->mNodeType != ast::Node::ATTR) {
            mUnwanted.insert(ToSlice(root->mName));
        }
    }

    OTS_ASSERT(mLogger, projection.isArray());
    for(int i = 0, sz = projection.size(); i < sz; ++i) {
        const Json::Value& name = projection[i];
        OTS_ASSERT(mLogger, name.isString())(ToString(projection))(i);
        mWanted.insert(name.asString());
    }
}
Exceptional RowToJsonVector::Wait()
{
    mSem->Wait();
    return mExcept;
}
void RowToJsonVector::OnInnerNext(const shared_ptr<AssociatedRow>& row)
{
    Json::Value jVal = Json::objectValue;
    FOREACH_ITER(i, *row) {
        OTS_LOG_DEBUG(mLogger)
            (i->first);
        if (IsWanted(i->first)) {
            const Json::Value& r = Jsonize(mLogger, i->second);
            OTS_LOG_DEBUG(mLogger)
                (i->first)
                (ToString(r));
            jVal[i->first] = r;
        }
    }
        
    mOut->push_back(jVal);
}
void RowToJsonVector::OnInnerCompletion()
{
    OTS_LOG_INFO(mLogger)
        .What("RowToJsonVector completion");
    mSem->Post();
}
void RowToJsonVector::OnInnerError(const Exceptional& err)
{
    OTS_LOG_DEBUG(mLogger).What("RowToJsonVector error");
    mExcept = err;
    mSem->Post();
}
bool RowToJsonVector::IsWanted(const string& name) const
{
    return !mUnwanted.count(ToSlice(name))
        && (mWanted.empty() || mWanted.count(name));
}

} // namespace static_index

#undef OBS_TRY
