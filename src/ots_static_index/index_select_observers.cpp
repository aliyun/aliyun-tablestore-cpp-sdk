#include "index_select_observers.h"
#include "def_ast.h"

using namespace ::std;
using namespace ::std::tr1;

#define OBS_TRY(err) \
    if (err.GetCode() != ::static_index::Exceptional::OTS_OK) { \
        NotifyError(err);                                       \
        return;                                                 \
    }

namespace static_index {

namespace {

Hit::Type MorePrecise(Hit::Type a, Hit::Type b)
{
    if (a == Hit::POINT || b == Hit::POINT) {
        return Hit::POINT;
    }
    if (a == Hit::RANGE || b == Hit::RANGE) {
        return Hit::RANGE;
    }
    return Hit::NONE;
}

} // namespace

AttrHitter::AttrHitter(
    Logger* logger,
    const string& name)
  : HitObservableObserver(logger),
    mLogger(logger),
    mHit(name, Hit::NONE)
{}
void AttrHitter::OnInnerNext(const Hit& v)
{
    if (ToSlice(mHit.mName) == ToSlice(v.mName)) {
        mHit.mType = MorePrecise(mHit.mType, v.mType);
    }
}
void AttrHitter::OnInnerCompletion()
{
    if (mHit.mType != Hit::NONE) {
        NotifyNext(mHit);
    }
    NotifyCompletion();
}
void AttrHitter::OnInnerError(const Exceptional& err)
{
    NotifyError(err);
}

OrderingHitter::OrderingHitter(
    Logger* logger,
    const string& name)
  : HitObservableObserver(logger),
    mLogger(logger),
    mHit(name, Hit::NONE)
{}
void OrderingHitter::OnInnerNext(const Hit& v)
{
    mHit.mType = MorePrecise(mHit.mType, v.mType);
}
void OrderingHitter::OnInnerCompletion()
{
    if (mHit.mType != Hit::NONE) {
        NotifyNext(mHit);
    }
    NotifyCompletion();
}
void OrderingHitter::OnInnerError(const Exceptional& err)
{
    NotifyError(err);
}

HashedHitter::HashedHitter(
    Logger* logger,
    const string& name)
  : HitObservableObserver(logger),
    mLogger(logger),
    mHit(name, Hit::NONE)
{}
void HashedHitter::OnInnerNext(const Hit& v)
{
    if (v.mType == Hit::POINT) {
        mHit.mType = Hit::POINT;
    }
}
void HashedHitter::OnInnerCompletion()
{
    if (mHit.mType != Hit::NONE) {
        NotifyNext(mHit);
    }
    NotifyCompletion();
}
void HashedHitter::OnInnerError(const Exceptional& err)
{
    NotifyError(err);
}

ConcatHitter::ConcatHitter(
    Logger* logger,
    const ast::Node& root)
  : HitObservableObserver(logger),
    mLogger(logger),
    mName(root.mName)
{
    FOREACH_ITER(i, root.mChildren) {
        mSubs.push_back((*i)->mName);
    }
}
void ConcatHitter::OnInnerNext(const Hit& v)
{
    QloMap<string, Hit::Type>::iterator it = mHits.find(v.mName);
    if (it == mHits.end()) {
        bool r = mHits.insert(make_pair(v.mName, v.mType)).second;
        OTS_ASSERT(mLogger, r);
    } else {
        it->second = MorePrecise(it->second, v.mType);
    }
}
void ConcatHitter::OnInnerCompletion()
{
    if (AllPointed()) {
        Hit hit(mName, Hit::POINT);
        NotifyNext(hit);
    } else if (NothingNone()) {
        Hit hit(mName, Hit::RANGE);
        NotifyNext(hit);
    }
    NotifyCompletion();
}
void ConcatHitter::OnInnerError(const Exceptional& err)
{
    NotifyError(err);
}
bool ConcatHitter::AllPointed() const
{
    FOREACH_ITER(i, mSubs) {
        const string& name = *i;
        QloMap<string, Hit::Type>::const_iterator it = mHits.find(name);
        if (it == mHits.end()) {
            return false;
        }
        if (it->second != Hit::POINT) {
            return false;
        }
    }
    return true;
}
bool ConcatHitter::NothingNone() const
{
    FOREACH_ITER(i, mSubs) {
        const string& name = *i;
        QloMap<string, Hit::Type>::const_iterator it = mHits.find(name);
        if (it == mHits.end()) {
            return false;
        }
        if (it->second == Hit::NONE) {
            return false;
        }
    }
    return true;
}


HitAggregator::HitAggregator(
    Logger* logger,
    const TableMeta& tmeta)
  : HitObserver(logger),
    mLogger(logger)
{
    mTableName = tmeta.mTableName;
    FOREACH_ITER(i, tmeta.mPrimaryKey) {
        mPkey.push_back((*i)->mName);
    }
}
const Exceptional& HitAggregator::Except() const
{
    return mExcept;
}
tuple<int64_t, int64_t> HitAggregator::GetRatio() const
{
    int64_t cnt = 0;
    FOREACH_ITER(i, mPkey) {
        QloMap<string, Hit::Type>::const_iterator it = mHits.find(*i);
        if (it == mHits.end()) {
            return tuple<int64_t, int64_t>(cnt, mPkey.size());
        }
        ++cnt;
        if (it->second == Hit::RANGE) {
            return tuple<int64_t, int64_t>(cnt, mPkey.size());
        }
    }
    return tuple<int64_t, int64_t>(cnt, mPkey.size());
}
::std::string HitAggregator::GetTableName() const
{
    return mTableName;
}
void HitAggregator::OnInnerNext(const Hit& v)
{
    QloMap<string, Hit::Type>::iterator it = mHits.find(v.mName);
    if (it == mHits.end()) {
        bool r = mHits.insert(make_pair(v.mName, v.mType)).second;
        OTS_ASSERT(mLogger, r);
    } else {
        if (it->second == Hit::NONE) {
            it->second = v.mType;
        } else if (it->second == Hit::RANGE && v.mType == Hit::POINT) {
            it->second = Hit::POINT;
        }
    }
}
void HitAggregator::OnInnerCompletion()
{}
void HitAggregator::OnInnerError(const Exceptional& err)
{
    mExcept = err;
}

void ConditionHitter::Go(const QloMap< ::std::string, Json::Value >& condition)
{
    FOREACH_ITER(i, condition) {
        Hit hit;
        hit.mName = i->first;
        const Json::Value& v = i->second;
        if (!v.isObject()) {
            hit.mType = Hit::POINT;
            NotifyNext(hit);
        } else {
            const vector<string>& ops = v.getMemberNames();
            FOREACH_ITER(j, ops) {
                const string& op = *j;
                if (ToSlice(op) == ToSlice("$eq") || ToSlice(op) == ToSlice("$in")) {
                    hit.mType = Hit::POINT;
                    NotifyNext(hit);
                } else if (ToSlice(op) == ToSlice("$lt")
                    || ToSlice(op) == ToSlice("$lte")
                    || ToSlice(op) == ToSlice("$gt")
                    || ToSlice(op) == ToSlice("$gte"))
                {
                    hit.mType = Hit::RANGE;
                    NotifyNext(hit);
                } else {
                    OTS_LOG_INFO(mLogger)
                        (op)
                        (ToString(v))
                        .What("unknown operator. just ignore.");
                }
            }
        }
    }
    NotifyCompletion();
}

} // namespace static_index

#undef OBS_TRY
