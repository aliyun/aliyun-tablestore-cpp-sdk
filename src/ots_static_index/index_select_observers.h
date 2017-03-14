#ifndef OTS_STATIC_INDEX_INDEX_SELECTION_OBSERVERS_H
#define OTS_STATIC_INDEX_INDEX_SELECTION_OBSERVERS_H

#include "insert_observers.h"
#include "slice.h"
#include "observable.h"
#include "ots_static_index/exceptional.h"
#include <tr1/tuple>
#include <string>
#include <deque>
#include <map>
#include <stdint.h>

namespace Json {
class Value;
} // namespace Json

namespace static_index {
class Column;
class TableMeta;
class Logger;
namespace ast {
class Node;
} // namespace ast

struct Hit
{
    enum Type
    {
        NONE,
        RANGE,
        POINT,
    };

    ::std::string mName;
    Type mType;

    explicit Hit()
      : mType(NONE)
    {}
    explicit Hit(const ::std::string& name, Type t)
      : mName(name), mType(t)
    {}
};

typedef DefaultObserver<Hit, Exceptional> HitObserver;
typedef DefaultObservable<Hit, Exceptional> HitObservable;
class HitObservableObserver: public HitObserver, public HitObservable
{
public:
    explicit HitObservableObserver(Logger* logger)
      : HitObserver(logger)
    {}
};

class AttrHitter: public HitObservableObserver
{
    Logger* mLogger;
    Hit mHit;

public:
    explicit AttrHitter(Logger* logger, const ::std::string&);

private:
    void OnInnerNext(const Hit& v);
    void OnInnerCompletion();
    void OnInnerError(const Exceptional& err);
};

class OrderingHitter: public HitObservableObserver
{
    Logger* mLogger;
    Hit mHit;

public:
    explicit OrderingHitter(Logger* logger, const ::std::string& name);

private:
    void OnInnerNext(const Hit& v);
    void OnInnerCompletion();
    void OnInnerError(const Exceptional& err);
};

class HashedHitter: public HitObservableObserver
{
    Logger* mLogger;
    Hit mHit;

public:
    explicit HashedHitter(Logger* logger, const ::std::string&);

private:
    void OnInnerNext(const Hit& v);
    void OnInnerCompletion();
    void OnInnerError(const Exceptional& err);
};

class ConcatHitter: public HitObservableObserver
{
    Logger* mLogger;
    QloMap< ::std::string, Hit::Type > mHits;
    ::std::deque< ::std::string > mSubs;
    ::std::string mName;

public:
    explicit ConcatHitter(Logger* logger, const ast::Node&);

private:
    void OnInnerNext(const Hit& v);
    void OnInnerCompletion();
    void OnInnerError(const Exceptional& err);

    bool AllPointed() const;
    bool NothingNone() const;
};

class HitAggregator: public HitObserver
{
    Logger* mLogger;
    Exceptional mExcept;
    ::std::string mTableName;
    ::std::deque< ::std::string > mPkey;
    QloMap< ::std::string, Hit::Type > mHits;

public:
    explicit HitAggregator(Logger*, const TableMeta&);

    const Exceptional& Except() const;
    ::std::tr1::tuple<int64_t, int64_t> GetRatio() const;
    ::std::string GetTableName() const;

private:
    void OnInnerNext(const Hit& v);
    void OnInnerCompletion();
    void OnInnerError(const Exceptional& err);
};

class ConditionHitter: public HitObservable
{
    Logger* mLogger;

public:
    explicit ConditionHitter(Logger* logger)
      : mLogger(logger)
    {}

    void Go(const QloMap< ::std::string, Json::Value >&);
};

} // namespace static_index

#endif /* OTS_STATIC_INDEX_INDEX_SELECTION_OBSERVERS_H */

