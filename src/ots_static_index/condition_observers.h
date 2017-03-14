#ifndef OTS_STATIC_INDEX_CONDITION_OBSERVERS_H
#define OTS_STATIC_INDEX_CONDITION_OBSERVERS_H

#include "insert_observers.h"
#include "type_delegates.h"
#include "slice.h"
#include "observable.h"
#include "ots_static_index/exceptional.h"
#include <tr1/unordered_set>
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

struct Boundary
{
    ::std::string mName;
    Value mLower;
    Value mUpper;

    explicit Boundary(const ::std::string&, const Value&, const Value&);
};

typedef DefaultObserver<Boundary, Exceptional> LimitObserver;
typedef DefaultObservable<Boundary, Exceptional> LimitObservable;
class LimitObservableObserver: public LimitObserver, public LimitObservable
{
public:
    explicit LimitObservableObserver(Logger* logger)
      : LimitObserver(logger)
    {}
};

class PickAttrLimiter: public LimitObservableObserver
{
    Logger* mLogger;
    ::std::string mName;

public:
    explicit PickAttrLimiter(Logger* logger, const ::std::string& name);

private:
    void OnInnerNext(const Boundary& v);
    void OnInnerCompletion();
    void OnInnerError(const Exceptional& err);
};

class HexLimiter: public LimitObservableObserver
{
    Logger* mLogger;
    ::std::string mName;
    uint64_t mLower;
    uint64_t mUpper;

public:
    explicit HexLimiter(Logger*, const ::std::string& name);

private:
    void OnInnerNext(const Boundary& v);
    void OnInnerCompletion();
    void OnInnerError(const Exceptional& err);
};

class ShiftToUint64Limiter: public LimitObservableObserver
{
    Logger* mLogger;
    ::std::string mName;
    int64_t mLower;
    int64_t mUpper;

public:
    explicit ShiftToUint64Limiter(Logger*, const ::std::string& name);

private:
    void OnInnerNext(const Boundary& v);
    void OnInnerCompletion();
    void OnInnerError(const Exceptional& err);
};

class Crc64Limiter: public LimitObservableObserver
{
    Logger* mLogger;
    ::std::string mName;

public:
    explicit Crc64Limiter(Logger*, const ::std::string& name);

private:
    void OnInnerNext(const Boundary& v);
    void OnInnerCompletion();
    void OnInnerError(const Exceptional& err);
};

class ConcatLimiter: public LimitObservableObserver
{
    Logger* mLogger;
    ::std::string mName;
    QloMap< ::std::string, int64_t> mLocations;
    ::std::deque< ::std::tr1::tuple<Value, Value> > mBoundaries;

public:
    explicit ConcatLimiter(Logger*, const ast::Node&);

private:
    void OnInnerNext(const Boundary& v);
    void OnInnerCompletion();
    void OnInnerError(const Exceptional& err);

private:
    Value Lower() const;
    Value Upper() const;
};


class LimitAggregator: public LimitObserver
{
public:
    typedef QloMap< ::std::string, Boundary > ColumnBoundaries;

private:
    Logger* mLogger;
    ColumnBoundaries mBoundaries;
    Exceptional mExcept;

public:
    explicit LimitAggregator(Logger* logger);

    const Exceptional& Except() const
    {
        return mExcept;
    }
    const ColumnBoundaries& GetBoundaries() const
    {
        return mBoundaries;
    }

private:
    void OnInnerNext(const Boundary& v);
    void OnInnerCompletion();
    void OnInnerError(const Exceptional& err);
};

class ConditionIssuer: public LimitObservable
{
    Logger* mLogger;

public:
    explicit ConditionIssuer(Logger* logger)
      : mLogger(logger)
    {}

    void Go(const QloMap< ::std::string, Json::Value>& condition);
};


class OptionAggregator: public ColumnObserver
{
public:
    typedef ::std::tr1::unordered_set<Value, ValueHasher> Options;
    typedef QloMap< ::std::string, Options > ColumnOptions;

private:
    Logger* mLogger;
    ColumnOptions mOptions;
    Exceptional mExcept;

public:
    explicit OptionAggregator(Logger*);

    const Exceptional& Except() const
    {
        return mExcept;
    }
    const ColumnOptions& GetOptions() const
    {
        return mOptions;
    }
private:
    void OnInnerNext(const Column&);
    void OnInnerCompletion();
    void OnInnerError(const Exceptional&);
};

class OptionExtractor: public ColumnObservable
{
    Logger* mLogger;

public:
    explicit OptionExtractor(Logger*);
    void Go(const QloMap< ::std::string, Json::Value>& condition);
};

} // namespace static_index

#endif /* OTS_STATIC_INDEX_CONDITION_OBSERVERS_H */

