#ifndef OTS_STATIC_INDEX_QUERY_OBSERVERS_H
#define OTS_STATIC_INDEX_QUERY_OBSERVERS_H

#include "threading.h"
#include "type_delegates.h"
#include "slice.h"
#include "observable.h"
#include "ots_static_index/exceptional.h"
#include <tr1/unordered_set>
#include <tr1/memory>
#include <string>
#include <deque>
#include <vector>
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

typedef QloMap< ::std::string, Value> AssociatedRow; // name -> value
typedef DefaultObserver< ::std::tr1::shared_ptr<AssociatedRow>, Exceptional> RowObserver;
typedef DefaultObservable< ::std::tr1::shared_ptr<AssociatedRow>, Exceptional> RowObservable;
class RowObservableObserver: public RowObserver, public RowObservable
{
public:
    explicit RowObservableObserver(Logger* logger)
      : RowObserver(logger)
    {}
};


class Latcher: public RowObservableObserver
{
    Logger* mLogger;
    Mutex mMutex;

public:
    explicit Latcher(Logger* logger);

private:
    void OnInnerNext(const ::std::tr1::shared_ptr<AssociatedRow>& row);
    void OnInnerCompletion();
    void OnInnerError(const Exceptional& err);
};

class ExactMatcher: public RowObservableObserver
{
    Logger* mLogger;
    const ::std::string mName;
    const Value mValue;

public:
    explicit ExactMatcher(
        Logger* logger,
        const ::std::string& name,
        const Value& value);

private:
    void OnInnerNext(const ::std::tr1::shared_ptr<AssociatedRow>& row);
    void OnInnerCompletion();
    void OnInnerError(const Exceptional& err);
};

class RangeMatcher: public RowObservableObserver
{
    Logger* mLogger;
    const ::std::string mName;
    const Value mValue;
    const uint64_t mRelation;

public:
    explicit RangeMatcher(
        Logger* logger,
        const ::std::string& name,
        const Value& value,
        const ::std::string& op);

private:
    void OnInnerNext(const ::std::tr1::shared_ptr<AssociatedRow>& row);
    void OnInnerCompletion();
    void OnInnerError(const Exceptional& err);

private:
    uint64_t MongoOpToRelation(const ::std::string& op);
};

class InsideMatcher: public RowObservableObserver
{
    Logger* mLogger;
    const ::std::string mName;
    ::std::tr1::unordered_set<Value, ValueHasher> mValues;

public:
    explicit InsideMatcher(
        Logger* logger,
        const ::std::string& name,
        const Json::Value& values);

private:
    void OnInnerNext(const ::std::tr1::shared_ptr<AssociatedRow>& row);
    void OnInnerCompletion();
    void OnInnerError(const Exceptional& err);

private:
    bool Hit(const Value& v) const;
};

class Sorter: public RowObservableObserver
{
    Logger* mLogger;
    ::std::string mOrderField;
    int mOrder; // 1: increasing, -1: decreasing
    ::std::deque< ::std::tr1::shared_ptr<AssociatedRow> > mUnrelated;

    class ValueComparison
    {
        int mOrder;
    public:
        explicit ValueComparison(int order)
          : mOrder(order)
        {}
        bool operator()(const Value& a, const Value& b) const;
        int CompareValueInMongoOrder(const Value& a, const Value& b) const;
    };
    ::std::multimap<Value, ::std::tr1::shared_ptr<AssociatedRow>, ValueComparison> mSorted;
    
public:
    explicit Sorter(Logger* logger, const ::std::string& field, int order);

private:
    void OnInnerNext(const ::std::tr1::shared_ptr<AssociatedRow>& row);
    void OnInnerCompletion();
    void OnInnerError(const Exceptional& err);

private:
    Value SearchField(const AssociatedRow& row);
};

class SliceWindow: public RowObservableObserver
{
    Logger* mLogger;
    int64_t mStart;
    int64_t mLimit;
    bool* mQuickQuit;

public:
    explicit SliceWindow(
        Logger* logger,
        bool* quickQuit,
        int64_t start,
        int64_t limit);

private:
    void OnInnerNext(const ::std::tr1::shared_ptr<AssociatedRow>& row);
    void OnInnerCompletion();
    void OnInnerError(const Exceptional& err);
};


class RowToJsonVector: public RowObserver
{
    Logger* mLogger;
    ::std::vector<Json::Value>* mOut;
    Exceptional mExcept;
    ::std::tr1::shared_ptr<Semaphore> mSem;
    QloSet<Slice> mUnwanted;
    QloSet< ::std::string> mWanted;

public:
    explicit RowToJsonVector(
        Logger* logger,
        ::std::vector<Json::Value>* out,
        const TableMeta& schema,
        const Json::Value& projection);

    Exceptional Wait();

private:
    void OnInnerNext(const ::std::tr1::shared_ptr<AssociatedRow>& row);
    void OnInnerCompletion();
    void OnInnerError(const Exceptional& err);

private:
    bool IsWanted(const ::std::string& name) const;
};


} // namespace static_index

#endif /* OTS_STATIC_INDEX_QUERY_OBSERVERS_H */
