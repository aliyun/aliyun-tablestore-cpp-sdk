#include "ots_static_index/static_index.h"
#include "insert_observers.h"
#include "query_observers.h"
#include "condition_observers.h"
#include "index_select_observers.h"
#include "bulk_executor.h"
#include "alarm_clock.h"
#include "def_ast.h"
#include "arithmetic.h"
#include "security.h"
#include "timestamp.h"
#include "random.h"
#include "type_delegates.h"
#include "logging_assert.h"
#include "observable.h"
#include "slice.h"
#include "foreach.h"
#include "jsonize.h"
#include <tr1/functional>
#include <tr1/unordered_set>
#include <tr1/tuple>
#include <limits>
#include <utility>
#include <set>
#include <map>
#include <algorithm>
#include <stdint.h>

using namespace ::std;
using namespace ::std::tr1;
using namespace ::std::tr1::placeholders;

#define OBS_TRY(err) \
    if (err.GetCode() != ::static_index::Exceptional::OTS_OK) { \
        NotifyError(err);                                       \
        return;                                                 \
    }
#define OTS_FICUS_TRY(x, what)                     \
    do {\
        const ::static_index::Exceptional& _ex12345 = (x);                   \
        if (_ex12345.GetCode() != ::static_index::Exceptional::OTS_OK) {      \
            OTS_LOG_ERROR(mLogger)\
                (_ex12345.GetErrorCode())\
                (_ex12345.GetErrorMessage())\
                (_ex12345.GetRequestId())\
                .What(what); \
            return FICUS_OBJECT_STORAGE_OPERATION_ERROR;\
        }\
    } while(false)

namespace static_index {

int64_t sOTS_StaticIndex_GetRangeLimit = 5000; // no more than 5000

namespace {

class Runnable
{
public:
    virtual ~Runnable() {}
    virtual Future* AsyncRun() =0;
};

string Tracker(IRandom* rnd, const string& base)
{
    string res = base;
    res.push_back('_');
    FillUuid(rnd, &res);
    return res;
}

typedef ::std::tr1::unordered_set<Value, ValueHasher> ValueSet;
typedef QloMap< ::std::string, ValueSet> ColumnValues;

class StaticallyIndexedImpl: public StaticallyIndexed
{
    Exceptional mExcept;
    ClientDelegate* mClient; // not own
    Logger* mLogger; // not own
    map<string, CollectionMeta> mSchema; // collection -> its schema
    auto_ptr<IRandom> mRandom;
    auto_ptr<AlarmClock> mAlarmClock;
    auto_ptr<BulkExecutor> mBulkExecutor;

public:
    StaticallyIndexedImpl(
        const Settings& settings,
        ClientDelegate* client)
      : mClient(client),
        mLogger(settings.mLogger),
        mRandom(NewDefaultRandom(settings.mLogger)),
        mAlarmClock(new AlarmClock(settings.mLogger, mRandom.get(), settings.mThreadPool)),
        mBulkExecutor(
            new BulkExecutor(
                settings.mLogger,
                settings.mThreadPool,
                mRandom.get(),
                mAlarmClock.get(),
                client))
    {
        OTS_LOG_INFO(mLogger)(mRandom->Seed());
        if (!settings.mSchema.isArray()) {
            OTS_LOG_ERROR(mLogger)
                (ToString(settings.mSchema))
                .What("schema must be an array.");
            mExcept = Exceptional("schema must be an array.");
            return;
        }
        for(int i = 0, sz = settings.mSchema.size(); i < sz; ++i) {
            const Json::Value& sch = settings.mSchema[i];
            CollectionMeta meta;
            mExcept = ParseSchema(mLogger, &meta, sch);
            if (mExcept.GetCode() != Exceptional::OTS_OK) {
                return;
            }
            mSchema[meta.mPrimaryTable] = meta;
        }
    }

    int Insert(const string& collection, const Json::Value& data);
    int Insert(
        const string& collection,
        const vector<Json::Value>& data);

    int Find(
        const ::std::string& collection,
        const Json::Value& projection,
        const Json::Value& condition,
        int64_t start,
        int64_t limit,
        const Json::Value& order,
        vector<Json::Value>& out);
    int Count(
        const ::std::string& collection,
        const ::Json::Value& condition,
        int64_t start,
        int64_t limit,
        uint64_t& total);

    int Delete(
        const ::std::string& collection,
        const Json::Value& condition);

    int Update(
        const ::std::string& collection,
        const ::Json::Value& condition,
        const ::Json::Value& newData);
    int Upsert(
        const ::std::string& collection,
        const ::Json::Value& condition,
        const ::Json::Value& newData);

    Exceptional _Insert(
        deque<PutRowRequest>* indexReqs,
        deque<PutRowRequest>* primaryReqs,
        const string& track,
        const CollectionMeta& cmeta,
        const Json::Value& data);
    Exceptional BuildInsertTableFlow(
        shared_ptr<ColumnObserver>* enter,
        const shared_ptr<PutRowRequestFiller>&,
        const TableMeta&);


    Exceptional SelectIndex(
        string*,
        const CollectionMeta& schema,
        const QloMap<string, Json::Value>& condition);
    HitObservableObserver* SelectIndexFactory(const ast::Node&);
        
    Exceptional BuildQueryPipelineOnScanningPrimaryTable(
        deque<shared_ptr<Runnable> >*,
        shared_ptr<RowToJsonVector>*,
        vector<Json::Value>* out,
        bool* quickQuit,
        const string& tracker,
        const TableMeta& schema,
        const QloMap<string, Json::Value>& cond,
        const Json::Value& order,
        const Json::Value& projection,
        int64_t start,
        int64_t limit);
    Exceptional BuildQueryPipelineOnScanningIndex(
        deque<shared_ptr<Runnable> >*,
        shared_ptr<RowToJsonVector>*,
        vector<Json::Value>* out,
        bool* quickQuit,
        const string& tracker,
        const TableMeta& primary,
        const TableMeta& index,
        const QloMap<string, Json::Value>& cond,
        const Json::Value& order,
        const Json::Value& projection,
        int64_t start,
        int64_t limit);
    Exceptional BuildQueryRequests(
        deque<GetRangeRequest>* getRanges,
        deque<GetRowRequest>* getRows,
        const string& tracker,
        const TableMeta& tmeta,
        const QloMap<string, Json::Value>& condition);
    Exceptional BuildGetRowRequests(
        deque<GetRowRequest>* results,
        QloMap<string, Value>* atomValues,
        const string& tracker,
        const TableMeta& tmeta,
        const ColumnValues& atoms,
        const string& lastColumn);
    Exceptional BuildGetRangeRequests(
        deque<GetRangeRequest>* getRows,
        QloMap<string, Value>* atomValues,
        const string& tracker,
        const TableMeta& tmeta,
        const LimitAggregator::ColumnBoundaries& boundaries,
        const ColumnValues& atoms,
        const string& lastColumn);

    Exceptional BuildBoundaryFlow(
        const shared_ptr<LimitObservable>& enter,
        const shared_ptr<LimitObserver>& leave,
        const TableMeta& schema);
    LimitObservableObserver* FindFactory(const ast::Node&);


    Exceptional _Delete(
        deque<DeleteRowRequest>* indexReqs,
        deque<DeleteRowRequest>* primaryReqs,
        const string& track,
        const CollectionMeta& cmeta,
        const Json::Value& data);
    Exceptional BuildDeleteTableFlow(
        const shared_ptr<ColumnObservable>& enter,
        const shared_ptr<DeleteRowRequestFiller>&,
        const TableMeta&);
};

int StaticallyIndexedImpl::Insert(
    const string& collection,
    const Json::Value& data)
{
    OTS_FICUS_TRY(mExcept, "error on initialization");
    map<string, CollectionMeta>::const_iterator it = mSchema.find(collection);
    if (it == mSchema.end()) {
        OTS_FICUS_TRY(
            Exceptional("unknown collection: " + collection),
            "error on insertion");
    }
    const CollectionMeta& schema = it->second;
    string track = Tracker(mRandom.get(), "Insert");
    OTS_LOG_DEBUG(mLogger)(collection)(track)(UtcTime::Now(mLogger).ToISO8601())
        (ToString(data))
        .What("Start inserting");

    deque<PutRowRequest> primaryReqs;
    deque<PutRowRequest> indexReqs;
    OTS_FICUS_TRY(
        _Insert(&indexReqs, &primaryReqs, track, schema, data),
        "error on inserting");
    deque<shared_ptr<Future> > futures;
    deque<Exceptional> excepts;
    FOREACH_ITER(i, indexReqs) {
        excepts.push_back(Exceptional());
        futures.push_back(mBulkExecutor->PutRow(&(excepts.back()), *i));
    }
    for(; !futures.empty(); futures.pop_back()) {
        futures.back()->Wait();
    }
    for(; !excepts.empty(); excepts.pop_back()) {
        OTS_FICUS_TRY(
            excepts.back(),
            "error on insertion");
    }
    FOREACH_ITER(i, primaryReqs) {
        excepts.push_back(Exceptional());
        futures.push_back(mBulkExecutor->PutRow(&(excepts.back()), *i));
    }
    for(; !futures.empty(); futures.pop_back()) {
        futures.back()->Wait();
    }
    for(; !excepts.empty(); excepts.pop_back()) {
        OTS_FICUS_TRY(
            excepts.back(),
            "error on insertion");
    }

    OTS_LOG_DEBUG(mLogger)
        (collection)
        (track)
        (UtcTime::Now(mLogger).ToISO8601())
        .What("Succeed inserting");
    return FICUS_SUCC;
}

int StaticallyIndexedImpl::Insert(
    const string& collection,
    const vector<Json::Value>& data)
{
    OTS_FICUS_TRY(mExcept, "error on initialization");
    map<string, CollectionMeta>::const_iterator it = mSchema.find(collection);
    if (it == mSchema.end()) {
        OTS_FICUS_TRY(
            Exceptional("unknown collection: " + collection),
            "error on insertion");
    }
    const CollectionMeta& schema = it->second;
    string track = Tracker(mRandom.get(), "Insert");
    OTS_LOG_DEBUG(mLogger)(collection)(track)(UtcTime::Now(mLogger).ToISO8601())
        .What("Start batch inserting");

    deque<PutRowRequest> primaryReqs;
    deque<PutRowRequest> indexReqs;
    FOREACH_ITER(i, data) {
        OTS_FICUS_TRY(
            _Insert(&indexReqs, &primaryReqs, track, schema, *i),
            "error on insertion");
    }

    OTS_LOG_DEBUG(mLogger)
        (track)
        (UtcTime::Now(mLogger).ToISO8601())
        .What("requests ready");
    deque<shared_ptr<Future> > futures;
    deque<Exceptional> excepts;
    FOREACH_ITER(i, indexReqs) {
        excepts.push_back(Exceptional());
        futures.push_back(mBulkExecutor->PutRow(&(excepts.back()), *i));
    }
    for(; !futures.empty(); futures.pop_back()) {
        futures.back()->Wait();
    }
    for(; !excepts.empty(); excepts.pop_back()) {
        OTS_FICUS_TRY(
            excepts.back(),
            "error on insertion");
    }

    OTS_LOG_DEBUG(mLogger)
        (track)
        (UtcTime::Now(mLogger).ToISO8601())
        .What("Indexes inserted");
    FOREACH_ITER(i, primaryReqs) {
        excepts.push_back(Exceptional());
        futures.push_back(mBulkExecutor->PutRow(&(excepts.back()), *i));
    }
    for(; !futures.empty(); futures.pop_back()) {
        futures.back()->Wait();
    }
    for(; !excepts.empty(); excepts.pop_back()) {
        OTS_FICUS_TRY(
            excepts.back(),
            "error on insertion");
    }

    OTS_LOG_DEBUG(mLogger)
        (collection)
        (track)
        (UtcTime::Now(mLogger).ToISO8601())
        .What("Succeed inserting");
    return FICUS_SUCC;
}

Exceptional StaticallyIndexedImpl::_Insert(
    deque<PutRowRequest>* indexReqs,
    deque<PutRowRequest>* primaryReqs,
    const string& track,
    const CollectionMeta& cmeta,
    const Json::Value& data)
{
    shared_ptr<JsonValueToColumn> init(new JsonValueToColumn(mLogger));

    shared_ptr<PutRowRequestFiller> primary;
    deque<shared_ptr<PutRowRequestFiller> > indexes;
    {
        const TableMeta& tmeta = cmeta.mTableMetas.at(cmeta.mPrimaryTable);
        string mytrack = Tracker(mRandom.get(), track);
        primary.reset(
            new PutRowRequestFiller(mLogger, primaryReqs, tmeta, mytrack, true));
        shared_ptr<ColumnObserver> enter;
        OTS_TRY(BuildInsertTableFlow(&enter, primary, tmeta));
        init->ObserveBy(enter);
    }

    FOREACH_ITER(i, cmeta.mIndexes) {
        const TableMeta& tmeta = cmeta.mTableMetas.at(*i);
        shared_ptr<AttrFilter> filter(new AttrFilter(mLogger, tmeta));
        init->ObserveBy(filter);

        string mytrack = Tracker(mRandom.get(), track);
        shared_ptr<PutRowRequestFiller> reqFiller(
            new PutRowRequestFiller(mLogger, indexReqs, tmeta, mytrack, false));
        shared_ptr<ColumnObserver> enter;
        OTS_TRY(BuildInsertTableFlow(&enter, reqFiller, tmeta));
        filter->ObserveBy(enter);
        indexes.push_back(reqFiller);
    }

    init->Go(track, data);
    OTS_TRY(primary->GetExcept());
    FOREACH_ITER(i, indexes) {
        OTS_TRY((*i)->GetExcept());
    }

    return Exceptional();
}

template<typename T>
Exceptional BuildProcessers(
    deque<shared_ptr<T> >* heads,
    shared_ptr<T>* tail,
    const shared_ptr<ast::Node>& root,
    const function<T*(const ast::Node&)>& factory)
{
    deque< shared_ptr<ast::Node> > bfs;
    ast::SortInBfs(&bfs, root);

    map<string, shared_ptr<T> > observers;
    for(; !bfs.empty(); bfs.pop_back()) {
        const ast::Node& node = *(bfs.back());
        if (observers.count(node.mName)) {
            *tail = observers[node.mName];
        } else {
            shared_ptr<T> p(factory(node));
            *tail = p;
            observers[node.mName] = p;
            if (node.mNodeType == ast::Node::ATTR) {
                heads->push_back(p);
            }
            FOREACH_ITER(i, node.mChildren) {
                const ast::Node& child = **i;
                const shared_ptr<T>& q = observers.at(child.mName);
                q->ObserveBy(p);
            }
        }
    }

    return Exceptional();
}

ColumnObservableObserver* InsertFactory(
    Logger* logger,
    const ast::Node& node)
{
    switch(node.mNodeType) {
    case ast::Node::ATTR: return new PickAttrTransformer(logger, node.mName);
    case ast::Node::CRC64INT: return new Crc64IntTransformer(logger, node.mName);
    case ast::Node::CRC64STR: return new Crc64StrTransformer(logger, node.mName);
    case ast::Node::HEX: return new HexTransformer(logger, node.mName);
    case ast::Node::SHIFT_TO_UINT64: return new ShiftToUint64Transformer(logger, node.mName);
    case ast::Node::CONCAT: return new ConcatTransformer(logger, node, '|');
    }
    OTS_ASSERT(logger, false);
    return NULL;
}

Exceptional StaticallyIndexedImpl::BuildInsertTableFlow(
    shared_ptr<ColumnObserver>* enter,
    const shared_ptr<PutRowRequestFiller>& reqFiller,
    const TableMeta& schema)
{
    shared_ptr<AttrValidator> av(new AttrValidator(mLogger, schema));
    av->ObserveBy(reqFiller);
    *enter = av;

    FOREACH_ITER(i, schema.mPrimaryKey) {
        const shared_ptr<ast::Node>& root = *i;
        if (root->mNodeType != ast::Node::ATTR) {
            deque<shared_ptr<ColumnObservableObserver> > heads;
            shared_ptr<ColumnObservableObserver> tail;
            OTS_TRY(BuildProcessers<ColumnObservableObserver>(
                    &heads, &tail, root,
                    bind(&InsertFactory, mLogger, _1)));

            FOREACH_ITER(i, heads) {
                av->ObserveBy(*i);
            }
            if (tail) {
                tail->ObserveBy(reqFiller);
            }
        }
    }
    return Exceptional();
}


class GetRangeRequester: public RowObservable, public Runnable
{
    Logger* mLogger;
    BulkExecutor* mBulkExecutor;
    GetRangeRequest mRequest;
    GetRangeResponse mResponse;
    Exceptional mExcept;
    const bool* mQuickQuit;

public:
    class GrrFuture: public Future
    {
        GetRangeRequester* mRequester;
        const bool* mQuickQuit;
        shared_ptr<Future> mFuture;

    public:
        explicit GrrFuture(
            GetRangeRequester* grr,
            BulkExecutor* client,
            const GetRangeRequest& req,
            const bool* quickQuit)
          : mRequester(grr),
            mQuickQuit(quickQuit)
        {
            Reset(req);
        }
        void Reset(const GetRangeRequest& req)
        {
            mRequester->mExcept = Exceptional();
            mRequester->mResponse = GetRangeResponse();
            if (__atomic_load_1(mQuickQuit, __ATOMIC_ACQUIRE)) {
                mFuture.reset(new Semaphore(mRequester->mLogger, 1));
            } else {
                mFuture = mRequester->mBulkExecutor->GetRange(
                    &(mRequester->mExcept),
                    &(mRequester->mResponse),
                    req);
            }
        }
        bool TryWait()
        {
            if (!mFuture->TryWait()) {
                return false;
            } else {
                return mRequester->Continue(this);
            }
        }
        void Wait()
        {
            for(;;) {
                mFuture->Wait();
                bool r = mRequester->Continue(this);
                if (!r) {
                    return;
                }
            }
        }
        bool Wait(const Interval& dur)
        {
            if (!mFuture->Wait(dur)) {
                return false;
            } else {
                return mRequester->Continue(this);
            }
        }
    };
    
    explicit GetRangeRequester(
        Logger* logger,
        BulkExecutor* client,
        const GetRangeRequest& req,
        const bool* quickQuit)
      : mLogger(logger),
        mBulkExecutor(client),
        mRequest(req),
        mQuickQuit(quickQuit)
    {}

    Future* AsyncRun()
    {
        OTS_LOG_INFO(mLogger)(mRequest.mTracker);
        OTS_LOG_DEBUG(mLogger)(ToString(Jsonize(mLogger, mRequest)));
        return new GrrFuture(this, mBulkExecutor, mRequest, mQuickQuit);
    }

private:
    bool Continue(GrrFuture* future)
    {
        OTS_LOG_DEBUG(mLogger)
            (ToString(Jsonize(mLogger, mRequest)))
            .What("continue");
        if (mExcept.GetCode() != Exceptional::OTS_OK) {
            NotifyError(mExcept);
            return true;
        }
        bool r = NotifyRow();
        if (!r) {
            OTS_LOG_DEBUG(mLogger)
                (ToString(Jsonize(mLogger, mRequest)))
                .What("retry");
            future->Reset(mRequest);
            return false;
        } else {
            OTS_LOG_DEBUG(mLogger)
                .What("done");
            NotifyCompletion();
            return true;
        }
    }
    bool NotifyRow()
    {
        FOREACH_ITER(i, mResponse.mRows) {
            if (__atomic_load_1(mQuickQuit, __ATOMIC_ACQUIRE)) {
                return true;
            }
            const shared_ptr<Row>& row = *i;
            shared_ptr<AssociatedRow> ass(new AssociatedRow());
            FOREACH_ITER(j, row->mPrimaryKey) {
                ass->insert(make_pair(j->mName, j->mValue));
            }
            FOREACH_ITER(j, row->mAttributes) {
                ass->insert(make_pair(j->mName, j->mValue));
            }
            NotifyNext(ass);
        }
        if (mResponse.mNextPkey.empty()) {
            return true;
        }
        ::std::swap(mRequest.mStartPkey, mResponse.mNextPkey);
        return false;
    }
};

class GetRowRequester: public RowObservable, public Runnable
{
    Logger* mLogger;
    BulkExecutor* mBulkExecutor;
    GetRowRequest mRequest;
    GetRowResponse mResponse;
    Exceptional mExcept;
    const bool* mQuickQuit;

public:
    class GrrFuture: public Future
    {
        GetRowRequester* mRequester;
        const bool* mQuickQuit;
        shared_ptr<Future> mSem;

    public:
        explicit GrrFuture(
            GetRowRequester* grr,
            BulkExecutor* client,
            const GetRowRequest& req,
            const bool* quickQuit)
          : mRequester(grr),
            mQuickQuit(quickQuit)
        {
            if (__atomic_load_1(mQuickQuit, __ATOMIC_ACQUIRE)) {
                mSem.reset(new Semaphore(mRequester->mLogger, 1));
            } else {
                mSem = mRequester->mBulkExecutor->GetRow(
                    &(mRequester->mExcept),
                    &(mRequester->mResponse),
                    req);
            }
        }

        bool TryWait()
        {
            if (!mSem->TryWait()) {
                return false;
            } else {
                mRequester->Continue();
                return true;
            }
        }
        void Wait()
        {
            mSem->Wait();
            mRequester->Continue();
        }
        bool Wait(const Interval& dur)
        {
            if (!mSem->Wait(dur)) {
                return false;
            } else {
                mRequester->Continue();
                return true;
            }
        }
    };

    explicit GetRowRequester(
        Logger* logger,
        BulkExecutor* client,
        const GetRowRequest& req,
        const bool* quickQuit)
      : mLogger(logger),
        mBulkExecutor(client),
        mRequest(req),
        mQuickQuit(quickQuit)
    {}

    Future* AsyncRun()
    {
        OTS_LOG_INFO(mLogger)(mRequest.mTracker);
        OTS_LOG_DEBUG(mLogger)(ToString(Jsonize(mLogger, mRequest)));
        return new GrrFuture(this, mBulkExecutor, mRequest, mQuickQuit);
    }

private:
    void Continue()
    {
        OBS_TRY(mExcept);
        NotifyRow();
        NotifyCompletion();
    }
    void NotifyRow()
    {
        if (__atomic_load_1(mQuickQuit, __ATOMIC_ACQUIRE)) {
            return;
        }
        const shared_ptr<Row>& row = mResponse.mRow;
        shared_ptr<AssociatedRow> ass(new AssociatedRow());
        FOREACH_ITER(j, row->mPrimaryKey) {
            ass->insert(make_pair(j->mName, j->mValue));
        }
        FOREACH_ITER(j, row->mAttributes) {
            ass->insert(make_pair(j->mName, j->mValue));
        }
        if (ass->empty()) {
            return;
        }
        NotifyNext(ass);
    }
};

class PkeyFiller: public ColumnObserver
{
    Logger* mLogger;
    deque<Column> mPkey;
    deque<string> mPkeyNames;
    QloMap<string, Value> mValues;
    Exceptional mExcept;

public:
    explicit PkeyFiller(
        Logger* logger,
        const TableMeta& schema)
      : ColumnObserver(logger),
        mLogger(logger)
    {
        FOREACH_ITER(i, schema.mPrimaryKey) {
            const ast::Node& rt = **i;
            mPkeyNames.push_back(rt.mName);
        }
    }

    const Exceptional& Except() const
    {
        return mExcept;
    }
    deque<Column>& GetPrimaryKey()
    {
        return mPkey;
    }

private:
    void OnInnerNext(const Column& c)
    {
        bool r = mValues.insert(make_pair(c.mName, c.mValue)).second;
        OTS_ASSERT(mLogger, r)
            (c.mName);
    }
    void OnInnerCompletion()
    {
        FOREACH_ITER(i, mPkeyNames) {
            const string& name = *i;
            QloMap<string, Value>::const_iterator it = mValues.find(name);
            if (it != mValues.end()) {
                Column c(mLogger);
                c.mName = name;
                c.mValue = it->second;
                mPkey.push_back(c);
            } else {
                mExcept = Exceptional("Missing primary key in primary table: " + name);
                return;
            }
        }
    }
    void OnInnerError(const Exceptional& err)
    {
        mExcept = err;
    }
};

class RowToPrimary: public ColumnObservable
{
    Logger* mLogger;
    
public:
    explicit RowToPrimary(
        Logger* logger)
      : mLogger(logger)
    {}

    void Go(const AssociatedRow& row)
    {
        FOREACH_ITER(i, row) {
            Column c(mLogger);
            c.mName = i->first;
            c.mValue = i->second;
            NotifyNext(c);
        }
        NotifyCompletion();
    }
};

class IndexRowToPrimaryRequester: public RowObservableObserver
{
    Logger* mLogger;
    BulkExecutor* mBulkExecutor;
    const bool* mQuickQuit;
    const TableMeta& mPrimaryTable;
    const string mTracker;
    Mutex mMutex;
    deque<Future*> mWaitings;

    struct IrtpFuture: public Future
    {
        IndexRowToPrimaryRequester* mRequester;
        const bool* mQuickQuit;
        GetRowRequest mRequest;
        GetRowResponse mResponse;
        Exceptional mExcept;
        shared_ptr<Future> mFuture;

        explicit IrtpFuture(
            IndexRowToPrimaryRequester* requester,
            const GetRowRequest& req)
          : mRequester(requester),
            mQuickQuit(requester->mQuickQuit),
            mRequest(req)
        {
            if (__atomic_load_1(mQuickQuit, __ATOMIC_ACQUIRE)) {
                mFuture.reset(new Semaphore(mRequester->mLogger, 1));
            } else {
                mFuture = mRequester->mBulkExecutor->GetRow(&mExcept, &mResponse, mRequest);
            }
        }
        bool TryWait()
        {
            bool r = mFuture->TryWait();
            if (!r) {
                return false;
            } else {
                mRequester->Continue(*this);
                return true;
            }
        }
        void Wait()
        {
            mFuture->Wait();
            mRequester->Continue(*this);
        }
        bool Wait(const Interval& dur)
        {
            bool r = mFuture->Wait(dur);
            if (!r) {
                return false;
            } else {
                mRequester->Continue(*this);
                return true;
            }
        }
    };

public:
    explicit IndexRowToPrimaryRequester(
        Logger* logger,
        BulkExecutor* client,
        const bool* quickQuit,
        const string& tracker,
        const TableMeta& primary)
      : RowObservableObserver(logger),
        mLogger(logger),
        mBulkExecutor(client),
        mQuickQuit(quickQuit),
        mPrimaryTable(primary),
        mTracker(tracker),
        mMutex(logger)
    {
    }

private:
    void Continue(const IrtpFuture& res)
    {
        if (res.mExcept.GetCode() != Exceptional::OTS_OK) {
            OTS_LOG_ERROR(mLogger)
                ((int) res.mExcept.GetCode())
                (res.mExcept.GetErrorMessage());
            return;
        }
        if (__atomic_load_1(mQuickQuit, __ATOMIC_ACQUIRE)) {
            return;
        }
        const shared_ptr<Row>& row = res.mResponse.mRow;
        OTS_LOG_DEBUG(mLogger)(ToString(Jsonize(mLogger, res.mResponse)));
        shared_ptr<AssociatedRow> ass(new AssociatedRow());
        FOREACH_ITER(j, row->mPrimaryKey) {
            ass->insert(make_pair(j->mName, j->mValue));
        }
        FOREACH_ITER(j, row->mAttributes) {
            ass->insert(make_pair(j->mName, j->mValue));
        }
        if (ass->empty()) {
            return;
        }
        NotifyNext(ass);
    }
    void OnInnerNext(const shared_ptr<AssociatedRow>& ass)
    {
        shared_ptr<PkeyFiller> pkeyFiller(new PkeyFiller(mLogger, mPrimaryTable));
        shared_ptr<AttrValidator> av(new AttrValidator(mLogger, mPrimaryTable));
        FOREACH_ITER(i, mPrimaryTable.mPrimaryKey) {
            const shared_ptr<ast::Node>& root = *i;
            deque<shared_ptr<ColumnObservableObserver> > heads;
            shared_ptr<ColumnObservableObserver> tail;
            const Exceptional& ex = BuildProcessers<ColumnObservableObserver>(
                &heads, &tail, root,
                bind(&InsertFactory, mLogger, _1));
            OTS_ASSERT(mLogger, ex.GetCode() == Exceptional::OTS_OK)
                ((int) ex.GetCode())
                (ex.GetErrorMessage());

            FOREACH_ITER(i, heads) {
                av->ObserveBy(*i);
            }
            if (tail) {
                tail->ObserveBy(pkeyFiller);
            }
        }
        shared_ptr<RowToPrimary> rtp(new RowToPrimary(mLogger));
        rtp->ObserveBy(av);
        rtp->Go(*ass);
        const Exceptional& ex = pkeyFiller->Except();
        OTS_ASSERT(mLogger, ex.GetCode() == Exceptional::OTS_OK)
            ((int) ex.GetCode())
            (ex.GetErrorMessage());

        GetRowRequest req;
        req.mTracker = mTracker;
        req.mTableName = mPrimaryTable.mTableName;
        req.mPrimaryKey.swap(pkeyFiller->GetPrimaryKey());
        OTS_LOG_INFO(mLogger)(req.mTracker);
        OTS_LOG_DEBUG(mLogger)(ToString(Jsonize(mLogger, req)));

        Future* future = new IrtpFuture(this, req);
        {
            Scoped<Mutex> g(&mMutex);
            mWaitings.push_back(future);
        }
    }
    void OnInnerCompletion()
    {
        deque<Future*> done;
        for(; !mWaitings.empty(); ) {
            RemoveDone(&mWaitings, &done);
            for(; !done.empty(); done.pop_back()) {
                delete done.back();
            }
        }
        NotifyCompletion();
    }
    void OnInnerError(const Exceptional&)
    {
    }

};

void ReformatCondition(QloMap<string, Json::Value>* out, const Json::Value& cond)
{
    const vector<string>& keys = cond.getMemberNames();
    FOREACH_ITER(i, keys) {
        (*out)[*i] = cond[*i];
    }
}

int StaticallyIndexedImpl::Count(
    const string& collection,
    const Json::Value& condition,
    int64_t start,
    int64_t limit,
    uint64_t& out)
{
    vector<Json::Value> res;
    int rtn = Find(
        collection, Json::arrayValue, condition, start, limit, Json::objectValue,
        res);
    if (rtn != FICUS_SUCC) {
        return rtn;
    }
    out = res.size();
    return FICUS_SUCC;
}

int StaticallyIndexedImpl::Find(
    const ::std::string& collection,
    const Json::Value& projection,
    const Json::Value& cond,
    int64_t start,
    int64_t limit,
    const Json::Value& order,
    vector<Json::Value>& out)
{
    OTS_FICUS_TRY(mExcept, "error on initialization");
    if (!projection.isArray()) {
        OTS_LOG_ERROR(mLogger)
            (ToString(projection))
            .What("\"projection\" must be array.");
        OTS_FICUS_TRY(
            Exceptional("\"projection\" must be array."),
            "error on finding");
    }
    if (!cond.isObject()) {
        OTS_LOG_ERROR(mLogger)
            (ToString(cond))
            .What("\"condition\" must be object.");
        OTS_FICUS_TRY(
            Exceptional("\"condition\" must be object."),
            "error on finding");
    }
    if (!order.isObject()) {
        OTS_LOG_ERROR(mLogger)
            (ToString(order))
            .What("\"order\" must be object.");
        OTS_FICUS_TRY(
            Exceptional("\"order\" must be object."),
            "error on finding");
    }
    if (start < 0) {
        OTS_LOG_ERROR(mLogger)
            (ToString(start))
            .What("\"start\" must be nonnegative.");
        OTS_FICUS_TRY(
            Exceptional("\"start\" must be nonnegative."),
            "error on finding");
    }
    if (limit < 0) {
        OTS_LOG_ERROR(mLogger)
            (ToString(limit))
            .What("\"limit\" must be nonnegative.");
        OTS_FICUS_TRY(
            Exceptional("\"limit\" must be nonnegative."),
            "error on finding");
    }

    out.clear();
    string tracker = "Find_";
    FillUuid(mRandom.get(), &tracker);
    OTS_LOG_DEBUG(mLogger)
        (tracker)
        (collection)
        (ToString(projection))
        (ToString(cond))
        (ToString(start))
        (ToString(limit))
        (ToString(order))
        .What("An incoming Find request");
    map<string, CollectionMeta>::const_iterator it = mSchema.find(collection);
    if (it == mSchema.end()) {
        OTS_FICUS_TRY(
            Exceptional("unknown collection: " + collection),
            "error on finding");
    }
    const CollectionMeta& schema = it->second;

    QloMap<string, Json::Value> condition;
    ReformatCondition(&condition, cond);
    string index;
    OTS_FICUS_TRY(
        SelectIndex(&index, schema, condition),
        "error on finding");
    bool quickQuit = false;
    deque<shared_ptr<Runnable> > requesters;
    shared_ptr<RowToJsonVector> final;
    if (index.empty() || index == collection) {
        OTS_LOG_INFO(mLogger)(tracker)(index).What("Primary table is chosen.");
        OTS_FICUS_TRY(
            BuildQueryPipelineOnScanningPrimaryTable(
                &requesters, &final,
                &out, &quickQuit,
                tracker,
                schema.mTableMetas.at(schema.mPrimaryTable),
                condition, order, projection, start, limit),
            "error on finding");
    } else {
        OTS_LOG_INFO(mLogger)(tracker)(index).What("Index is chosen.");
        OTS_FICUS_TRY(
            BuildQueryPipelineOnScanningIndex(
                &requesters, &final,
                &out, &quickQuit,
                tracker,
                schema.mTableMetas.at(schema.mPrimaryTable),
                schema.mTableMetas.at(index),
                condition, order, projection, start, limit),
            "error on finding");
    }

    deque<Future*> waiting;
    FOREACH_ITER(i, requesters) {
        waiting.push_back((*i)->AsyncRun());
    }
    OTS_LOG_DEBUG(mLogger)
        (waiting.size())
        .What("total requests");
    deque<Future*> done;
    for(; !waiting.empty();) {
        RemoveDone(&waiting, &done);
        for(; !done.empty(); done.pop_back()) {
            delete done.back();
        }
        Interval dur = Interval::FromUsec(mLogger, NextInt(mRandom.get(), 100, 1000));
        SleepFor(dur);
    }

    OTS_FICUS_TRY(
        final->Wait(),
        "error on finding");
    return FICUS_SUCC;
}

Exceptional StaticallyIndexedImpl::SelectIndex(
    string* table,
    const CollectionMeta& schema,
    const QloMap<string, Json::Value>& condition)
{
    shared_ptr<ConditionHitter> ch(new ConditionHitter(mLogger));
    deque<shared_ptr<HitAggregator> > aggrs;

    shared_ptr<HitAggregator> primary(
        new HitAggregator(
            mLogger,
            schema.mTableMetas.at(schema.mPrimaryTable)));
    aggrs.push_back(primary);
    FOREACH_ITER(i, schema.mIndexes) {
        shared_ptr<HitAggregator> index(
            new HitAggregator(
                mLogger,
                schema.mTableMetas.at(*i)));
        aggrs.push_back(index);
    }

    deque<shared_ptr<HitObservableObserver> > heads;
    FOREACH_ITER(i, aggrs) {
        const shared_ptr<HitAggregator>& hit = *i;
        const TableMeta& tmeta = schema.mTableMetas.at(hit->GetTableName());
        FOREACH_ITER(j, tmeta.mPrimaryKey) {
            shared_ptr<HitObservableObserver> tail;
            OTS_TRY(BuildProcessers<HitObservableObserver>(
                    &heads, &tail, *j,
                    bind(&StaticallyIndexedImpl::SelectIndexFactory, this, _1)));
            tail->ObserveBy(hit);
        }
    }
    FOREACH_ITER(i, heads) {
        ch->ObserveBy(*i);
    }

    ch->Go(condition);

    int64_t maxDividen = 0;
    int64_t maxDivisor = 1;
    shared_ptr<HitAggregator> maxHit;
    FOREACH_ITER(i, aggrs) {
        const shared_ptr<HitAggregator>& ag = *i;
        int64_t dividen = 0;
        int64_t divisor = 0;
        tie(dividen, divisor) = ag->GetRatio();
        if (maxDividen * divisor < dividen * maxDivisor) {
            OTS_LOG_DEBUG(mLogger)
                (ag->GetTableName())
                (dividen)
                (divisor);
            maxDividen = dividen;
            maxDivisor = divisor;
            maxHit = ag;
        } else if (maxDividen * divisor == dividen * maxDivisor
            && ToSlice(ag->GetTableName()) == ToSlice(schema.mPrimaryTable)) {
            OTS_LOG_DEBUG(mLogger)
                (ag->GetTableName())
                (dividen)
                (divisor);
            maxDividen = dividen;
            maxDivisor = divisor;
            maxHit = ag;
        }
    }
    *table = maxHit->GetTableName();
    return Exceptional();
}

HitObservableObserver* StaticallyIndexedImpl::SelectIndexFactory(const ast::Node& node)
{
    switch(node.mNodeType) {
    case ast::Node::ATTR: return new AttrHitter(mLogger, node.mName);
    case ast::Node::CRC64INT: return new HashedHitter(mLogger, node.mName);
    case ast::Node::CRC64STR: return new HashedHitter(mLogger, node.mName);
    case ast::Node::HEX: return new OrderingHitter(mLogger, node.mName);
    case ast::Node::SHIFT_TO_UINT64: return new OrderingHitter(mLogger, node.mName);
    case ast::Node::CONCAT: return new ConcatHitter(mLogger, node);
    }
    OTS_ASSERT(mLogger, false);
    return NULL;
}


Exceptional StaticallyIndexedImpl::BuildQueryPipelineOnScanningPrimaryTable(
    deque<shared_ptr<Runnable> >* run,
    shared_ptr<RowToJsonVector>* final,
    vector<Json::Value>* out,
    bool* quickQuit,
    const string& tracker,
    const TableMeta& schema,
    const QloMap<string, Json::Value>& condition,
    const Json::Value& order,
    const Json::Value& projection,
    int64_t start,
    int64_t limit)
{
    deque<shared_ptr<RowObservable> > requesters;
    {
        deque<GetRangeRequest> getRanges;
        deque<GetRowRequest> getRows;
        OTS_TRY(BuildQueryRequests(
                &getRanges, &getRows, tracker, schema, condition));
        FOREACH_ITER(i, getRows) {
            shared_ptr<GetRowRequester> req(
                new GetRowRequester(mLogger, mBulkExecutor.get(), *i, quickQuit));
            run->push_back(req);
            requesters.push_back(req);
        }
        FOREACH_ITER(i, getRanges) {
            shared_ptr<GetRangeRequester> req(
                new GetRangeRequester(mLogger, mBulkExecutor.get(), *i, quickQuit));
            run->push_back(req);
            requesters.push_back(req);
        }
    }
    shared_ptr<Observable<shared_ptr<AssociatedRow>, Exceptional> > leave;
    shared_ptr<Observer<shared_ptr<AssociatedRow>, Exceptional> > enter;
    shared_ptr<Latcher> latcher(new Latcher(mLogger));
    leave = latcher;
    enter = latcher;

    FOREACH_ITER(i, condition) {
        const string& name = i->first;
        const Json::Value& matcher = i->second;
        OTS_ASSERT(mLogger, !matcher.isArray());
        if (!matcher.isObject()) {
            Value val(mLogger);
            Unjsonize(mLogger, &val, matcher);
            shared_ptr<ExactMatcher> m(new ExactMatcher(mLogger, name, val));
            m->ObserveBy(enter);
            enter = m;
            continue;
        }

        const vector<string>& ops = matcher.getMemberNames();
        FOREACH_ITER(i, ops) {
            const string& op = *i;
            if (ToSlice(op) == ToSlice("$in")) {
                shared_ptr<InsideMatcher> m(
                    new InsideMatcher(mLogger, name, matcher[op]));
                m->ObserveBy(enter);
                enter = m;
                continue;
            }

            Value val(mLogger);
            Unjsonize(mLogger, &val, matcher[op]);
            if (ToSlice(op) == ToSlice("$eq")) {
                shared_ptr<ExactMatcher> m(
                    new ExactMatcher(mLogger, name, val));
                m->ObserveBy(enter);
                enter = m;
                continue;
            } else {
                shared_ptr<RangeMatcher> m(
                    new RangeMatcher(mLogger, name, val, op));
                m->ObserveBy(enter);
                enter = m;
            }
        }
    }

    if (!order.empty()) {
        const vector<string>& xs = order.getMemberNames();
        if (xs.size() != 1) {
            return Exceptional(
                "So far exactly one sort key is accepted.");
        }
        const string& field = xs[0];
        const Json::Value& ord = order[field];
        if (!ord.isInt()) {
            return Exceptional("Ordering must be either 1 or -1");
        }
        int o = ord.asInt();
        if (o != 1 && o != -1) {
            return Exceptional("Ordering must be either 1 or -1");
        }
        shared_ptr<Sorter> sorter(new Sorter(mLogger, field, o));
        leave->ObserveBy(sorter);
        leave = sorter;
    }

    if (start != 0 || limit != 0) {
        shared_ptr<SliceWindow> window(new SliceWindow(mLogger, quickQuit, start, limit));
        leave->ObserveBy(window);
        leave = window;
    }

    *final = shared_ptr<RowToJsonVector>(
        new RowToJsonVector(mLogger, out, schema, projection));
    leave->ObserveBy(*final);
    FOREACH_ITER(i, requesters) {
        (*i)->ObserveBy(enter);
    }

    return Exceptional();
}

Exceptional StaticallyIndexedImpl::BuildQueryPipelineOnScanningIndex(
    deque<shared_ptr<Runnable> >* run,
    shared_ptr<RowToJsonVector>* final,
    vector<Json::Value>* out,
    bool* quickQuit,
    const string& tracker,
    const TableMeta& primary,
    const TableMeta& index,
    const QloMap<string, Json::Value>& condition,
    const Json::Value& order,
    const Json::Value& projection,
    int64_t start,
    int64_t limit)
{
    deque<shared_ptr<RowObservable> > requesters;
    {
        deque<GetRangeRequest> getRanges;
        deque<GetRowRequest> getRows;
        OTS_TRY(BuildQueryRequests(
                &getRanges, &getRows, tracker, index, condition));
        FOREACH_ITER(i, getRows) {
            shared_ptr<GetRowRequester> req(
                new GetRowRequester(mLogger, mBulkExecutor.get(), *i, quickQuit));
            run->push_back(req);
            requesters.push_back(req);
        }
        FOREACH_ITER(i, getRanges) {
            shared_ptr<GetRangeRequester> req(
                new GetRangeRequester(mLogger, mBulkExecutor.get(), *i, quickQuit));
            run->push_back(req);
            requesters.push_back(req);
        }
    }
    shared_ptr<IndexRowToPrimaryRequester> indexRow(
        new IndexRowToPrimaryRequester(
            mLogger,
            mBulkExecutor.get(),
            quickQuit,
            Tracker(mRandom.get(), tracker),
            primary));
    FOREACH_ITER(i, requesters) {
        (*i)->ObserveBy(indexRow);
    }
    shared_ptr<Observable<shared_ptr<AssociatedRow>, Exceptional> > leave;
    shared_ptr<Observer<shared_ptr<AssociatedRow>, Exceptional> > enter;
    shared_ptr<Latcher> latcher(new Latcher(mLogger));
    leave = latcher;
    enter = latcher;

    FOREACH_ITER(i, condition) {
        const string& name = i->first;
        const Json::Value& matcher = i->second;
        OTS_ASSERT(mLogger, !matcher.isArray());
        if (!matcher.isObject()) {
            Value val(mLogger);
            Unjsonize(mLogger, &val, matcher);
            shared_ptr<ExactMatcher> m(new ExactMatcher(mLogger, name, val));
            m->ObserveBy(enter);
            enter = m;
            continue;
        }

        const vector<string>& ops = matcher.getMemberNames();
        FOREACH_ITER(i, ops) {
            const string& op = *i;
            if (ToSlice(op) == ToSlice("$in")) {
                shared_ptr<InsideMatcher> m(
                    new InsideMatcher(mLogger, name, matcher[op]));
                m->ObserveBy(enter);
                enter = m;
                continue;
            }

            Value val(mLogger);
            Unjsonize(mLogger, &val, matcher[op]);
            if (ToSlice(op) == ToSlice("$eq")) {
                shared_ptr<ExactMatcher> m(
                    new ExactMatcher(mLogger, name, val));
                m->ObserveBy(enter);
                enter = m;
                continue;
            } else {
                shared_ptr<RangeMatcher> m(
                    new RangeMatcher(mLogger, name, val, op));
                m->ObserveBy(enter);
                enter = m;
            }
        }
    }

    if (!order.empty()) {
        const vector<string>& xs = order.getMemberNames();
        if (xs.size() != 1) {
            return Exceptional("So far exactly one sort key is accepted.");
        }
        const string& field = xs[0];
        const Json::Value& ord = order[field];
        if (!ord.isInt()) {
            return Exceptional("Ordering must be either 1 or -1");
        }
        int o = ord.asInt();
        if (o != 1 && o != -1) {
            return Exceptional("Ordering must be either 1 or -1");
        }
        shared_ptr<Sorter> sorter(new Sorter(mLogger, field, o));
        leave->ObserveBy(sorter);
        leave = sorter;
    }

    if (start != 0 || limit != 0) {
        shared_ptr<SliceWindow> window(new SliceWindow(mLogger, quickQuit, start, limit));
        leave->ObserveBy(window);
        leave = window;
    }

    *final = shared_ptr<RowToJsonVector>(
        new RowToJsonVector(mLogger, out, primary, projection));
    leave->ObserveBy(*final);
    indexRow->ObserveBy(enter);

    return Exceptional();
}

Exceptional StaticallyIndexedImpl::BuildBoundaryFlow(
    const shared_ptr<LimitObservable>& enter,
    const shared_ptr<LimitObserver>& leave,
    const TableMeta& schema)
{
    FOREACH_ITER(i, schema.mPrimaryKey) {
        const shared_ptr<ast::Node>& root = *i;
        if (root->mNodeType == ast::Node::ATTR) {
            shared_ptr<PickAttrLimiter> pick(
                new PickAttrLimiter(mLogger, root->mName));
            enter->ObserveBy(pick);
            pick->ObserveBy(leave);
        } else {
            deque<shared_ptr<LimitObservableObserver> > heads;
            shared_ptr<LimitObservableObserver> tail;
            OTS_TRY(BuildProcessers<LimitObservableObserver>(
                    &heads, &tail, root,
                    bind(&StaticallyIndexedImpl::FindFactory, this, _1)));

            FOREACH_ITER(i, heads) {
                enter->ObserveBy(*i);
            }
            tail->ObserveBy(leave);
        }
    }
    return Exceptional();
}

LimitObservableObserver* StaticallyIndexedImpl::FindFactory(const ast::Node& node)
{
    switch(node.mNodeType) {
    case ast::Node::ATTR: return new PickAttrLimiter(mLogger, node.mName);
    case ast::Node::CRC64INT: return new Crc64Limiter(mLogger, node.mName);
    case ast::Node::CRC64STR: return new Crc64Limiter(mLogger, node.mName);
    case ast::Node::HEX: return new HexLimiter(mLogger, node.mName);
    case ast::Node::SHIFT_TO_UINT64: return new ShiftToUint64Limiter(mLogger, node.mName);
    case ast::Node::CONCAT: return new ConcatLimiter(mLogger, node);
    }
    OTS_ASSERT(mLogger, false);
    return NULL;
}

void CalcAtoms(
    QloSet<string>* atoms,
    const ast::Node& root)
{
    if (root.mNodeType == ast::Node::ATTR) {
        atoms->insert(root.mName);
    } else {
        FOREACH_ITER(i, root.mChildren) {
            CalcAtoms(atoms, **i);
        }
    }
}

Exceptional CalcFixedAtoms(
    Logger* logger,
    ColumnValues* out,
    const QloSet<string>& atoms,
    const QloMap<string, Json::Value>& condition)
{
    FOREACH_ITER(i, condition) {
        const string& name = i->first;
        if (atoms.find(name) == atoms.end()) {
            continue;
        }
        const Json::Value& jCond = i->second;
        if (!jCond.isObject()) {
            Value v(logger);
            Unjsonize(logger, &v, jCond);
            (*out)[name].insert(v);
        } else {
            const vector<string>& ops = jCond.getMemberNames();
            FOREACH_ITER(i, ops) {
                const string& op = *i;
                if (ToSlice(op) == ToSlice("$eq")) {
                    Value v(logger);
                    Unjsonize(logger, &v, jCond[op]);
                    (*out)[name].insert(v);
                } else if (ToSlice(op) == ToSlice("$in")) {
                    const Json::Value& jOptions = jCond[op];
                    if (!jOptions.isArray()) {
                        OTS_LOG_ERROR(logger)
                            (name)
                            (ToString(jCond));
                        OTS_TRY(Exceptional("Value to $in must be an array")) ;
                    }
                    for(int i = 0, sz = jOptions.size(); i < sz; ++i) {
                        const Json::Value& jVal = jOptions[i];
                        Value v(logger);
                        Unjsonize(logger, &v, jVal);
                        (*out)[name].insert(v);
                    }
                } else {
                    OTS_LOG_DEBUG(logger)
                        (name)
                        (op)
                        .What("operator which is not of point query.");
                }
            }
        }
    }
    return Exceptional();
}

bool IsFixed(const ColumnValues& atoms, const ast::Node& root)
{
    if (root.mNodeType == ast::Node::ATTR) {
        return atoms.find(root.mName) != atoms.end();
    } else {
        FOREACH_ITER(i, root.mChildren) {
            bool r = IsFixed(atoms, **i);
            if (!r) {
                return false;
            }
        }
        return true;
    }
}

bool IsPrimaryKeyFixed(const TableMeta& tmeta, const ColumnValues& atoms)
{
    FOREACH_ITER(i, tmeta.mPrimaryKey) {
        bool r = IsFixed(atoms, **i);
        if (!r) {
            return false;
        }
    }
    return true;
}

Exceptional StaticallyIndexedImpl::BuildQueryRequests(
    deque<GetRangeRequest>* getRanges,
    deque<GetRowRequest>* getRows,
    const string& tracker,
    const TableMeta& tmeta,
    const QloMap<string, Json::Value>& condition)
{
    QloSet<string> atomNames;
    FOREACH_ITER(i, tmeta.mPrimaryKey) {
        CalcAtoms(&atomNames, **i);
    }
    ColumnValues atoms;
    OTS_TRY(CalcFixedAtoms(mLogger, &atoms, atomNames, condition));

    if (IsPrimaryKeyFixed(tmeta, atoms)) {
        OTS_LOG_DEBUG(mLogger)(tmeta.mTableName).What("Point query");
        QloMap<string, Value> atomValues;
        OTS_TRY(BuildGetRowRequests(
            getRows, &atomValues,
            tracker, tmeta, atoms, string()));
        return Exceptional();
    } else {
        OTS_LOG_DEBUG(mLogger)(tmeta.mTableName).What("Range query");

        shared_ptr<LimitAggregator> la(new LimitAggregator(mLogger));
        shared_ptr<ConditionIssuer> ci(new ConditionIssuer(mLogger));
        OTS_TRY(BuildBoundaryFlow(ci, la, tmeta));
        ci->Go(condition);
        OTS_TRY(la->Except());

        QloMap<string, Value> atomValues;
        OTS_TRY(BuildGetRangeRequests(
            getRanges, &atomValues,
            tracker, tmeta, la->GetBoundaries(), atoms, string()));
        return Exceptional();
    }
    return Exceptional();
}

Exceptional Eval(
    Logger* logger,
    Value* out,
    const QloMap<string, Value>& atoms,
    const string& rootName,
    const ast::Node& cur)
{
    switch(cur.mNodeType) {
    case ast::Node::ATTR: {
        QloMap<string, Value>::const_iterator i = atoms.find(cur.mName);
        if (i == atoms.end()) {
            OTS_LOG_INFO(logger)
                (rootName)
                (cur.mName)
                .What("expression is unevaluable because of missing atoms");
            return Exceptional("unevaluable expression");
        } else {
            *out = i->second;
        }
        break;
    }
    case ast::Node::CRC64INT: {
        Value sub(logger);
        OTS_ASSERT(logger, cur.mChildren.size() == 1)(cur.mChildren.size());
        OTS_TRY(Eval(logger, &sub, atoms, rootName, *(cur.mChildren[0])));
        uint64_t x = sub.Int();
        uint64_t y = Crc64Int(x);
        out->SetInt(y);
        break;
    }
    case ast::Node::CRC64STR: {
        Value sub(logger);
        OTS_ASSERT(logger, cur.mChildren.size() == 1)(cur.mChildren.size());
        OTS_TRY(Eval(logger, &sub, atoms, rootName, *(cur.mChildren[0])));
        string x = sub.Str();
        uint64_t y = Crc64Str(ToSlice(x));
        out->SetInt(y);
        break;
    }
    case ast::Node::HEX: {
        Value sub(logger);
        OTS_ASSERT(logger, cur.mChildren.size() == 1)(cur.mChildren.size());
        OTS_TRY(Eval(logger, &sub, atoms, rootName, *(cur.mChildren[0])));
        uint64_t x = sub.Int();
        string y = Hex(x);
        out->SetStr(y);
        break;
    }
    case ast::Node::SHIFT_TO_UINT64: {
        Value sub(logger);
        OTS_ASSERT(logger, cur.mChildren.size() == 1)(cur.mChildren.size());
        OTS_TRY(Eval(logger, &sub, atoms, rootName, *(cur.mChildren[0])));
        int64_t x = sub.Int();
        uint64_t y = ShiftToUint64(x);
        out->SetInt(y);
        break;
    }
    case ast::Node::CONCAT: {
        string res;
        FOREACH_ITER(i, cur.mChildren) {
            if (i != cur.mChildren.begin()) {
                res.push_back('|');
            }
            Value v(logger);
            OTS_TRY(Eval(logger, &v, atoms, rootName, **i));
            res.append(v.Str());
        }
        out->SetStr(res);
        break;
    }
    }
    return Exceptional();
}

Exceptional StaticallyIndexedImpl::BuildGetRowRequests(
    deque<GetRowRequest>* results,
    QloMap<string, Value>* atomValues,
    const string& tracker,
    const TableMeta& tmeta,
    const ColumnValues& atoms,
    const string& lastColumn)
{
    ColumnValues::const_iterator it = atoms.upper_bound(lastColumn);
    if (it == atoms.end()) {
        OTS_LOG_INFO(mLogger)
            .What("a new GetRow");
        // a new GetRowRequest
        GetRowRequest req;
        req.mTracker = Tracker(mRandom.get(), tracker);
        req.mTableName = tmeta.mTableName;
        FOREACH_ITER(i, tmeta.mPrimaryKey) {
            const ast::Node& root = **i;
            Column c(mLogger);
            c.mName = root.mName;
            OTS_TRY(Eval(mLogger, &(c.mValue), *atomValues, root.mName, root));
            req.mPrimaryKey.push_back(c);
        }
        results->push_back(req);
    } else {
        const string& name = it->first;
        const ValueSet& opts = it->second;
        FOREACH_ITER(i, opts) {
            const Value& v = *i;
            OTS_LOG_INFO(mLogger)
                (lastColumn)
                (name)
                (ToString(Jsonize(mLogger, v)))
                .What("build GetRow");
            atomValues->insert(make_pair(name, v));
            OTS_TRY(BuildGetRowRequests(
                    results, atomValues, tracker, tmeta, atoms, name));
            atomValues->erase(name);
        }
    }
    return Exceptional();
}

void PurifyLower(deque<Column>* lower)
{
    deque<Column>::iterator i = lower->begin();
    for(; i != lower->end() && !i->mValue.IsInfMin(); ++i) {
    }
    if (i == lower->end()) {
        return;
    }
    for(++i; i != lower->end(); ++i) {
        i->mValue.SetInfMin();
    }
}
void PurifyUpper(deque<Column>* upper)
{
    deque<Column>::iterator i = upper->begin();
    for(; i != upper->end() && !i->mValue.IsInfMax(); ++i) {
    }
    if (i == upper->end()) {
        upper->back().mValue = upper->back().mValue.Next();
    } else {
        for(++i; i != upper->end(); ++i) {
            i->mValue.SetInfMax();
        }
    }
}

Exceptional StaticallyIndexedImpl::BuildGetRangeRequests(
    deque<GetRangeRequest>* results,
    QloMap<string, Value>* atomValues,
    const string& tracker,
    const TableMeta& tmeta,
    const LimitAggregator::ColumnBoundaries& boundaries,
    const ColumnValues& atoms,
    const string& lastColumn)
{
    ColumnValues::const_iterator it = atoms.upper_bound(lastColumn);
    if (it == atoms.end()) {
        // a new GetRangeRequest
        GetRangeRequest req;
        req.mTracker = tracker;
        req.mTableName = tmeta.mTableName;
        req.mLimit = sOTS_StaticIndex_GetRangeLimit;
        FOREACH_ITER(i, tmeta.mPrimaryKey) {
            const ast::Node& root = **i;

            // try point query
            Value v(mLogger);
            Exceptional ex = Eval(mLogger, &v, *atomValues, root.mName, root);
            if (ex.GetCode() == Exceptional::OTS_OK) {
                Column c(mLogger);
                c.mName = root.mName;
                c.mValue = v;
                req.mStartPkey.push_back(c);
                req.mStopPkey.push_back(c);
            } else {
                Column lower(mLogger);
                lower.mName = root.mName;
                Column upper(mLogger);
                upper.mName = root.mName;
                LimitAggregator::ColumnBoundaries::const_iterator bit =
                    boundaries.find(root.mName);
                if (bit == boundaries.end()) {
                    lower.mValue = Value(mLogger, Value::InfMin());
                    upper.mValue = Value(mLogger, Value::InfMax());
                } else {
                    lower.mValue = bit->second.mLower;
                    upper.mValue = bit->second.mUpper;
                }
                req.mStartPkey.push_back(lower);
                req.mStopPkey.push_back(upper);
            }
        }
        PurifyLower(&req.mStartPkey);
        PurifyUpper(&req.mStopPkey);
        results->push_back(req);
    } else {
        const string& name = it->first;
        const ValueSet& opts = it->second;
        FOREACH_ITER(i, opts) {
            const Value& v = *i;
            OTS_LOG_INFO(mLogger)
                (lastColumn)
                (name)
                (ToString(Jsonize(mLogger, v)))
                .What("build GetRow");
            atomValues->insert(make_pair(name, v));
            OTS_TRY(BuildGetRangeRequests(
                    results, atomValues, tracker, tmeta, boundaries, atoms, name));
            atomValues->erase(name);
        }
    }
    return Exceptional();
}

int StaticallyIndexedImpl::Delete(
    const string& collection,
    const Json::Value& cond)
{
    OTS_FICUS_TRY(mExcept, "error on initialization");

    Json::Value projection = Json::arrayValue;
    Json::Value order = Json::objectValue;
    vector<Json::Value> rows;
    int rtn = Find(collection, projection, cond, 0, 0, order, rows);
    if (rtn != FICUS_SUCC) {
        return rtn;
    }

    map<string, CollectionMeta>::const_iterator it = mSchema.find(collection);
    if (it == mSchema.end()) {
        OTS_FICUS_TRY(
            Exceptional("unknown collection: " + collection),
            "error on insertion");
    }
    const CollectionMeta& schema = it->second;
    string track = Tracker(mRandom.get(), "Delete");
    OTS_LOG_DEBUG(mLogger)(collection)(track)(UtcTime::Now(mLogger).ToISO8601())
        .What("Start deleting");

    deque<DeleteRowRequest> primaryReqs;
    deque<DeleteRowRequest> indexReqs;
    FOREACH_ITER(i, rows) {
        OTS_FICUS_TRY(
            _Delete(&indexReqs, &primaryReqs, track, schema, *i),
            "error on deleting");
    }

    deque<shared_ptr<Future> > futures;
    deque<Exceptional> excepts;
    FOREACH_ITER(i, primaryReqs) {
        excepts.push_back(Exceptional());
        futures.push_back(mBulkExecutor->DeleteRow(&(excepts.back()), *i));
    }
    for(; !futures.empty(); futures.pop_back()) {
        futures.back()->Wait();
    }
    for(; !excepts.empty(); excepts.pop_back()) {
        OTS_FICUS_TRY(
            excepts.back(),
            "error on deleting");
    }
    FOREACH_ITER(i, indexReqs) {
        excepts.push_back(Exceptional());
        futures.push_back(mBulkExecutor->DeleteRow(&(excepts.back()), *i));
    }
    for(; !futures.empty(); futures.pop_back()) {
        futures.back()->Wait();
    }
    for(; !excepts.empty(); excepts.pop_back()) {
        OTS_FICUS_TRY(
            excepts.back(),
            "error on deleting");
    }

    OTS_LOG_DEBUG(mLogger)
        (collection)
        (track)
        (UtcTime::Now(mLogger).ToISO8601())
        .What("Succeed deleting");
    return FICUS_SUCC;
}

Exceptional StaticallyIndexedImpl::_Delete(
    deque<DeleteRowRequest>* indexReqs,
    deque<DeleteRowRequest>* primaryReqs,
    const string& track,
    const CollectionMeta& cmeta,
    const Json::Value& data)
{
    shared_ptr<JsonValueToColumn> init(new JsonValueToColumn(mLogger));

    shared_ptr<DeleteRowRequestFiller> primary;
    deque<shared_ptr<DeleteRowRequestFiller> > indexes;
    {
        const TableMeta& tmeta = cmeta.mTableMetas.at(cmeta.mPrimaryTable);
        string mytrack = Tracker(mRandom.get(), track);
        primary.reset(
            new DeleteRowRequestFiller(mLogger, primaryReqs, tmeta, mytrack));
        OTS_TRY(BuildDeleteTableFlow(init, primary, tmeta));
    }

    FOREACH_ITER(i, cmeta.mIndexes) {
        const TableMeta& tmeta = cmeta.mTableMetas.at(*i);
        string mytrack = Tracker(mRandom.get(), track);
        shared_ptr<DeleteRowRequestFiller> reqFiller(
            new DeleteRowRequestFiller(mLogger, indexReqs, tmeta, mytrack));
        OTS_TRY(BuildDeleteTableFlow(init, reqFiller, tmeta));
        indexes.push_back(reqFiller);
    }

    init->Go(track, data);
    OTS_TRY(primary->GetExcept());
    FOREACH_ITER(i, indexes) {
        OTS_TRY((*i)->GetExcept());
    }

    return Exceptional();
}

Exceptional StaticallyIndexedImpl::BuildDeleteTableFlow(
    const shared_ptr<ColumnObservable>& enter,
    const shared_ptr<DeleteRowRequestFiller>& reqFiller,
    const TableMeta& schema)
{
    FOREACH_ITER(i, schema.mPrimaryKey) {
        const shared_ptr<ast::Node>& root = *i;
        deque<shared_ptr<ColumnObservableObserver> > heads;
        shared_ptr<ColumnObservableObserver> tail;
        OTS_TRY(BuildProcessers<ColumnObservableObserver>(
                &heads, &tail, root,
                bind(&InsertFactory, mLogger, _1)));

        FOREACH_ITER(i, heads) {
            enter->ObserveBy(*i);
        }
        if (tail) {
            tail->ObserveBy(reqFiller);
        }
    }
    return Exceptional();
}

Exceptional ValidateCondition(Logger* logger, const Json::Value& cond)
{
    string condStr = ToString(cond);
    const vector<string>& members = cond.getMemberNames();
    FOREACH_ITER(i, members) {
        const string& field = *i;
        const Json::Value& val = cond[field];
        if (val.isObject() || val.isArray() || val.isNull()) {
            OTS_LOG_ERROR(logger)
                (condStr)
                .What("only primitive values are allowed in conditions in Update/Upsert.");
            return Exceptional("only primitive values are allowed in conditions in Update/Upsert");
        }
    }
    return Exceptional();
}

bool IsReplace(const vector<string>& fields)
{
    FOREACH_ITER(i, fields) {
        if (ToSlice(*i) == ToSlice("$set")) {
            return false;
        }
        if (ToSlice(*i) == ToSlice("$unset")) {
            return false;
        }
    }
    return true;
}

Exceptional MongoMerge(Logger* logger, Json::Value& old, const Json::Value& newData)
{
    const vector<string>& fields = newData.getMemberNames();
    if (IsReplace(fields)) {
        old = newData;
        return Exceptional();
    } else {
        old.removeMember("_track");
        FOREACH_ITER(i, fields) {
            const string& field = *i;
            const Json::Value& newVal = newData[field];
            if (ToSlice(*i) == ToSlice("$set")) {
                if (!newVal.isObject()) {
                    OTS_LOG_ERROR(logger)
                        (ToString(newData))
                        .What("invalid syntax in $set");
                    return Exceptional("invalid syntax in $set");
                } else {
                    const vector<string>& xs = newVal.getMemberNames();
                    FOREACH_ITER(j, xs) {
                        const string& x = *j;
                        old[x] = newVal[x];
                    }
                }
            } else if (ToSlice(*i) == ToSlice("$unset")) {
                if (!newVal.isObject()) {
                    OTS_LOG_ERROR(logger)
                        (ToString(newData))
                        .What("invalid syntax in $unset");
                    return Exceptional("invalid syntax in $unset");
                } else {
                    const vector<string>& xs = newVal.getMemberNames();
                    FOREACH_ITER(j, xs) {
                        const string& x = *j;
                        old.removeMember(x);
                    }
                }
            } else {
                OTS_LOG_ERROR(logger)
                    (*i)
                    .What("unsupported modifiers in updating");
                return Exceptional("unsupported modifiers in updating");
            }
        }
        return Exceptional();
    }
}

Exceptional Merge(Logger* logger, Json::Value& oldVal, const Json::Value& newVal)
{
    const vector<string>& fields = newVal.getMemberNames();
    FOREACH_ITER(i, fields) {
        const string& field = *i;
        const Json::Value& nval = newVal[field];
        if (nval.isNull() || nval.isObject() || nval.isArray()) {
            OTS_LOG_ERROR(logger)
                (ToString(newVal))
                .What("unsupported value");
            return Exceptional("unsupported value");
        }
        oldVal[field] = nval;
    }
    return Exceptional();
}

int StaticallyIndexedImpl::Update(
    const string& collection,
    const Json::Value& condition,
    const Json::Value& newData)
{
    OTS_FICUS_TRY(mExcept, "error on initialization");
    OTS_FICUS_TRY(
        ValidateCondition(mLogger, condition),
        "error on updating");
    
    Json::Value projection = Json::arrayValue;
    Json::Value order = Json::objectValue;
    vector<Json::Value> rows;
    int rtn = Find(collection, projection, condition, 0, 0, order, rows);
    if (rtn != FICUS_SUCC) {
        return rtn;
    }
    FOREACH_ITER(i, rows) {
        Json::Value dat = *i;
        OTS_FICUS_TRY(
            MongoMerge(mLogger, dat, newData),
            "error on updating");
        rtn = Delete(collection, condition);
        if (rtn != FICUS_SUCC) {
            return rtn;
        }
        rtn = Insert(collection, dat);
        if (rtn != FICUS_SUCC) {
            return rtn;
        }
    }
    return FICUS_SUCC;
}

int StaticallyIndexedImpl::Upsert(
    const string& collection,
    const Json::Value& condition,
    const Json::Value& newData)
{
    OTS_FICUS_TRY(mExcept, "error on initialization");
    OTS_FICUS_TRY(
        ValidateCondition(mLogger, condition),
        "error on upserting");
    OTS_LOG_DEBUG(mLogger)
        (collection)
        (UtcTime::Now(mLogger).ToISO8601())
        .What("Start upserting");
    
    Json::Value projection = Json::arrayValue;
    Json::Value order = Json::objectValue;
    vector<Json::Value> rows;
    int rtn = Find(collection, projection, condition, 0, 0, order, rows);
    if (rtn != FICUS_SUCC) {
        OTS_LOG_ERROR(mLogger)
            (collection)
            (ToString(condition))
            (ToString(rtn))
            .What("error on finding in upserting");
        return rtn;
    }
    if (rows.empty()) {
        OTS_LOG_DEBUG(mLogger)
            (collection)
            (UtcTime::Now(mLogger).ToISO8601())
            .What("To Insert");
        Json::Value dat = newData;
        OTS_FICUS_TRY(
            Merge(mLogger, dat, condition),
            "error on upserting");
        int rtn = Insert(collection, dat);
        if (rtn != FICUS_SUCC) {
            return rtn;
        }
    } else if (rows.size() == 1) {
        OTS_LOG_DEBUG(mLogger)
            (collection)
            (UtcTime::Now(mLogger).ToISO8601())
            .What("To update");
        Json::Value dat = rows[0];
        OTS_FICUS_TRY(
            MongoMerge(mLogger, dat, newData),
            "error on upserting");
        int rtn = Delete(collection, condition);
        if (rtn != FICUS_SUCC) {
            return rtn;
        }
        rtn = Insert(collection, dat);
        if (rtn != FICUS_SUCC) {
            return rtn;
        }
    } else {
        OTS_LOG_ERROR(mLogger)
            (collection)
            (ToString(rows.size()))
            (ToString(condition))
            .What("too many rows match in upserting");
        return FICUS_OBJECT_STORAGE_OPERATION_ERROR;
    }
    OTS_LOG_DEBUG(mLogger)
        (collection)
        (UtcTime::Now(mLogger).ToISO8601())
        .What("Succeed upserting");
    return FICUS_SUCC;
}

} // namespace

StaticallyIndexed* StaticallyIndexed::New(
    const Settings& settings,
    ClientDelegate* client)
{
    return new StaticallyIndexedImpl(settings, client);
}

} // namespace static_index

#undef OBS_TRY
