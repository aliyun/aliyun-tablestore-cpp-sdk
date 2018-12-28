#include "async_batch_writer.hpp"
#include "tablestore/core/retry.hpp"
#include "tablestore/util/try.hpp"
#include "tablestore/util/logging.hpp"
#include "tablestore/util/security.hpp"
#include <boost/ref.hpp>
#include <tr1/unordered_set>
#include <tr1/tuple>
#include <limits>

using namespace std;
using namespace std::tr1;
using namespace std::tr1::placeholders;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {

namespace util {
namespace impl {

template<>
struct Adler32Updater<uint16_t, void>
{
    void operator()(Adler32& adl, uint16_t x) const
    {
        adl.update(static_cast<uint8_t>(x >> 8));
        adl.update(static_cast<uint8_t>(x));
    }
};

template<>
struct Adler32Updater<uint32_t, void>
{
    void operator()(Adler32& adl, uint32_t x) const
    {
        update(adl, static_cast<uint16_t>(x >> 16));
        update(adl, static_cast<uint16_t>(x));
    }
};

template<>
struct Adler32Updater<uint64_t, void>
{
    void operator()(Adler32& adl, uint64_t x) const
    {
        update(adl, static_cast<uint32_t>(x >> 32));
        update(adl, static_cast<uint32_t>(x));
    }
};

template<>
struct Adler32Updater<string, void>
{
    void operator()(Adler32& adl, const string& x) const
    {
        MemPiece p = MemPiece::from(x);
        update(adl, static_cast<uint64_t>(p.length()));
        update(adl, p);
    }
};

} // namespace
} // namespace

namespace core {
namespace impl {

namespace {

void update(Adler32& adl, const PrimaryKeyValue& val, uint64_t& autoIncrPkeyCnt)
{
    switch(val.category()) {
    case PrimaryKeyValue::kNone: case PrimaryKeyValue::kInfMin: case PrimaryKeyValue::kInfMax:
        OTS_ASSERT(false);
        break;
    case PrimaryKeyValue::kAutoIncr: {
        adl.update(static_cast<uint8_t>(PrimaryKeyValue::kAutoIncr));
        update(adl, autoIncrPkeyCnt);
        ++autoIncrPkeyCnt;
        break;
    }
    case PrimaryKeyValue::kInteger: {
        adl.update(static_cast<uint8_t>(PrimaryKeyValue::kInteger));
        update(adl, static_cast<uint64_t>(val.integer()));
        break;
    }
    case PrimaryKeyValue::kString: {
        adl.update(static_cast<uint8_t>(PrimaryKeyValue::kString));
        update(adl, val.str());
        break;
    }
    case PrimaryKeyValue::kBinary: {
        adl.update(static_cast<uint8_t>(PrimaryKeyValue::kBinary));
        update(adl, val.blob());
        break;
    }
    }
}

uint32_t hash(uint64_t& autoIncrPkeyCnt, const MemPiece& table, const PrimaryKey& pk)
{
    Adler32 adl;
    update(adl, table);
    for(int64_t i = 0, sz = pk.size(); i < sz; ++i) {
        const PrimaryKeyColumn& c = pk[i];
        const PrimaryKeyValue& v = c.value();
        update(adl, v, autoIncrPkeyCnt);
    }
    return adl.get();
}

tuple<MemPiece, const PrimaryKey&> getTableAndPkey(
    const AsyncBatchWriter::Item& item)
{
    switch(item.mType) {
    case AsyncBatchWriter::Item::kInvalid: {
        OTS_ASSERT(false);
        break;
    }
    case AsyncBatchWriter::Item::kPutRow: {
        return tuple<MemPiece, const PrimaryKey&>(
            MemPiece::from(item.mRowPutChange.table()),
            item.mRowPutChange.primaryKey());
    }
    case AsyncBatchWriter::Item::kUpdateRow: {
        return tuple<MemPiece, const PrimaryKey&>(
            MemPiece::from(item.mRowUpdateChange.table()),
            item.mRowUpdateChange.primaryKey());
    }
    case AsyncBatchWriter::Item::kDeleteRow: {
        return tuple<MemPiece, const PrimaryKey&>(
            MemPiece::from(item.mRowDeleteChange.table()),
            item.mRowDeleteChange.primaryKey());
    }
    }
    OTS_ASSERT(false);
    PrimaryKey dummy;
    return tuple<MemPiece, const PrimaryKey&>(MemPiece(), dummy);
}

} // namespace

typedef AsyncBatchWriter::PutRowCallback PutRowCallback;
typedef AsyncBatchWriter::UpdateRowCallback UpdateRowCallback;
typedef AsyncBatchWriter::DeleteRowCallback DeleteRowCallback;

AsyncBatchWriter::AsyncBatchWriter(
    AsyncClient& client,
    const BatchWriterConfig& cfg)
  : mLogger(client.mutableLogger().spawn("BatchWriter")),
    mClient(client),
    mMaxConcurrency(cfg.maxConcurrency()),
    mMaxBatchSize(cfg.maxBatchSize()),
    mRegularNap(cfg.regularNap()),
    mMaxNap(cfg.maxNap()),
    mNapShrinkStep(cfg.napShrinkStep()),
    mAggregateSem(0),
    mExit(false),
    mOngoingRequests(0),
    mRng(random::newDefault()),
    mShouldBackoff(false),
    mActorSelector(0)
{
    if (cfg.actors().present()) {
        mActors = *cfg.actors();
    } else {
        for(int64_t i = 0; i < sDefaultActors; ++i) {
            mActors.push_back(shared_ptr<Actor>(new Actor()));
        }
    }

    Thread t(bind(&AsyncBatchWriter::aggregator, this));
    moveAssign(mAggregateThread, util::move(t));
}

AsyncBatchWriter::~AsyncBatchWriter()
{
    OTS_LOG_INFO(*mLogger)
        ("What", "BatchWriter is quiting.");
    mExit.store(true, boost::memory_order_release);
    mAggregateSem.post();
    mAggregateThread.join();

    for(;;) {
        int64_t ongoing = mOngoingRequests.load(boost::memory_order_acquire);
        OTS_ASSERT(ongoing >= 0)
            (ongoing);
        if (ongoing == 0) {
            break;
        }
        OTS_LOG_DEBUG(*mLogger)
            ("OngoingRequests", ongoing);
        sleepFor(Duration::fromMsec(20));
    }

    OTS_LOG_INFO(*mLogger)
        ("What", "BatchWriter has been quited.");
}

int64_t AsyncBatchWriter::sConcurrencyIncStep = 1;
int64_t AsyncBatchWriter::sDefaultActors = 32;

void AsyncBatchWriter::aggregator()
{
    Duration nap = mRegularNap;
    int64_t concurrency = mMaxConcurrency;
    for(;;) {
        takeSomeNap(nap);
        if (mExit.load(boost::memory_order_acquire)) {
            break;
        }
        tie(nap, concurrency) = nextNapAndConcurrency(
            mShouldBackoff,
            concurrency,
            nap);
        send(concurrency);
    }
    OTS_LOG_DEBUG(*mLogger)
        ("What", "AsyncBatchWriter::aggregator() is quitting.");
}

void AsyncBatchWriter::takeSomeNap(util::Duration upper)
{
    int64_t n = upper.toUsec();
    int64_t m = random::nextInt(*mRng, n / 2 + 1, n + 1);
    Semaphore::Status s = mAggregateSem.waitFor(Duration::fromUsec(m));
    (void) s;
}

tuple<Duration, int64_t> AsyncBatchWriter::nextNapAndConcurrency(
    boost::atomic<bool>& atomicBackoff,
    int64_t curConcur,
    Duration curNap)
{
    bool backoff = atomicBackoff.exchange(false, boost::memory_order_acq_rel);
    if (!backoff) {
        if (curNap - mNapShrinkStep >= mRegularNap) {
            return make_tuple(curNap - mNapShrinkStep, 1);
        } else {
            return make_tuple(
                mRegularNap,
                std::min(
                    curConcur + sConcurrencyIncStep,
                    mMaxConcurrency));
        }
    } else {
        if (curConcur > 1) {
            return make_tuple(curNap, curConcur / 2);
        } else {
            return make_tuple(std::min(curNap * 2, mMaxNap), 1);
        }
    }
}

void AsyncBatchWriter::send(int64_t concurrency)
{
    int64_t watermark = mOngoingRequests.load(boost::memory_order_acquire);
    int64_t incr = 0;
    for(; incr < concurrency - watermark; ++incr) {
        BatchWriteRowRequest batchReq;
        auto_ptr<CallbackCarrier> carrier(new CallbackCarrier());
        int64_t remains = 0;
        int64_t num = 0;
        batch(batchReq, *carrier, remains, num);

        if (num == 0) {
            break;
        }
        if (num == mMaxBatchSize || remains == 0) {
            OTS_LOG_DEBUG(*mLogger)
                ("What", "Sending a batch of writes")
                ("BatchSize", num)
                ("Remains", remains);
        } else {
            OTS_LOG_INFO(*mLogger)
                ("What", "Sending a batch of writes")
                ("BatchSize", num)
                ("Remains", remains);
        }
        mClient.batchWriteRow(
            batchReq,
            bind(&AsyncBatchWriter::callbackOnBatch,
                this, carrier.release(), _1, _2, _3));
    }
    if (incr > 0) {
        mOngoingRequests.fetch_add(incr, boost::memory_order_acq_rel);
    }
}

namespace {

void itemToBatchRequest(
    BatchWriteRowRequest& batchReq,
    AsyncBatchWriter::CallbackCarrier& carrier,
    AsyncBatchWriter::Item& item)
{
    switch(item.mType) {
    case AsyncBatchWriter::Item::kInvalid:
        OTS_ASSERT(false);
        break;
    case AsyncBatchWriter::Item::kPutRow: {
        carrier.mPutCallbacks.push_back(item.mPutRowCallback);
        BatchWriteRowRequest::Put& chg = batchReq
            .mutablePuts().append();
        moveAssign(chg.mutableGet(), util::move(item.mRowPutChange));
        break;
    }
    case AsyncBatchWriter::Item::kUpdateRow: {
        carrier.mUpdateCallbacks.push_back(item.mUpdateRowCallback);
        BatchWriteRowRequest::Update& chg = batchReq
            .mutableUpdates().append();
        moveAssign(chg.mutableGet(), util::move(item.mRowUpdateChange));
        break;
    }
    case AsyncBatchWriter::Item::kDeleteRow: {
        carrier.mDeleteCallbacks.push_back(item.mDeleteRowCallback);
        BatchWriteRowRequest::Delete& chg = batchReq
            .mutableDeletes().append();
        moveAssign(chg.mutableGet(), util::move(item.mRowDeleteChange));
        break;
    }
    }
}

} // namespace

void AsyncBatchWriter::batch(
    BatchWriteRowRequest& batchReq,
    CallbackCarrier& carrier,
    int64_t& remains,
    int64_t& num)
{
    unordered_set<uint32_t> conflicts;
    uint64_t autoIncPkeyCnt = 0;
    ScopedLock g(mMutex);
    for(; num < mMaxBatchSize && !mWaitingList.empty(); ++num) {
        tuple<MemPiece, const PrimaryKey&> r = getTableAndPkey(mWaitingList.front());
        MemPiece table = get<0>(r);
        const PrimaryKey& pk = get<1>(r);
        uint32_t h = hash(autoIncPkeyCnt, table, pk);
        if (conflicts.count(h)) {
            break;
        }
        conflicts.insert(h);

        Item item;
        moveAssign(item, util::move(mWaitingList.front()));
        mWaitingList.pop_front();
        itemToBatchRequest(batchReq, carrier, item);
    }
    remains = mWaitingList.size();
}

namespace {

template<class T>
struct RequestTraits
{};

template<>
struct RequestTraits<BatchWriteRowRequest::Put>
{
    IVector<BatchWriteRowResponse::Result>& results(BatchWriteRowResponse& resp)
    {
        return resp.mutablePutResults();
    }
    IVector<BatchWriteRowRequest::Put>& requests(BatchWriteRowRequest& req)
    {
        return req.mutablePuts();
    }
    deque<PutRowCallback>& callbacks(AsyncBatchWriter::CallbackCarrier& carrier)
    {
        return carrier.mPutCallbacks;
    }
};
template<>
struct RequestTraits<BatchWriteRowRequest::Update>
{
    IVector<BatchWriteRowResponse::Result>& results(BatchWriteRowResponse& resp)
    {
        return resp.mutableUpdateResults();
    }
    IVector<BatchWriteRowRequest::Update>& requests(BatchWriteRowRequest& req)
    {
        return req.mutableUpdates();
    }
    deque<UpdateRowCallback>& callbacks(AsyncBatchWriter::CallbackCarrier& carrier)
    {
        return carrier.mUpdateCallbacks;
    }
};
template<>
struct RequestTraits<BatchWriteRowRequest::Delete>
{
    IVector<BatchWriteRowResponse::Result>& results(BatchWriteRowResponse& resp)
    {
        return resp.mutableDeleteResults();
    }
    IVector<BatchWriteRowRequest::Delete>& requests(BatchWriteRowRequest& req)
    {
        return req.mutableDeletes();
    }
    deque<DeleteRowCallback>& callbacks(AsyncBatchWriter::CallbackCarrier& carrier)
    {
        return carrier.mDeleteCallbacks;
    }
};


template<class T>
void addItem(
    deque<AsyncBatchWriter::Item>& items,
    typename WriteTraits<T>::SingleRowChange& chg,
    typename WriteTraits<T>::Callback& cb)
{
    AsyncBatchWriter::Item item(chg, cb);
    items.push_back(AsyncBatchWriter::Item());
    moveAssign(items.back(), util::move(item));
}

template<class T>
void _turnToItem(
    deque<AsyncBatchWriter::Item>& items,
    AsyncBatchWriter::CallbackCarrier& carrier,
    BatchWriteRowRequest& req)
{
    typedef typename WriteTraits<T>::TypeInBatchWriteRequest Subtype;
    typedef typename WriteTraits<T>::Callback Callback;
    RequestTraits<T> reqTraits;
    
    IVector<Subtype>& causes = reqTraits.requests(req);
    deque<Callback>& callbacks = reqTraits.callbacks(carrier);
    OTS_ASSERT(causes.size() == static_cast<int64_t>(callbacks.size()))
        (causes.size())
        (callbacks.size());
    for(int64_t i = 0, sz = causes.size(); i < sz; ++i) {
        Subtype& cause = causes[i];
        Callback& callback = callbacks[i];
        addItem<Subtype>(items, cause.mutableGet(), callback);
    }
}

void turnToItems(
    deque<AsyncBatchWriter::Item>& items,
    AsyncBatchWriter::CallbackCarrier& carrier,
    BatchWriteRowRequest& req)
{
    _turnToItem<BatchWriteRowRequest::Put>(items, carrier, req);
    _turnToItem<BatchWriteRowRequest::Update>(items, carrier, req);
    _turnToItem<BatchWriteRowRequest::Delete>(items, carrier, req);
}

template<class Request>
void callbackWithContext(AsyncBatchWriter::Context<Request>* ctx)
{
    OTS_ASSERT(ctx != NULL);
    auto_ptr<AsyncBatchWriter::Context<Request> > c(ctx);
    c->callback()(c->mutableRequest(), c->mutableError(), c->mutableResponse());
}

} // namespace

void AsyncBatchWriter::callbackOnBatch(
    CallbackCarrier* car,
    BatchWriteRowRequest& req,
    Optional<OTSError>& err,
    BatchWriteRowResponse& resp)
{
    auto_ptr<CallbackCarrier> carrier(car);
    if (err.present()) {
        RetryStrategy::RetryCategory retriable = RetryStrategy::retriable(*err);
        switch(retriable) {
        case RetryStrategy::UNRETRIABLE: {
            OTS_LOG_INFO(*mLogger)
                ("What", "An unretriable error occurs in sending a BatchWriteRowRequest")
                ("Request", req)
                ("Error", *err);
            feedbackAllError<PutRowRequest>(*car, req, *err);
            feedbackAllError<UpdateRowRequest>(*car, req, *err);
            feedbackAllError<DeleteRowRequest>(*car, req, *err);
            break;
        }
        case RetryStrategy::RETRIABLE: case RetryStrategy::DEPENDS: {
            OTS_LOG_INFO(*mLogger)
                ("What", "A retriable error occurs in sending a BatchWriteRowRequest")
                ("Request", req)
                ("Error", *err);
            mShouldBackoff.store(true, boost::memory_order_release);
            waitAgain(*carrier, req);
            break;
        }
        }
    } else {
        deque<Item> items;
        feedbackFromBatchReq<PutRowRequest>(items, *carrier, req, resp);
        feedbackFromBatchReq<UpdateRowRequest>(items, *carrier, req, resp);
        feedbackFromBatchReq<DeleteRowRequest>(items, *carrier, req, resp);

        if (!items.empty()) {
            OTS_LOG_DEBUG(*mLogger)
                ("What", "Retries in a batch")
                ("Size", items.size());
            mShouldBackoff.store(true, boost::memory_order_release);
            prependWaitingList(items);
        }
    }

    mOngoingRequests.fetch_sub(1, boost::memory_order_acq_rel);
}

template<class Request>
void AsyncBatchWriter::feedbackAllError(
    AsyncBatchWriter::CallbackCarrier& carrier,
    BatchWriteRowRequest& req,
    const OTSError& err)
{
    typedef typename WriteTraits<Request>::TypeInBatchWriteRequest Subtype;
    typedef typename WriteTraits<Request>::Callback Callback;
    RequestTraits<Subtype> reqTraits;
    
    IVector<Subtype>& causes = reqTraits.requests(req);
    deque<Callback>& callbacks = reqTraits.callbacks(carrier);
    OTS_ASSERT(causes.size() == static_cast<int64_t>(callbacks.size()))
        (causes.size())
        (callbacks.size());
    for(int64_t i = 0, sz = causes.size(); i < sz; ++i) {
        Subtype& cause = causes[i];
        auto_ptr<Context<Request> > ctx(new Context<Request>());
        ctx->mutableCallback() = callbacks[i];
        moveAssign(ctx->mutableRequest().mutableRowChange(), util::move(cause.mutableGet()));
        ctx->mutableError().reset(err);
        triggerCallback(
            bind(callbackWithContext<Request>,
                ctx.release()));
    }
}


template<class Request>
void AsyncBatchWriter::feedbackFromBatchReq(
    deque<Item>& items,
    CallbackCarrier& carrier,
    BatchWriteRowRequest& req,
    BatchWriteRowResponse& resp)
{
    typedef typename WriteTraits<Request>::TypeInBatchWriteRequest Subtype;
    typedef typename WriteTraits<Request>::Callback Callback;
    RequestTraits<Subtype> reqTraits;

    IVector<Subtype>& causes = reqTraits.requests(req);
    IVector<BatchWriteRowResponse::Result>& results = reqTraits.results(resp);
    deque<Callback>& callbacks = reqTraits.callbacks(carrier);
    OTS_ASSERT(causes.size() == results.size())
        (causes.size())
        (results.size());
    OTS_ASSERT(causes.size() == static_cast<int64_t>(callbacks.size()))
        (causes.size())
        (callbacks.size());
    for(int64_t i = 0, sz = results.size(); i < sz; ++i) {
        Subtype& cause = causes[i];
        Callback& callback = callbacks[i];
        Result<util::Optional<Row>, OTSError>& result = results[i].mutableGet();
        if (result.ok()) {
            feedbackOkRequest<Request>(
                cause.mutableGet(),
                callback,
                result.mutableOkValue(),
                resp.requestId(),
                resp.traceId());
        } else {
            feedbackErrRequest<Request>(
                items,
                cause.mutableGet(),
                callback,
                result.mutableErrValue(),
                resp.requestId(),
                resp.traceId());
        }
    }
}

template<class Request>
void AsyncBatchWriter::feedbackOkRequest(
    typename WriteTraits<Request>::SingleRowChange& chg,
    typename WriteTraits<Request>::Callback& cb,
    Optional<Row>& row,
    const string& requestId,
    const string& traceId)
{
    auto_ptr<Context<Request> > ctx(new Context<Request>());
    ctx->mutableCallback() = cb;
    moveAssign(ctx->mutableRequest().mutableRowChange(), util::move(chg));
    ctx->mutableResponse().mutableRequestId() = requestId;
    ctx->mutableResponse().mutableTraceId() = traceId;
    moveAssign(ctx->mutableResponse().mutableRow(), util::move(row));
    triggerCallback(
        bind(callbackWithContext<Request>,
            ctx.release()));
}

template<class Request>
void AsyncBatchWriter::feedbackErrRequest(
    deque<AsyncBatchWriter::Item>& items,
    typename WriteTraits<Request>::SingleRowChange& chg,
    typename WriteTraits<Request>::Callback& cb,
    OTSError& err,
    const string& requestId,
    const string& traceId)
{
    RetryStrategy::RetryCategory retriable = RetryStrategy::retriable(err);
    switch(retriable) {
    case RetryStrategy::UNRETRIABLE: {
        OTS_LOG_INFO(*mLogger)
            ("RowChange", chg)
            ("Error", err)
            .what("An unretriable single-row error occurs in BatchWriteRowResponse");
        auto_ptr<Context<Request> > ctx(new Context<Request>());
        ctx->mutableCallback() = cb;
        moveAssign(ctx->mutableRequest().mutableRowChange(), util::move(chg));
        ctx->mutableError().reset(util::move(err));
        ctx->mutableError()->mutableRequestId() = requestId;
        ctx->mutableError()->mutableTraceId() = traceId;
        triggerCallback(
            bind(callbackWithContext<Request>,
                ctx.release()));
        break;
    }
    case RetryStrategy::RETRIABLE: case RetryStrategy::DEPENDS: {
        OTS_LOG_INFO(*mLogger)
            ("RowChange", chg)
            ("Error", err)
            .what("A retriable single-row error occurs in BatchWriteRowResponse");
        addItem<Request>(items, chg, cb);
        break;
    }
    }
}

void AsyncBatchWriter::waitAgain(
    CallbackCarrier& carrier,
    BatchWriteRowRequest& req)
{
    deque<Item> items;
    turnToItems(items, carrier, req);
    prependWaitingList(items);
}

void AsyncBatchWriter::prependWaitingList(deque<Item>& items)
{
    ScopedLock g(mMutex);
    for(; !items.empty(); items.pop_back()) {
        mWaitingList.push_front(Item());
        moveAssign(mWaitingList.front(), util::move(items.back()));
    }
}


void AsyncBatchWriter::triggerCallback(const function<void()>& cb)
{
    uint64_t act = mActorSelector.fetch_add(1, boost::memory_order_acq_rel);
    act = act % mActors.size();
    mActors[act]->pushBack(cb);
}

void AsyncBatchWriter::putRow(
    PutRowRequest& req,
    const PutRowCallback& cb)
{
    issue(req, cb);
}

void AsyncBatchWriter::updateRow(
    UpdateRowRequest& req,
    const UpdateRowCallback& cb)
{
    issue(req, cb);
}

void AsyncBatchWriter::deleteRow(
    DeleteRowRequest& req,
    const DeleteRowCallback& cb)
{
    issue(req, cb);
}

template<class Request>
void AsyncBatchWriter::issue(
    Request& req,
    const typename WriteTraits<Request>::Callback& cb)
{
    typedef typename WriteTraits<Request>::Callback Callback;

    if (!cb) {
        OTSError err(OTSError::kPredefined_OTSParameterInvalid);
        err.mutableMessage() = "Callback should not be null.";
        auto_ptr<Context<Request> > ctx(new Context<Request>());
        ctx->mutableCallback() = cb;
        moveAssign(ctx->mutableRequest(), util::move(req));
        ctx->mutableError().reset(util::move(err));
        triggerCallback(
            bind(callbackWithContext<Request>,
                ctx.release()));
        return;
    }

    Optional<OTSError> err = req.validate();
    if (err.present()) {
        PutRowResponse resp;
        auto_ptr<Context<Request> > ctx(new Context<Request>());
        ctx->mutableCallback() = cb;
        moveAssign(ctx->mutableRequest(), util::move(req));
        moveAssign(ctx->mutableError(), util::move(err));
        triggerCallback(
            bind(callbackWithContext<Request>,
                ctx.release()));
        return;
    }

    Callback _cb = cb;
    Item item(req.mutableRowChange(), _cb);
    {
        ScopedLock g(mMutex);
        mWaitingList.push_back(Item());
        moveAssign(mWaitingList.back(), util::move(item));
    }
}

AsyncBatchWriter::Item::Item(RowPutChange& chg, PutRowCallback& cb)
  : mType(kPutRow)
{
    moveAssign(mRowPutChange, util::move(chg));
    moveAssign(mPutRowCallback, util::move(cb));
}

AsyncBatchWriter::Item::Item(RowUpdateChange& chg, UpdateRowCallback& cb)
  : mType(kUpdateRow)
{
    moveAssign(mRowUpdateChange, util::move(chg));
    moveAssign(mUpdateRowCallback, util::move(cb));
}

AsyncBatchWriter::Item::Item(RowDeleteChange& chg, DeleteRowCallback& cb)
  : mType(kDeleteRow)
{
    moveAssign(mRowDeleteChange, util::move(chg));
    moveAssign(mDeleteRowCallback, util::move(cb));
}

inline SingleRowType AsyncBatchWriter::IContext::type() const
{
    return mType;
}

inline SingleRowType& AsyncBatchWriter::IContext::mutableType()
{
    return mType;
}

template<class Request>
AsyncBatchWriter::Context<Request>::Context()
  : IContext(WriteTraits<Request>::kType)
{}

template<class Request>
const typename AsyncBatchWriter::Context<Request>::Callback&
AsyncBatchWriter::Context<Request>::callback() const
{
    return mCallback;
}

template<class Request>
typename AsyncBatchWriter::Context<Request>::Callback&
AsyncBatchWriter::Context<Request>::mutableCallback()
{
    return mCallback;
}

template<class Request>
const Request& AsyncBatchWriter::Context<Request>::request() const
{
    return mRequest;
}

template<class Request>
Request& AsyncBatchWriter::Context<Request>::mutableRequest()
{
    return mRequest;
}

template<class Request>
const util::Optional<OTSError>& AsyncBatchWriter::Context<Request>::error() const
{
    return mError;
}

template<class Request>
util::Optional<OTSError>& AsyncBatchWriter::Context<Request>::mutableError()
{
    return mError;
}

template<class Request>
const typename AsyncBatchWriter::Context<Request>::Response&
AsyncBatchWriter::Context<Request>::response() const
{
    return mResponse;
}

template<class Request>
typename AsyncBatchWriter::Context<Request>::Response&
AsyncBatchWriter::Context<Request>::mutableResponse()
{
    return mResponse;
}

void AsyncBatchWriter::flush()
{
    mAggregateSem.post();
}

} // namespace impl
} // namespace core
} // namespace tablestore
} // namespace aliyun

