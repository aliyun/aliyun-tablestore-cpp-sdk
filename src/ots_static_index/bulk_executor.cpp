#include "bulk_executor.h"
#include "alarm_clock.h"
#include "security.h"
#include "threading.h"
#include "timestamp.h"
#include "slice.h"
#include "random.h"
#include "logging_assert.h"
#include "foreach.h"
#include "ots_static_index/client_delegate.h"
#include "ots_static_index/thread_pool.h"
#include "ots_static_index/logger.h"
#include <tr1/unordered_set>

using namespace ::std;
using namespace ::std::tr1;

namespace static_index {

int64_t BulkExecutor::sBatchWriteLimit = 100;
int64_t BulkExecutor::sBatchGetRowLimit = 10;
int64_t BulkExecutor::sBgLaucherInterval = 5000; // 5ms

BulkExecutor::BulkExecutor(
    Logger* logger,
    ThreadPool* tpool,
    IRandom* rnd,
    AlarmClock* alarm,
    ClientDelegate* client)
  : mLogger(logger),
    mThreadPool(tpool),
    mRandom(rnd),
    mClient(client),
    mAlarmClock(alarm),
    mStop(false),
    mBgLauncherSem(new Semaphore(logger, 0)),
    mMutex(logger),
    mOnGoingExecutors(0)
{
    mBgLauncherThread.reset(
        Thread::New(mLogger, bind(&BulkExecutor::BgLauncher, this)));
}

BulkExecutor::~BulkExecutor()
{
    mStop = true;
    mBgLauncherSem->Post();
    mBgLauncherThread->Join();

    for(;;) {
        int64_t cnt = __atomic_load_8(&mOnGoingExecutors, __ATOMIC_ACQUIRE);
        OTS_LOG_DEBUG(mLogger)(cnt);
        if (cnt == 0) {
            break;
        } else {
            SleepFor(Interval::FromMsec(mLogger, 1));
        }
    }
}

namespace {

bool ShouldRetry(const Exceptional& ex)
{
    if (ex.GetCode() == Exceptional::OTS_OK) {
        return false;
    }
    const string& errCode = ex.GetErrorCode();
    if (ToSlice(errCode) == ToSlice("OTSNetworkError")) {
        return true;
    } else if (ToSlice(errCode) == ToSlice("OTSRequestTimeout")) {
        return true;
    } else if (ToSlice(errCode) == ToSlice("OTSInternalServerError")) {
        return true;
    } else if (ToSlice(errCode) == ToSlice("OTSRowOperationConflict")) {
        return true;
    } else if (ToSlice(errCode) == ToSlice("OTSTableNotReady")) {
        return true;
    } else if (ToSlice(errCode) == ToSlice("OTSQuotaExhausted")) {
        return true;
    } else if (ToSlice(errCode) == ToSlice("OTSServerBusy")) {
        return true;
    } else if (ToSlice(errCode) == ToSlice("OTSPartitionUnavailable")) {
        return true;
    } else if (ToSlice(errCode) == ToSlice("OTSTimeout")) {
        return true;
    } else if (ToSlice(errCode) == ToSlice("OTSServerUnavailable")) {
        return true;
    } else if (ToSlice(errCode) == ToSlice("OTSCapacityUnitExhausted")) {
        return true;
    } else {
        return false;
    }
}

class ScopedDecrement
{
    int64_t* mCnt;
public:
    explicit ScopedDecrement(int64_t* cnt)
      : mCnt(cnt)
    {}
    
    ~ScopedDecrement()
    {
        __atomic_sub_fetch(mCnt, 1, __ATOMIC_ACQ_REL);
    }
};

} // namespace

void BulkExecutor::ExecuteWrite()
{
    deque<WriteHandler> rows;
    PopWrites(&rows);
    ScopedDecrement g(&mOnGoingExecutors);
    if (rows.empty()) {
        return;
    }

    string track("BatchWrite_");
    FillUuid(mRandom, &track);
    OTS_LOG_DEBUG(mLogger)(track).What("a new BatchWrite");
    BatchWriteRequest bwReq;
    Into(&bwReq, rows, track);
    BatchWriteResponse bwResp;
    Exceptional ex = mClient->BatchWrite(&bwResp, bwReq);
    OTS_LOG_DEBUG(mLogger)
        ((int) ex.GetCode())
        (ex.GetErrorMessage())
        (ex.GetRequestId());
    if (ex.GetCode() == Exceptional::OTS_OK) {
        FOREACH_ITER(i, bwResp.mPutRows) {
            DealWithSingleWrite(track, *i);
        }
        FOREACH_ITER(i, bwResp.mDelRows) {
            DealWithSingleWrite(track, *i);
        }
    } else if (ShouldRetry(ex)) {
        OTS_LOG_INFO(mLogger)
            (track)
            ((int) ex.GetCode())
            (ex.GetErrorMessage())
            (ex.GetRequestId())
            .What("Retry entire bulk");
        Scoped<Mutex> g(&mMutex);
        for(; !rows.empty(); rows.pop_back()) {
            mWriteHandlers.push_front(rows.back());
        }
    } else {
        OTS_ASSERT(mLogger, false)
            ((int) ex.GetCode())
            (ex.GetErrorCode())
            (ex.GetErrorMessage())
            (ex.GetRequestId())
            .What("Unrecoverable error");
    }
}

void BulkExecutor::DealWithSingleWrite(
    const string& tracker,
    const tuple<Exceptional, const void*>& resp)
{
    Exceptional ex = get<0>(resp);
    WriteHandler& hdl = *static_cast<WriteHandler*>(
        const_cast<void*>(get<1>(resp)));
    *(hdl.mExcept) = ex;
    if (ex.GetCode() == Exceptional::OTS_OK) {
        OTS_LOG_DEBUG(mLogger)
            (tracker)
            (GetRequestTracker(hdl))
            .What("Successful");
        hdl.mSem->Post();
    } else if (ShouldRetry(ex)) {
        OTS_LOG_INFO(mLogger)
            (tracker)
            (GetRequestTracker(hdl))
            (ex.GetErrorMessage())
            (ex.GetRequestId())
            .What("Retry a single write");
        Scoped<Mutex> g(&mMutex);
        mWriteHandlers.push_front(hdl);
    } else {
        OTS_LOG_ERROR(mLogger)
            (tracker)
            (GetRequestTracker(hdl))
            (ex.GetErrorMessage())
            (ex.GetRequestId())
            .What("Unretryable fault");
        hdl.mSem->Post();
    }
}

void BulkExecutor::PopWrites(deque<WriteHandler>* rows)
{
    unordered_set<size_t> hashs;
    ColumnsHasher colHasher;
    hash<string> tblHasher;
    Scoped<Mutex> g(&mMutex);
    for(;
        !mWriteHandlers.empty() && static_cast<int64_t>(rows->size()) < sBatchWriteLimit;
        mWriteHandlers.pop_front())
    {
        const WriteHandler& row = mWriteHandlers.front();
        size_t h = 0;
        if (row.mPutRow.get() != NULL) {
            h = tblHasher(row.mPutRow->mTableName) ^ colHasher(row.mPutRow->mPrimaryKey);
        } else if (row.mDelRow.get() != NULL) {
            h = tblHasher(row.mDelRow->mTableName) ^ colHasher(row.mDelRow->mPrimaryKey);
        } else {
            OTS_ASSERT(mLogger, false).What("no request in WriteHandler");
        }
        bool r = hashs.insert(h).second;
        if (!r) {
            OTS_LOG_DEBUG(mLogger)
                (rows->size())
                .What("Duplicated rows");
            break;
        }
        rows->push_back(row);
    }
}

void BulkExecutor::Into(
    BatchWriteRequest* req,
    const deque<WriteHandler>& rows,
    const string& track)
{
    req->mTracker = track;
    FOREACH_ITER(i, rows) {
        const WriteHandler& hdl = *i;
        if (hdl.mPutRow.get() != NULL) {
            OTS_LOG_DEBUG(mLogger)(track)(hdl.mPutRow->mTracker);
            req->mPutRows.push_back(make_tuple(*(hdl.mPutRow), &hdl));
        } else if (hdl.mDelRow.get() != NULL) {
            OTS_LOG_DEBUG(mLogger)(track)(hdl.mDelRow->mTracker);
            req->mDelRows.push_back(make_tuple(*(hdl.mDelRow), &hdl));
        } else {
            OTS_ASSERT(mLogger, false).What("no request in WriteHandler");
        }
    }
}

BulkExecutor::WriteHandler::WriteHandler(
    Logger* logger,
    Exceptional* exc,
    const PutRowRequest& req)
  : mPutRow(new PutRowRequest(req)),
    mExcept(exc),
    mSem(new Semaphore(logger, 0))
{}

BulkExecutor::WriteHandler::WriteHandler(
    Logger* logger,
    Exceptional* exc,
    const DeleteRowRequest& req)
  : mDelRow(new DeleteRowRequest(req)),
    mExcept(exc),
    mSem(new Semaphore(logger, 0))
{}

shared_ptr<Future> BulkExecutor::PutRow(Exceptional* exc, const PutRowRequest& req)
{
    WriteHandler hdl(mLogger, exc, req);
    {
        Scoped<Mutex> g(&mMutex);
        mWriteHandlers.push_back(hdl);
    }
    return hdl.mSem;
}

shared_ptr<Future> BulkExecutor::DeleteRow(Exceptional* exc, const DeleteRowRequest& req)
{
    WriteHandler hdl(mLogger, exc, req);
    {
        Scoped<Mutex> g(&mMutex);
        mWriteHandlers.push_back(hdl);
    }
    return hdl.mSem;
}


namespace {

void IssueGetRange(
    ClientDelegate* client,
    Exceptional* exc,
    GetRangeResponse* resp,
    const GetRangeRequest& req,
    const shared_ptr<Semaphore>& sem)
{
    *exc = client->GetRange(resp, req);
    sem->Post();
}

}

BulkExecutor::GetRangeHandler::GetRangeHandler(
    BulkExecutor* executor,
    Exceptional* exc,
    GetRangeResponse* resp,
    const GetRangeRequest& req)
  : mExecutor(executor),
    mRequest(req),
    mResponse(resp),
    mExcept(exc),
    mSem(new Semaphore(executor->mLogger, 0))
{
    function<void()> fn = bind(IssueGetRange,
        mExecutor->mClient, mExcept, mResponse, mRequest, mSem);
    bool r = mExecutor->mThreadPool->TryEnqueue(fn);
    if (!r) {
        OTS_LOG_ERROR(mExecutor->mLogger)
            (mExecutor->mThreadPool->CountItems())
            .What("Too busy to add a GetRange");
        Interval wait = Interval::FromUsec(
            mExecutor->mLogger,
            NextInt(mExecutor->mRandom, 5000, 20000));
        mExecutor->mAlarmClock->AddRelatively(wait, fn);
    }
}

bool BulkExecutor::GetRangeHandler::TryWait()
{
    if (!mSem->TryWait()) {
        return false;
    }
    if (!ShouldRetry(*mExcept)) {
        if (mExcept->GetCode() == Exceptional::OTS_OK) {
            OTS_LOG_DEBUG(mExecutor->mLogger)
                (ToString(Jsonize(mExecutor->mLogger, mRequest)))
                (ToString(Jsonize(mExecutor->mLogger, *mResponse)));
        } else {
            OTS_LOG_ERROR(mExecutor->mLogger)
                (ToString(Jsonize(mExecutor->mLogger, mRequest)))
                (mExcept->GetErrorMessage())
                (mExcept->GetRequestId());
        }
        return true;
    }
    OTS_LOG_DEBUG(mExecutor->mLogger)
        (ToString(Jsonize(mExecutor->mLogger, mRequest)))
        .What("retry");
    function<void()> fn = bind(IssueGetRange,
        mExecutor->mClient, mExcept, mResponse, mRequest, mSem);
    Interval wait = Interval::FromUsec(
        mExecutor->mLogger,
        NextInt(mExecutor->mRandom, 5000, 20000));
    mExecutor->mAlarmClock->AddRelatively(wait, fn);
    return false;
}

bool BulkExecutor::GetRangeHandler::Wait(const Interval& dur)
{
    if (!mSem->Wait(dur)) {
        return false;
    }
    if (!ShouldRetry(*mExcept)) {
        if (mExcept->GetCode() == Exceptional::OTS_OK) {
            OTS_LOG_DEBUG(mExecutor->mLogger)
                (ToString(Jsonize(mExecutor->mLogger, mRequest)))
                (ToString(Jsonize(mExecutor->mLogger, *mResponse)));
        } else {
            OTS_LOG_ERROR(mExecutor->mLogger)
                (ToString(Jsonize(mExecutor->mLogger, mRequest)))
                (mExcept->GetErrorMessage())
                (mExcept->GetRequestId());
        }
        return true;
    }
    OTS_LOG_DEBUG(mExecutor->mLogger)
        (ToString(Jsonize(mExecutor->mLogger, mRequest)))
        .What("retry");
    function<void()> fn = bind(IssueGetRange,
        mExecutor->mClient, mExcept, mResponse, mRequest, mSem);
    Interval wait = Interval::FromUsec(
        mExecutor->mLogger,
        NextInt(mExecutor->mRandom, 5000, 20000));
    mExecutor->mAlarmClock->AddRelatively(wait, fn);
    return false;
}

void BulkExecutor::GetRangeHandler::Wait()
{
    for(;;) {
        mSem->Wait();
        if (!ShouldRetry(*mExcept)) {
            if (mExcept->GetCode() == Exceptional::OTS_OK) {
                OTS_LOG_DEBUG(mExecutor->mLogger)
                    (ToString(Jsonize(mExecutor->mLogger, mRequest)))
                    (ToString(Jsonize(mExecutor->mLogger, *mResponse)));
            } else {
                OTS_LOG_ERROR(mExecutor->mLogger)
                    (ToString(Jsonize(mExecutor->mLogger, mRequest)))
                    (mExcept->GetErrorMessage());
            }
            return;
        }
        OTS_LOG_DEBUG(mExecutor->mLogger)
            (ToString(Jsonize(mExecutor->mLogger, mRequest)))
            .What("retry");
        function<void()> fn = bind(IssueGetRange,
            mExecutor->mClient, mExcept, mResponse, mRequest, mSem);
        Interval wait = Interval::FromUsec(
            mExecutor->mLogger,
            NextInt(mExecutor->mRandom, 5000, 20000));
        mExecutor->mAlarmClock->AddRelatively(wait, fn);
    }
}

shared_ptr<Future> BulkExecutor::GetRange(
    Exceptional* exc,
    GetRangeResponse* resp,
    const GetRangeRequest& req)
{
    shared_ptr<Future> res(new GetRangeHandler(this, exc, resp, req));
    return res;
}

BulkExecutor::GetRowHandler::GetRowHandler(
    Logger* logger,
    Exceptional* exc,
    GetRowResponse* resp,
    const GetRowRequest& req)
  : mRequest(req),
    mResponse(resp),
    mExcept(exc),
    mSem(new Semaphore(logger, 0))
{}

shared_ptr<Future> BulkExecutor::GetRow(
    Exceptional* exc,
    GetRowResponse* resp,
    const GetRowRequest& req)
{
    GetRowHandler hdl(mLogger, exc, resp, req);
    {
        Scoped<Mutex> g(&mMutex);
        mReadHandlers.push_back(hdl);
    }
    return hdl.mSem;
}

void BulkExecutor::PopReads(deque<GetRowHandler>* rows)
{
    unordered_set<size_t> hashs;
    ColumnsHasher colHasher;
    hash<string> tblHasher;
    Scoped<Mutex> g(&mMutex);
    for(; !mReadHandlers.empty() && static_cast<int64_t>(rows->size()) < sBatchGetRowLimit;
        mReadHandlers.pop_front())
    {
        const GetRowHandler& row = mReadHandlers.front();
        size_t h = tblHasher(row.mRequest.mTableName) ^ colHasher(row.mRequest.mPrimaryKey);
        bool r = hashs.insert(h).second;
        if (!r) {
            break;
        }
        rows->push_back(row);
    }
}

void BulkExecutor::ExecuteRead()
{
    deque<GetRowHandler> rows;
    PopReads(&rows);
    ScopedDecrement g(&mOnGoingExecutors);
    if (rows.empty()) {
        return;
    }

    string track("BatchGetRow_");
    FillUuid(mRandom, &track);
    OTS_LOG_DEBUG(mLogger)(track).What("a new BatchGetRow");
    BatchGetRowRequest bwReq;
    Into(&bwReq, rows, track);
    BatchGetRowResponse bwResp;
    Exceptional ex = mClient->BatchGetRow(&bwResp, bwReq);
    OTS_LOG_DEBUG(mLogger)
        ((int) ex.GetCode())
        (ex.GetErrorMessage())
        (ex.GetRequestId());
    if (ex.GetCode() == Exceptional::OTS_OK) {
        FOREACH_ITER(i, bwResp.mGetRows) {
            Exceptional ex = get<0>(*i);
            GetRowHandler& hdl = *static_cast<GetRowHandler*>(
                const_cast<void*>(get<2>(*i)));
            *hdl.mExcept = ex;
            if (ex.GetCode() == Exceptional::OTS_OK) {
                OTS_LOG_INFO(mLogger)(track)(hdl.mRequest.mTracker).What("Successful");
                *hdl.mResponse = get<1>(*i);
                hdl.mResponse->mTracker = hdl.mRequest.mTracker;
                OTS_LOG_DEBUG(mLogger)(track)
                    (ToString(Jsonize(mLogger, hdl.mRequest)))
                    (ToString(Jsonize(mLogger, *hdl.mResponse)));
                hdl.mSem->Post();
            } else if (ShouldRetry(ex)) {
                OTS_LOG_INFO(mLogger)
                    (track)
                    (hdl.mRequest.mTracker)
                    (ex.GetErrorMessage())
                    (ex.GetRequestId())
                    .What("Retry a single GetRow");
                Scoped<Mutex> g(&mMutex);
                mReadHandlers.push_front(hdl);
            } else {
                OTS_LOG_ERROR(mLogger)
                    (track)
                    (hdl.mRequest.mTracker)
                    (ex.GetErrorMessage())
                    (ex.GetRequestId())
                    .What("Unretryable fault");
                hdl.mSem->Post();
            }
        }
    } else if (ShouldRetry(ex)) {
        OTS_LOG_INFO(mLogger)
            (track)
            ((int) ex.GetCode())
            (ex.GetErrorMessage())
            (ex.GetRequestId())
            .What("Retry entire bulk");
        Scoped<Mutex> g(&mMutex);
        for(; !rows.empty(); rows.pop_back()) {
            mReadHandlers.push_front(rows.back());
        }
    } else {
        OTS_ASSERT(mLogger, false)
            ((int) ex.GetCode())
            (ex.GetErrorMessage())
            (ex.GetRequestId())
            .What("Unrecoverable error");
    }
}

void BulkExecutor::Into(
    BatchGetRowRequest* req,
    const deque<GetRowHandler>& rows,
    const string& tracker)
{
    req->mTracker = tracker;
    FOREACH_ITER(i, rows) {
        const GetRowHandler& hdl = *i;
        OTS_LOG_DEBUG(mLogger)(tracker)(hdl.mRequest.mTracker);
        req->mGetRows.push_back(make_tuple(hdl.mRequest, &hdl));
    }
}

void BulkExecutor::BgLauncher()
{
    Interval intv = Interval::FromUsec(mLogger, sBgLaucherInterval);
    ThreadPool::Closure write = bind(&BulkExecutor::ExecuteWrite, this);
    ThreadPool::Closure read = bind(&BulkExecutor::ExecuteRead, this);
    for(;;) {
        mBgLauncherSem->Wait(intv);
        if (mStop) {
            return;
        }
        {
            bool r = mThreadPool->TryEnqueue(write);
            if (!r) {
                OTS_LOG_ERROR(mLogger).What("Too busy to add a BatchWrite.");
            } else {
                __atomic_add_fetch(&mOnGoingExecutors, 1, __ATOMIC_ACQ_REL);
            }
        }
        {
            bool r = mThreadPool->TryEnqueue(read);
            if (!r) {
                OTS_LOG_ERROR(mLogger).What("Too busy to add a BatchGetRow.");
            } else {
                __atomic_add_fetch(&mOnGoingExecutors, 1, __ATOMIC_ACQ_REL);
            }
        }
    }
}

string BulkExecutor::GetRequestTracker(const WriteHandler& hdl)
{
    if (hdl.mPutRow.get() != NULL) {
        return hdl.mPutRow->mTracker;
    } else if (hdl.mDelRow.get() != NULL) {
        return hdl.mDelRow->mTracker;
    } else {
        return string();
    }
}
} // namespace static_index

