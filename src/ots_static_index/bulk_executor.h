#ifndef OTS_STATIC_INDEX_BULK_EXECUTOR_H
#define OTS_STATIC_INDEX_BULK_EXECUTOR_H

#include "type_delegates.h"
#include "threading.h"
#include "logging_assert.h"
#include "ots_static_index/exceptional.h"
#include <tr1/functional>
#include <tr1/memory>
#include <deque>
#include <string>
#include <stdint.h>

namespace static_index {

class Logger;
class Thread;
class ThreadPool;
class IRandom;
class Semaphore;
class ClientDelegate;
class AlarmClock;

class BulkExecutor
{
public:
    typedef ::std::tr1::function<Exceptional ()> Closure;

    static int64_t sBgLaucherInterval; // in usec
    static int64_t sBatchWriteLimit;
    static int64_t sBatchGetRowLimit;

private:
    Logger* mLogger;
    ThreadPool* mThreadPool;
    IRandom* mRandom;
    ClientDelegate* mClient;
    AlarmClock* mAlarmClock;
    bool mStop;
    ::std::tr1::shared_ptr<Semaphore> mBgLauncherSem;
    ::std::auto_ptr<Thread> mBgLauncherThread;
    
    struct WriteHandler
    {
        std::tr1::shared_ptr<PutRowRequest> mPutRow;
        std::tr1::shared_ptr<DeleteRowRequest> mDelRow;
        Exceptional* mExcept;
        ::std::tr1::shared_ptr<Semaphore> mSem;

        explicit WriteHandler(Logger*, Exceptional*, const PutRowRequest&);
        explicit WriteHandler(Logger*, Exceptional*, const DeleteRowRequest&);
    };
    struct GetRowHandler
    {
        GetRowRequest mRequest;
        GetRowResponse* mResponse;
        Exceptional* mExcept;
        ::std::tr1::shared_ptr<Semaphore> mSem;

        explicit GetRowHandler(Logger*, Exceptional*, GetRowResponse*, const GetRowRequest&);
    };
    struct GetRangeHandler: public Future
    {
        BulkExecutor* mExecutor;
        GetRangeRequest mRequest;
        GetRangeResponse* mResponse;
        Exceptional* mExcept;
        ::std::tr1::shared_ptr<Semaphore> mSem;

        explicit GetRangeHandler(
            BulkExecutor*, Exceptional*, GetRangeResponse*, const GetRangeRequest&);
        bool TryWait();
        bool Wait(const Interval&);
        void Wait();
    };

    Mutex mMutex;
    ::std::deque<WriteHandler> mWriteHandlers;
    ::std::deque<GetRowHandler> mReadHandlers;
    int64_t mOnGoingExecutors;

public:
    explicit BulkExecutor(Logger*, ThreadPool*, IRandom*, AlarmClock*, ClientDelegate*);
    ~BulkExecutor();

    ::std::tr1::shared_ptr<Future> PutRow(Exceptional*, const PutRowRequest&);
    ::std::tr1::shared_ptr<Future> DeleteRow(Exceptional*, const DeleteRowRequest&);
    ::std::tr1::shared_ptr<Future> GetRange(
        Exceptional*, GetRangeResponse*, const GetRangeRequest&);
    ::std::tr1::shared_ptr<Future> GetRow(
        Exceptional*, GetRowResponse*, const GetRowRequest&);

private:
    void BgLauncher();
    void ExecuteWrite();
    void PopWrites(::std::deque<WriteHandler>*);
    void ExecuteRead();
    void PopReads(::std::deque<GetRowHandler>*);
    void Into(
        BatchWriteRequest*,
        const ::std::deque<WriteHandler>&,
        const ::std::string& track);
    void Into(
        BatchGetRowRequest*,
        const ::std::deque<GetRowHandler>&,
        const ::std::string& track);
    static std::string GetRequestTracker(const WriteHandler&);
    void DealWithSingleWrite(
        const std::string& tracker,
        const std::tr1::tuple<Exceptional, const void*>&);
};

} // namespace static_index

#endif /* OTS_STATIC_INDEX_BULK_EXECUTOR_H */
