/*
BSD 3-Clause License

Copyright (c) 2017, Alibaba Cloud
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
#include "common.hpp"
#include "src/tablestore/core/impl/async_batch_writer.hpp"
#include "tablestore/core/batch_writer.hpp"
#include "tablestore/util/optional.hpp"
#include "tablestore/util/threading.hpp"
#include "tablestore/util/assert.hpp"
#include "tablestore/util/logging.hpp"
#include "tablestore/util/foreach.hpp"
#include "testa/testa.hpp"
#include <boost/ref.hpp>
#include <tr1/functional>
#include <tr1/memory>
#include <string>
#include <set>

using namespace std;
using namespace std::tr1;
using namespace std::tr1::placeholders;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {

namespace {
SyncBatchWriter* newSyncBatchWriter(
    AsyncClient& client,
    Logger& logger,
    const BatchWriterConfig& cfg)
{
    SyncBatchWriter* w = NULL;
    Optional<OTSError> err = SyncBatchWriter::create(w, client, logger, cfg);
    TESTA_ASSERT(!err.present())
        (*err).issue();
    return w;
}

impl::AsyncBatchWriter* newAsyncBatchWriter(
    AsyncClient& client,
    Logger& logger,
    const BatchWriterConfig& cfg)
{
    AsyncBatchWriter* w = NULL;
    Optional<OTSError> err = AsyncBatchWriter::create(w, client, logger, cfg);
    TESTA_ASSERT(!err.present())
        (*err).issue();
    impl::AsyncBatchWriter* res = dynamic_cast<impl::AsyncBatchWriter*>(w);
    TESTA_ASSERT(res != NULL).issue();
    return res;
}

} // namespace

namespace {

template<typename T>
struct RequestTraits
{};

template<>
struct RequestTraits<BatchWriteRowRequest::Put>
{
    IVector<BatchWriteRowResponse::Result>& results(BatchWriteRowResponse& resp)
    {
        return resp.mutablePutResults();
    }
    const IVector<BatchWriteRowRequest::Put>& requests(const BatchWriteRowRequest& req)
    {
        return req.puts();
    }
};
template<>
struct RequestTraits<BatchWriteRowRequest::Update>
{
    IVector<BatchWriteRowResponse::Result>& results(BatchWriteRowResponse& resp)
    {
        return resp.mutableUpdateResults();
    }
    const IVector<BatchWriteRowRequest::Update>& requests(const BatchWriteRowRequest& req)
    {
        return req.updates();
    }
};
template<>
struct RequestTraits<BatchWriteRowRequest::Delete>
{
    IVector<BatchWriteRowResponse::Result>& results(BatchWriteRowResponse& resp)
    {
        return resp.mutableDeleteResults();
    }
    const IVector<BatchWriteRowRequest::Delete>& requests(const BatchWriteRowRequest& req)
    {
        return req.deletes();
    }
};

template<typename T>
void cloneRowsInRequest(
    BatchWriteRowResponse& resp,
    const BatchWriteRowRequest& req)
{
    RequestTraits<T> traits;
    IVector<BatchWriteRowResponse::Result>& results = traits.results(resp);
    const IVector<T>& requests = traits.requests(req);
    for(int64_t i = 0, sz = requests.size(); i < sz; ++i) {
        const T& cause = requests[i];
        Result<Optional<Row>, OTSError>& result = results.append().mutableGet();
        Row row;
        row.mutablePrimaryKey() = cause.get().primaryKey();
        result.mutableOkValue().reset(row);
    }
}

void SyncBatchWriter_Cb(
    Logger& logger,
    BatchWriteRowRequest& req,
    const function<void(
        BatchWriteRowRequest&, util::Optional<OTSError>&, BatchWriteRowResponse&)>& cb)
{
    BatchWriteRowResponse resp;
    cloneRowsInRequest<BatchWriteRowRequest::Put>(resp, req);
    cloneRowsInRequest<BatchWriteRowRequest::Update>(resp, req);
    cloneRowsInRequest<BatchWriteRowRequest::Delete>(resp, req);
    OTS_LOG_DEBUG(logger)
        ("Request", req)
        ("Response", resp);
    Optional<OTSError> err;
    cb(req, err, resp);
}

template<typename Request>
void fillRequired(Request& req, const string& table, const PrimaryKeyValue& v)
{
    typename impl::WriteTraits<Request>::SingleRowChange& chg = req.mutableRowChange();
    chg.mutableTable() = table;
    PrimaryKey& pkey = chg.mutablePrimaryKey();
    pkey.append() = PrimaryKeyColumn("pkey", v);
}

void SyncBatchWriter_PutRow(const string&)
{
    auto_ptr<Logger> logger(createLogger("/", Logger::kDebug));
    MockAsyncClient client;
    client.mutableBatchWriteRow() =
        bind(SyncBatchWriter_Cb, boost::ref(*logger), _1, _2);
    BatchWriterConfig cfg;
    auto_ptr<SyncBatchWriter> writer(newSyncBatchWriter(client, *logger, cfg));

    PutRowRequest req;
    fillRequired(req, "Table", PrimaryKeyValue::toInteger(0));
    PutRowResponse resp;
    Optional<OTSError> err = writer->putRow(resp, req);

    TESTA_ASSERT(!err.present())
        (*err).issue();
    const Optional<Row>& row = resp.row();
    TESTA_ASSERT(row.present()).issue();
    TESTA_ASSERT(pp::prettyPrint(row->primaryKey()[0].value()) == "0")
        (resp).issue();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(SyncBatchWriter_PutRow);

namespace {
void SyncBatchWriter_UpdateRow(const string&)
{
    auto_ptr<Logger> logger(createLogger("/", Logger::kDebug));
    MockAsyncClient client;
    client.mutableBatchWriteRow() =
        bind(SyncBatchWriter_Cb, boost::ref(*logger), _1, _2);
    BatchWriterConfig cfg;
    auto_ptr<SyncBatchWriter> writer(newSyncBatchWriter(client, *logger, cfg));

    UpdateRowRequest req;
    fillRequired(req, "Table", PrimaryKeyValue::toInteger(0));
    UpdateRowResponse resp;
    Optional<OTSError> err = writer->updateRow(resp, req);

    TESTA_ASSERT(!err.present())
        (*err).issue();
    const Optional<Row>& row = resp.row();
    TESTA_ASSERT(row.present()).issue();
    TESTA_ASSERT(pp::prettyPrint(row->primaryKey()[0].value()) == "0")
        (resp).issue();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(SyncBatchWriter_UpdateRow);

namespace {
void SyncBatchWriter_DeleteRow(const string&)
{
    auto_ptr<Logger> logger(createLogger("/", Logger::kDebug));
    MockAsyncClient client;
    client.mutableBatchWriteRow() =
        bind(SyncBatchWriter_Cb, boost::ref(*logger), _1, _2);
    BatchWriterConfig cfg;
    auto_ptr<SyncBatchWriter> writer(newSyncBatchWriter(client, *logger, cfg));

    DeleteRowRequest req;
    fillRequired(req, "Table", PrimaryKeyValue::toInteger(0));
    DeleteRowResponse resp;
    Optional<OTSError> err = writer->deleteRow(resp, req);

    TESTA_ASSERT(!err.present())
        (*err).issue();
    const Optional<Row>& row = resp.row();
    TESTA_ASSERT(row.present()).issue();
    TESTA_ASSERT(pp::prettyPrint(row->primaryKey()[0].value()) == "0")
        (resp).issue();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(SyncBatchWriter_DeleteRow);

namespace {
template<typename Request, typename Response>
void AsyncBatchWriter_Callback(
    Logger& logger,
    Mutex& mutex,
    deque<PrimaryKeyValue>& receivedRows,
    Request& req,
    Optional<OTSError>& err,
    Response& resp)
{
    TESTA_ASSERT(!err.present())
        (*err).issue();
    const Optional<Row>& row = resp.row();
    TESTA_ASSERT(row.present()).issue();
    const PrimaryKey& pk = row->primaryKey();
    TESTA_ASSERT(pk.size() == 1)
        (pk.size()).issue();
    {
        ScopedLock g(mutex);
        receivedRows.push_back(pk[0].value());
    }
}

void AsyncBatchWriter_Cb(
    Logger& logger,
    boost::atomic<int64_t>& sentReqs,
    BatchWriteRowRequest& req,
    const function<void(
        BatchWriteRowRequest&, util::Optional<OTSError>&, BatchWriteRowResponse&)>& cb)
{
    sentReqs.fetch_add(1, boost::memory_order_acq_rel);
    SyncBatchWriter_Cb(logger, req, cb);
}

void AsyncBatchWriter_Aggregation(const string&)
{
    auto_ptr<Logger> logger(createLogger("/", Logger::kDebug));
    MockAsyncClient client;
    BatchWriterConfig cfg;
    cfg.mutableRegularNap() = Duration::fromHour(1);
    cfg.mutableMaxNap() = cfg.regularNap() * 2;
    auto_ptr<impl::AsyncBatchWriter> writer(newAsyncBatchWriter(client, *logger, cfg));

    Mutex mutex;
    boost::atomic<int64_t> sentReqs(0);
    deque<PrimaryKeyValue> receivedRows;
    client.mutableBatchWriteRow() =
        bind(AsyncBatchWriter_Cb,
            boost::ref(*logger),
            boost::ref(sentReqs),
            _1, _2);
    {
        PutRowRequest req;
        fillRequired(req, "Table", PrimaryKeyValue::toInteger(0));
        writer->putRow(
            req,
            bind(AsyncBatchWriter_Callback<PutRowRequest, PutRowResponse>,
                boost::ref(*logger),
                boost::ref(mutex),
                boost::ref(receivedRows),
                _1, _2, _3));
    }
    {
        UpdateRowRequest req;
        fillRequired(req, "Table", PrimaryKeyValue::toInteger(1));
        writer->updateRow(
            req,
            bind(AsyncBatchWriter_Callback<UpdateRowRequest, UpdateRowResponse>,
                boost::ref(*logger),
                boost::ref(mutex),
                boost::ref(receivedRows),
                _1, _2, _3));
    }
    {
        DeleteRowRequest req;
        fillRequired(req, "Table", PrimaryKeyValue::toInteger(2));
        writer->deleteRow(
            req,
            bind(AsyncBatchWriter_Callback<DeleteRowRequest, DeleteRowResponse>,
                boost::ref(*logger),
                boost::ref(mutex),
                boost::ref(receivedRows),
                _1, _2, _3));
    }
    sleepFor(Duration::fromMsec(100));

    TESTA_ASSERT(sentReqs.load(boost::memory_order_acquire) == 0)
        (sentReqs.load(boost::memory_order_acquire)).issue();

    writer->flush();
    sleepFor(Duration::fromMsec(100));
    writer.reset();

    TESTA_ASSERT(sentReqs.load(boost::memory_order_acquire) == 1)
        (sentReqs.load(boost::memory_order_acquire)).issue();
    set<int64_t> results;
    FOREACH_ITER(i, receivedRows) {
        results.insert(i->integer());
    }
    TESTA_ASSERT(pp::prettyPrint(results) == "[0,1,2]")
        (results).issue();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(AsyncBatchWriter_Aggregation);

namespace {
void AsyncBatchWriter_Aggregation_DuplicatedRows(const string&)
{
    auto_ptr<Logger> logger(createLogger("/", Logger::kDebug));
    MockAsyncClient client;
    BatchWriterConfig cfg;
    cfg.mutableRegularNap() = Duration::fromHour(1);
    cfg.mutableMaxNap() = cfg.regularNap() * 2;
    auto_ptr<impl::AsyncBatchWriter> writer(newAsyncBatchWriter(client, *logger, cfg));

    Mutex mutex;
    boost::atomic<int64_t> sentReqs(0);
    deque<PrimaryKeyValue> receivedRows;
    client.mutableBatchWriteRow() =
        bind(AsyncBatchWriter_Cb,
            boost::ref(*logger),
            boost::ref(sentReqs),
            _1, _2);
    {
        PutRowRequest req;
        fillRequired(req, "Table", PrimaryKeyValue::toInteger(0));
        writer->putRow(
            req,
            bind(AsyncBatchWriter_Callback<PutRowRequest, PutRowResponse>,
                boost::ref(*logger),
                boost::ref(mutex),
                boost::ref(receivedRows),
                _1, _2, _3));
    }
    {
        UpdateRowRequest req;
        fillRequired(req, "Table", PrimaryKeyValue::toInteger(0));
        writer->updateRow(
            req,
            bind(AsyncBatchWriter_Callback<UpdateRowRequest, UpdateRowResponse>,
                boost::ref(*logger),
                boost::ref(mutex),
                boost::ref(receivedRows),
                _1, _2, _3));
    }
    writer->flush();
    sleepFor(Duration::fromMsec(100));
    TESTA_ASSERT(sentReqs.load(boost::memory_order_acquire) == 2)
        (sentReqs.load(boost::memory_order_acquire)).issue();
    TESTA_ASSERT(pp::prettyPrint(receivedRows) == "[0,0]")
        (receivedRows).issue();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(AsyncBatchWriter_Aggregation_DuplicatedRows);

namespace {
void AsyncBatchWriter_Aggregation_AutoIncr(const string&)
{
    auto_ptr<Logger> logger(createLogger("/", Logger::kDebug));
    MockAsyncClient client;
    BatchWriterConfig cfg;
    cfg.mutableRegularNap() = Duration::fromHour(1);
    cfg.mutableMaxNap() = cfg.regularNap() * 2;
    auto_ptr<impl::AsyncBatchWriter> writer(newAsyncBatchWriter(client, *logger, cfg));

    Mutex mutex;
    boost::atomic<int64_t> sentReqs(0);
    deque<PrimaryKeyValue> receivedRows;
    client.mutableBatchWriteRow() =
        bind(AsyncBatchWriter_Cb,
            boost::ref(*logger),
            boost::ref(sentReqs),
            _1, _2);
    {
        PutRowRequest req;
        fillRequired(req, "Table", PrimaryKeyValue::toAutoIncrement());
        writer->putRow(
            req,
            bind(AsyncBatchWriter_Callback<PutRowRequest, PutRowResponse>,
                boost::ref(*logger),
                boost::ref(mutex),
                boost::ref(receivedRows),
                _1, _2, _3));
    }
    {
        PutRowRequest req;
        fillRequired(req, "Table", PrimaryKeyValue::toAutoIncrement());
        writer->putRow(
            req,
            bind(AsyncBatchWriter_Callback<PutRowRequest, PutRowResponse>,
                boost::ref(*logger),
                boost::ref(mutex),
                boost::ref(receivedRows),
                _1, _2, _3));
    }
    sleepFor(Duration::fromMsec(100));

    TESTA_ASSERT(sentReqs.load(boost::memory_order_acquire) == 0)
        (sentReqs.load(boost::memory_order_acquire)).issue();

    writer->flush();
    sleepFor(Duration::fromMsec(100));

    TESTA_ASSERT(sentReqs.load(boost::memory_order_acquire) == 1)
        (sentReqs.load(boost::memory_order_acquire)).issue();
    TESTA_ASSERT(pp::prettyPrint(receivedRows) == "[auto-incr,auto-incr]")
        (receivedRows).issue();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(AsyncBatchWriter_Aggregation_AutoIncr);

namespace {

void _AsyncBatchWriter_CbWithDelay(
    Duration delay,
    Logger& logger,
    boost::atomic<int64_t>& sentReqs,
    BatchWriteRowRequest req,
    const function<void(
        BatchWriteRowRequest&, util::Optional<OTSError>&, BatchWriteRowResponse&)>& cb)
{
    sleepFor(delay);
    AsyncBatchWriter_Cb(logger, sentReqs, req, cb);
}

void AsyncBatchWriter_CbWithDelay(
    Actor& actor,
    Duration delay,
    Logger& logger,
    boost::atomic<int64_t>& sentReqs,
    BatchWriteRowRequest& req,
    const function<void(
        BatchWriteRowRequest&, util::Optional<OTSError>&, BatchWriteRowResponse&)>& cb)
{
    actor.pushBack(bind(_AsyncBatchWriter_CbWithDelay,
            delay,
            boost::ref(logger),
            boost::ref(sentReqs),
            req,
            cb));
}

void AsyncBatchWriter_Dtor(const string&)
{
    auto_ptr<Logger> logger(createLogger("/", Logger::kDebug));
    MockAsyncClient client;
    BatchWriterConfig cfg;
    cfg.mutableRegularNap() = Duration::fromHour(1);
    cfg.mutableMaxNap() = cfg.regularNap() * 2;
    auto_ptr<impl::AsyncBatchWriter> writer(newAsyncBatchWriter(client, *logger, cfg));

    Mutex mutex;
    boost::atomic<int64_t> sentReqs(0);
    deque<PrimaryKeyValue> receivedRows;
    Actor actor;
    client.mutableBatchWriteRow() =
        bind(AsyncBatchWriter_CbWithDelay,
            boost::ref(actor),
            Duration::fromMsec(500),
            boost::ref(*logger),
            boost::ref(sentReqs),
            _1, _2);
    {
        PutRowRequest req;
        fillRequired(req, "Table", PrimaryKeyValue::toInteger(0));
        writer->putRow(
            req,
            bind(AsyncBatchWriter_Callback<PutRowRequest, PutRowResponse>,
                boost::ref(*logger),
                boost::ref(mutex),
                boost::ref(receivedRows),
                _1, _2, _3));
    }
    writer->flush();
    sleepFor(Duration::fromMsec(100));
    writer.reset();
    sleepFor(Duration::fromMsec(100));

    TESTA_ASSERT(sentReqs.load(boost::memory_order_acquire) == 1)
        (sentReqs.load(boost::memory_order_acquire)).issue();
    TESTA_ASSERT(pp::prettyPrint(receivedRows) == "[0]")
        (receivedRows).issue();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(AsyncBatchWriter_Dtor);

namespace {
void AsyncBatchWriter_NextNapAndConcurrency(const string&)
{
    auto_ptr<Logger> logger(createLogger("/", Logger::kDebug));
    MockAsyncClient client;
    BatchWriterConfig cfg;
    cfg.mutableRegularNap() = Duration::fromMsec(10);
    cfg.mutableMaxNap() = Duration::fromMsec(30);
    cfg.mutableNapShrinkStep() = Duration::fromMsec(3);
    cfg.mutableMaxConcurrency() = 3;
    auto_ptr<impl::AsyncBatchWriter> writer(newAsyncBatchWriter(client, *logger, cfg));

    {
        // regular period
        boost::atomic<bool> backoff(false);
        Duration nap;
        int64_t concurrency = 0;
        tie(nap, concurrency) = writer->nextNapAndConcurrency(
            backoff, cfg.maxConcurrency(), cfg.regularNap());
        TESTA_ASSERT(nap == cfg.regularNap())
            (nap)
            (cfg.regularNap()).issue();
        TESTA_ASSERT(concurrency == cfg.maxConcurrency())
            (concurrency)
            (cfg.maxConcurrency()).issue();
    }
    {
        // backoff: shrink concurrency
        boost::atomic<bool> backoff(true);
        int64_t curConcur = 10;
        Duration nextNap;
        int64_t nextConcur = 0;
        tie(nextNap, nextConcur) = writer->nextNapAndConcurrency(
            backoff, curConcur, cfg.regularNap());
        TESTA_ASSERT(nextNap == cfg.regularNap())
            (nextNap)
            (cfg.regularNap()).issue();
        TESTA_ASSERT(nextConcur == curConcur / 2)
            (nextConcur)
            (curConcur).issue();
    }
    {
        // backoff: extend nap time
        boost::atomic<bool> backoff(true);
        int64_t curConcur = 1;
        Duration nextNap;
        int64_t nextConcur = 0;
        tie(nextNap, nextConcur) = writer->nextNapAndConcurrency(
            backoff, curConcur, cfg.regularNap());
        TESTA_ASSERT(nextNap == cfg.regularNap() * 2)
            (nextNap)
            (cfg.regularNap()).issue();
        TESTA_ASSERT(nextConcur == 1)
            (nextConcur)
            (curConcur).issue();
    }
    {
        // recover: shrink nap time
        boost::atomic<bool> backoff(false);
        Duration curNap = Duration::fromMsec(15);
        int64_t curConcur = 1;
        Duration nextNap;
        int64_t nextConcur = 0;
        tie(nextNap, nextConcur) = writer->nextNapAndConcurrency(
            backoff, curConcur, curNap);
        TESTA_ASSERT(nextNap == curNap - cfg.napShrinkStep())
            (nextNap)
            (curNap)
            (cfg.napShrinkStep()).issue();
        TESTA_ASSERT(nextConcur == 1)
            (nextConcur)
            (curConcur).issue();
    }
    {
        // recover: increase concurrency
        boost::atomic<bool> backoff(false);
        Duration curNap = cfg.regularNap();
        int64_t curConcur = 1;
        Duration nextNap;
        int64_t nextConcur = 0;
        tie(nextNap, nextConcur) = writer->nextNapAndConcurrency(
            backoff, curConcur, curNap);
        TESTA_ASSERT(nextNap == cfg.regularNap())
            (nextNap)
            (cfg.regularNap()).issue();
        TESTA_ASSERT(nextConcur == 2)
            (nextConcur)
            (curConcur).issue();
    }
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(AsyncBatchWriter_NextNapAndConcurrency);

namespace {
void AsyncBatchWriter_CbWithRetry(
    bool& flip,
    Logger& logger,
    boost::atomic<int64_t>& sentReqs,
    BatchWriteRowRequest& req,
    const function<void(
        BatchWriteRowRequest&, util::Optional<OTSError>&, BatchWriteRowResponse&)>& cb)
{
    if (flip) {
        Optional<OTSError> err(OTSError(OTSError::kPredefined_OTSServerBusy));
        BatchWriteRowResponse resp;
        cb(req, err, resp);
        sentReqs.fetch_add(1, boost::memory_order_acq_rel);
    } else {
        AsyncBatchWriter_Cb(logger, sentReqs, req, cb);
    }
    flip = !flip;
}

void AsyncBatchWriter_Retry(const string&)
{
    auto_ptr<Logger> logger(createLogger("/", Logger::kDebug));
    MockAsyncClient client;
    BatchWriterConfig cfg;
    auto_ptr<impl::AsyncBatchWriter> writer(newAsyncBatchWriter(client, *logger, cfg));

    bool flip = true;
    Mutex mutex;
    boost::atomic<int64_t> sentReqs(0);
    deque<PrimaryKeyValue> receivedRows;
    Actor actor;
    client.mutableBatchWriteRow() =
        bind(AsyncBatchWriter_CbWithRetry,
            boost::ref(flip),
            boost::ref(*logger),
            boost::ref(sentReqs),
            _1, _2);
    {
        PutRowRequest req;
        fillRequired(req, "Table", PrimaryKeyValue::toInteger(0));
        writer->putRow(
            req,
            bind(AsyncBatchWriter_Callback<PutRowRequest, PutRowResponse>,
                boost::ref(*logger),
                boost::ref(mutex),
                boost::ref(receivedRows),
                _1, _2, _3));
    }
    writer->flush();
    sleepFor(Duration::fromMsec(100));
    writer->flush();
    sleepFor(Duration::fromMsec(100));
    writer.reset();

    TESTA_ASSERT(sentReqs.load(boost::memory_order_acquire) == 2)
        (sentReqs.load(boost::memory_order_acquire)).issue();
    TESTA_ASSERT(pp::prettyPrint(receivedRows) == "[0]")
        (receivedRows).issue();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(AsyncBatchWriter_Retry);

namespace {
template<class T>
void errorEveryRow(
    BatchWriteRowResponse& resp,
    const BatchWriteRowRequest& req)
{
    RequestTraits<T> traits;
    IVector<BatchWriteRowResponse::Result>& results = traits.results(resp);
    const IVector<T>& requests = traits.requests(req);
    for(int64_t i = 0, sz = requests.size(); i < sz; ++i) {
        Result<Optional<Row>, OTSError>& result = results.append().mutableGet();
        OTSError err(OTSError::kPredefined_OTSServerBusy);
        result.mutableErrValue() = err;
    }
}

void AsyncBatchWriter_CbWithRetrySingleRow(
    bool& flip,
    Logger& logger,
    boost::atomic<int64_t>& sentReqs,
    BatchWriteRowRequest& req,
    const function<void(
        BatchWriteRowRequest&, util::Optional<OTSError>&, BatchWriteRowResponse&)>& cb)
{
    if (flip) {
        Optional<OTSError> err;
        BatchWriteRowResponse resp;
        errorEveryRow<BatchWriteRowRequest::Put>(resp, req);
        errorEveryRow<BatchWriteRowRequest::Update>(resp, req);
        errorEveryRow<BatchWriteRowRequest::Delete>(resp, req);
        cb(req, err, resp);
        sentReqs.fetch_add(1, boost::memory_order_acq_rel);
    } else {
        AsyncBatchWriter_Cb(logger, sentReqs, req, cb);
    }
    flip = !flip;
}

void AsyncBatchWriter_RetrySingleRow(const string&)
{
    auto_ptr<Logger> logger(createLogger("/", Logger::kDebug));
    MockAsyncClient client;
    BatchWriterConfig cfg;
    auto_ptr<impl::AsyncBatchWriter> writer(newAsyncBatchWriter(client, *logger, cfg));

    bool flip = true;
    Mutex mutex;
    boost::atomic<int64_t> sentReqs(0);
    deque<PrimaryKeyValue> receivedRows;
    Actor actor;
    client.mutableBatchWriteRow() =
        bind(AsyncBatchWriter_CbWithRetrySingleRow,
            boost::ref(flip),
            boost::ref(*logger),
            boost::ref(sentReqs),
            _1, _2);
    {
        PutRowRequest req;
        fillRequired(req, "Table", PrimaryKeyValue::toInteger(0));
        writer->putRow(
            req,
            bind(AsyncBatchWriter_Callback<PutRowRequest, PutRowResponse>,
                boost::ref(*logger),
                boost::ref(mutex),
                boost::ref(receivedRows),
                _1, _2, _3));
    }
    writer->flush();
    sleepFor(Duration::fromMsec(100));
    writer->flush();
    sleepFor(Duration::fromMsec(100));
    writer.reset();

    TESTA_ASSERT(sentReqs.load(boost::memory_order_acquire) == 2)
        (sentReqs.load(boost::memory_order_acquire)).issue();
    TESTA_ASSERT(pp::prettyPrint(receivedRows) == "[0]")
        (receivedRows).issue();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(AsyncBatchWriter_RetrySingleRow);

namespace {
void CallbackForErroneousReq(
    Logger& logger,
    Semaphore& sem,
    Mutex& mutex,
    Optional<OTSError>& out,
    PutRowRequest& req,
    Optional<OTSError>& err,
    PutRowResponse&)
{
    if (err.present()) {
        OTS_LOG_DEBUG(logger)
            ("Error", *err)
            .what("Expected error");
    } else {
        OTS_LOG_ERROR(logger)
            .what("An error is required");
    }
    ScopedLock g(mutex);
    out = err;
    sem.post();
}

void AsyncBatchWriter_ErroneousReq(const string&)
{
    auto_ptr<Logger> logger(createLogger("/", Logger::kDebug));
    MockAsyncClient client;
    BatchWriterConfig cfg;
    auto_ptr<impl::AsyncBatchWriter> writer(newAsyncBatchWriter(client, *logger, cfg));

    PutRowRequest req; // erroneous request
    Optional<OTSError> err;
    Semaphore sem(0);
    Mutex mutex;
    {
        ScopedLock g(mutex);
        writer->putRow(
            req,
            bind(CallbackForErroneousReq,
                boost::ref(*logger),
                boost::ref(sem),
                boost::ref(mutex),
                boost::ref(err),
                _1, _2, _3));
    }
    sem.wait();
    TESTA_ASSERT(err.present()).issue();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(AsyncBatchWriter_ErroneousReq);

} // namespace core
} // namespace tablestore
} // namespace aliyun
