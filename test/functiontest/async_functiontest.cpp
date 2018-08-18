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
#include "testa/testa.hpp"
#include "config.hpp"
#include "tablestore/core/batch_writer.hpp"
#include "tablestore/core/client.hpp"
#include "tablestore/core/types.hpp"
#include "tablestore/core/range_iterator.hpp"
#include "tablestore/util/logging.hpp"
#include "tablestore/util/threading.hpp"
#include <boost/ref.hpp>
#include <tr1/functional>
#include <tr1/memory>
#include <stdexcept>
#include <set>

using namespace std;
using namespace std::tr1;
using namespace std::tr1::placeholders;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {

namespace {

class Testbench;

class Trial
{
public:
    explicit Trial(Testbench& tb)
      : mTb(tb)
    {}

    virtual ~Trial() {}

protected:
    Testbench& testbench()
    {
        return mTb;
    }

private:
    virtual void asyncDo() =0;

private:
    Testbench& mTb;

    friend class Testbench;
};

class Testbench
{
public:
    explicit Testbench(
        Optional<OTSError>&,
        const string& csname);

    void operator()(
        CreateTableRequest&, Optional<OTSError>&, CreateTableResponse&);
    void operator()(
        DeleteTableRequest&, Optional<OTSError>&, DeleteTableResponse&);

    void asyncCreateTable();
    void asyncDeleteTable();
    void asyncDo();

    void gogogo(Trial& trial)
    {
        mTrial = &trial;
        asyncCreateTable();
        mSem->wait();
    }

    AsyncClient& client()
    {
        return *mClient;
    }

    const string& caseName() const
    {
        return mCaseName;
    }

    Logger& logger()
    {
        return *mRootLogger;
    }

private:
    Trial* mTrial;
    Optional<OTSError>& mOutError;
    const string& mCaseName;

    shared_ptr<Semaphore> mSem;
    shared_ptr<Logger> mRootLogger;
    shared_ptr<AsyncClient> mClient;
};

Testbench::Testbench(
    Optional<OTSError>& outError,
    const string& csname)
  : mTrial(NULL),
    mOutError(outError),
    mCaseName(csname),
    mSem(new Semaphore(0)),
    mRootLogger(createLogger("/", Logger::kDebug))
{
    Endpoint ep;
    Credential cr;
    read(ep, cr);
    ep.mutableEndpoint() = "https://" + ep.endpoint();
    ClientOptions opts;
    opts.mutableRequestTimeout() = Duration::fromSec(30);
    opts.mutableMaxConnections() = 2;
    opts.resetLogger(mRootLogger->spawn("sdk"));
    AsyncClient* p = NULL;
    Optional<OTSError> err = AsyncClient::create(p, ep, cr, opts);
    OTS_ASSERT(!err.present())
        (*err)
        (ep)
        (cr)
        (opts);
    mClient.reset(p);
}

void Testbench::operator()(
    CreateTableRequest& req,
    Optional<OTSError>& err,
    CreateTableResponse& resp)
{
    if (err.present()) {
        OTS_LOG_DEBUG(*mRootLogger)
            ("Error", *err)
            .what("FT: fail to create table");
        moveAssign(mOutError, util::move(err));
        asyncDeleteTable();
    } else {
        OTS_LOG_DEBUG(*mRootLogger)
            .what("FT: table created");
        asyncDo();
    }
}

void Testbench::operator()(
    DeleteTableRequest& req,
    Optional<OTSError>& err,
    DeleteTableResponse& resp)
{
    if (err.present()) {
        OTS_LOG_DEBUG(*mRootLogger)
            ("Error", *err)
            .what("FT: fail to delete table");
        if (!mOutError.present()) {
            moveAssign(mOutError, util::move(err));
        }
    } else {
        OTS_LOG_DEBUG(*mRootLogger)
            .what("FT: table deleted");
    }
    mSem->post();
}

void Testbench::asyncCreateTable()
{
    CreateTableRequest req;
    req.mutableMeta().mutableTableName() = mCaseName;
    req.mutableMeta().mutableSchema().append() =
        PrimaryKeyColumnSchema("pkey", kPKT_Integer);
    mClient->createTable(req, *this);
}

void Testbench::asyncDeleteTable()
{
    DeleteTableRequest req;
    req.mutableTable() = mCaseName;
    mClient->deleteTable(req, *this);
}

void Testbench::asyncDo()
{
    OTS_ASSERT(mTrial != NULL);
    mTrial->asyncDo();
}

class ListTableTrial: public Trial
{
public:
    explicit ListTableTrial(
        Testbench& tb,
        Optional<OTSError>& outErr,
        ListTableResponse& outResp)
      : Trial(tb),
        mOutError(outErr),
        mOutResp(outResp)
    {}

    void asyncDo()
    {
        ListTableRequest req;
        testbench().client().listTable(req, *this);
    }

    void operator()(
        ListTableRequest&,
        Optional<OTSError>& err,
        ListTableResponse& resp)
    {
        if (err.present()) {
            OTS_LOG_ERROR(testbench().logger())
                ("Error", *err)
                .what("FT: fail to list table");
            moveAssign(mOutError, util::move(err));
        } else {
            OTS_LOG_DEBUG(testbench().logger())
                .what("FT: done");
            moveAssign(mOutResp, util::move(resp));
        }
        testbench().asyncDeleteTable();
    }

private:
    Optional<OTSError>& mOutError;
    ListTableResponse& mOutResp;
};

void AsyncListTable(const string& csname)
{
    Optional<OTSError> err;
    ListTableResponse resp;
    Testbench tb(err, csname);
    ListTableTrial trial(tb, err, resp);
    tb.gogogo(trial);

    TESTA_ASSERT(!err.present())
        (*err).issue();
    set<string> tables;
    const IVector<string>& xs = resp.tables();
    for(int64_t i = 0; i < xs.size(); ++i) {
        tables.insert(xs[i]);
    }
    TESTA_ASSERT(tables.find(csname) != tables.end())
        (tables)(csname).issue();
}
TESTA_DEF_JUNIT_LIKE1(AsyncListTable);

class DescribeTableTrial: public Trial
{
public:
    explicit DescribeTableTrial(
        Testbench& tb,
        Optional<OTSError>& outErr,
        DescribeTableResponse& outResp)
      : Trial(tb),
        mOutError(outErr),
        mOutResp(outResp)
    {}

    void asyncDo()
    {
        DescribeTableRequest req;
        req.mutableTable() = testbench().caseName();
        testbench().client().describeTable(req, *this);
    }

    void operator()(
        DescribeTableRequest& req,
        Optional<OTSError>& err,
        DescribeTableResponse& resp)
    {
        if (err.present()) {
            OTS_LOG_ERROR(testbench().logger())
                ("Error", *err)
                .what("FT: fail to describe table");
            moveAssign(mOutError, util::move(err));
        } else {
            OTS_LOG_DEBUG(testbench().logger())
                .what("FT: table described");
            moveAssign(mOutResp, util::move(resp));
        }
        testbench().asyncDeleteTable();
    }

private:
    Optional<OTSError>& mOutError;
    DescribeTableResponse& mOutResp;
};

void AsyncDescribeTable(const string& csname)
{
    Optional<OTSError> err;
    DescribeTableResponse resp;
    Testbench tb(err, csname);
    DescribeTableTrial trial(tb, err, resp);
    tb.gogogo(trial);

    TESTA_ASSERT(!err.present())
        (*err).issue();
    const TableMeta& meta = resp.meta();
    TESTA_ASSERT(meta.tableName() == csname)
        (csname)(resp).issue();
    TESTA_ASSERT(pp::prettyPrint(meta.schema()) == "[{\"pkey\":kPKT_Integer}]")
        (resp).issue();
    const TableOptions& tblOpts = resp.options();
    TESTA_ASSERT(pp::prettyPrint(tblOpts) == "{"
        "\"ReservedThroughput\":{\"Read\":0,\"Write\":0},"
        "\"TimeToLive\":-1,\"MaxVersions\":1,\"MaxTimeDeviation\":86400}")
        (resp).issue();
    TESTA_ASSERT(resp.status() == kTS_Active)
        (resp).issue();
    TESTA_ASSERT(resp.shardSplitPoints().size() == 0)
        (resp).issue();
}
TESTA_DEF_JUNIT_LIKE1(AsyncDescribeTable);

class UpdateTableTrial: public Trial
{
public:
    explicit UpdateTableTrial(
        Testbench& tb,
        Optional<OTSError>& outErr,
        DescribeTableResponse& dtResp,
        UpdateTableRequest& utReq)
      : Trial(tb),
        mOutError(outErr),
        mDescribeTableResponse(dtResp),
        mUpdateTableRequest(utReq)
    {}

    void asyncDo()
    {
        testbench().client().updateTable(mUpdateTableRequest, *this);
    }

    void operator()(
        UpdateTableRequest& req,
        Optional<OTSError>& err,
        UpdateTableResponse& resp)
    {
        moveAssign(mUpdateTableRequest, util::move(req));
        if (err.present()) {
            OTS_LOG_ERROR(testbench().logger())
                ("Error", *err)
                .what("FT: fail to update table");
            moveAssign(mOutError, util::move(err));
            testbench().asyncDeleteTable();
        } else {
            OTS_LOG_DEBUG(testbench().logger())
                .what("FT: table updated");
            DescribeTableRequest req;
            req.mutableTable() = testbench().caseName();
            testbench().client().describeTable(req, *this);
        }
    }

    void operator()(
        DescribeTableRequest& req,
        Optional<OTSError>& err,
        DescribeTableResponse& resp)
    {
        if (err.present()) {
            OTS_LOG_ERROR(testbench().logger())
                ("Error", *err)
                .what("FT: fail to describe table");
            moveAssign(mOutError, util::move(err));
        } else {
            OTS_LOG_DEBUG(testbench().logger())
                .what("FT: table described");
            moveAssign(mDescribeTableResponse, util::move(resp));
        }
        testbench().asyncDeleteTable();
    }

private:
    Optional<OTSError>& mOutError;
    DescribeTableResponse& mDescribeTableResponse;
    UpdateTableRequest& mUpdateTableRequest;
};

void AsyncUpdateTable(const string& csname)
{
    Optional<OTSError> err;
    UpdateTableRequest utReq;
    utReq.mutableTable() = csname;
    utReq.mutableOptions().mutableMaxVersions().reset(2);
    DescribeTableResponse dtResp;
    Testbench tb(err, csname);
    UpdateTableTrial trial(tb, err, dtResp, utReq);
    tb.gogogo(trial);

    TESTA_ASSERT(!err.present())
        (*err)(utReq)(dtResp).issue();
    TESTA_ASSERT(dtResp.options().maxVersions().present())
        (utReq)(dtResp).issue();
    TESTA_ASSERT(*dtResp.options().maxVersions() == *utReq.options().maxVersions())
        (utReq)(dtResp).issue();
}
TESTA_DEF_JUNIT_LIKE1(AsyncUpdateTable);

class PutRowTrial: public Trial
{
public:
    explicit PutRowTrial(
        Testbench& tb,
        Optional<OTSError>& outErr,
        deque<Row>& resultRows,
        PutRowRequest& prReq)
      : Trial(tb),
        mOutError(outErr),
        mResultRows(resultRows),
        mPutRowRequest(prReq)
    {}

    void close()
    {
        mReadRowsThread.join();
    }

    void asyncDo()
    {
        testbench().client().putRow(
            mPutRowRequest,
            bind(&PutRowTrial::putRowCallback, this, _1, _2, _3));
    }

    void putRowCallback(
        PutRowRequest& req,
        Optional<OTSError>& err,
        PutRowResponse& resp)
    {
        moveAssign(mPutRowRequest, util::move(req));
        if (err.present()) {
            OTS_LOG_ERROR(testbench().logger())
                ("Error", *err)
                .what("FT: fail to put row");
            moveAssign(mOutError, util::move(err));
            testbench().asyncDeleteTable();
        } else {
            OTS_LOG_DEBUG(testbench().logger())
                .what("FT: row put");
            Thread t(bind(&PutRowTrial::readRows, this));
            moveAssign(mReadRowsThread, util::move(t));
        }
    }

    void readRows()
    {
        RangeQueryCriterion cri;
        cri.mutableInclusiveStart().append() =
            PrimaryKeyColumn("pkey", PrimaryKeyValue::toInfMin());
        cri.mutableExclusiveEnd().append() =
            PrimaryKeyColumn("pkey", PrimaryKeyValue::toInfMax());
        cri.mutableTable() = testbench().caseName();
        cri.mutableMaxVersions().reset(1);
        RangeIterator iter(testbench().client(), cri);
        for(;;) {
            Optional<OTSError> e = iter.moveNext();
            if (e.present()) {
                OTS_LOG_DEBUG(testbench().logger())
                    ("Error", *e)
                    .what("RI: Error on range iterator");
                moveAssign(mOutError, util::move(e));
                break;
            }
            if (!iter.valid()) {
                OTS_LOG_DEBUG(testbench().logger())
                    ("RowCount", mResultRows.size())
                    .what("RI: finish on range iterator");
                break;
            }
            mResultRows.push_back(Row());
            moveAssign(mResultRows.back(), util::move(iter.get()));
        }
        testbench().asyncDeleteTable();
    }

private:
    Optional<OTSError>& mOutError;
    deque<Row>& mResultRows;
    PutRowRequest& mPutRowRequest;
    Thread mReadRowsThread;
};

void AsyncPutRow(const string& csname)
{
    Optional<OTSError> err;
    PutRowRequest req;
    req.mutableRowChange().mutableTable() = csname;
    req.mutableRowChange().mutablePrimaryKey().append() =
        PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(123));
    req.mutableRowChange().mutableAttributes().append() =
        Attribute("attr", AttributeValue::toStr("abc"));
    deque<Row> resultRows;
    Testbench tb(err, csname);
    PutRowTrial trial(tb, err, resultRows, req);
    tb.gogogo(trial);
    trial.close();

    TESTA_ASSERT(!err.present())
        (*err).issue();
    TESTA_ASSERT(resultRows.size() == 1)
        (resultRows)(req).issue();
    const Row& row = resultRows[0];
    TESTA_ASSERT(row.primaryKey().compare(req.rowChange().primaryKey()) == kCR_Equivalent)
        (resultRows)(req).issue();
    TESTA_ASSERT(row.attributes().size() == 1)
        (resultRows)(req).issue();
    TESTA_ASSERT(row.attributes()[0].name() == req.rowChange().attributes()[0].name())
        (resultRows)(req).issue();
    TESTA_ASSERT(row.attributes()[0].value().compare(req.rowChange().attributes()[0].value()) == kCR_Equivalent)
        (resultRows)(req).issue();
}
TESTA_DEF_JUNIT_LIKE1(AsyncPutRow);

class GetRowTrial: public Trial
{
public:
    explicit GetRowTrial(
        Testbench& tb,
        Optional<OTSError>& outErr,
        GetRowResponse& grRespHit,
        GetRowResponse& grRespMiss,
        PutRowRequest& prReq)
      : Trial(tb),
        mOutError(outErr),
        mGetRowRespHit(grRespHit),
        mGetRowRespMiss(grRespMiss),
        mPutRowRequest(prReq)
    {}

    void asyncDo()
    {
        PutRowRequest req(mPutRowRequest);
        testbench().client().putRow(req,
            bind(&GetRowTrial::callbackPutRow, this, _1, _2, _3));
    }

    void callbackPutRow(
        PutRowRequest& req,
        Optional<OTSError>& err,
        PutRowResponse& resp)
    {
        moveAssign(mPutRowRequest, util::move(req));
        if (err.present()) {
            OTS_LOG_ERROR(testbench().logger())
                ("Error", *err)
                .what("FT: fail to put row");
            moveAssign(mOutError, util::move(err));
            testbench().asyncDeleteTable();
        } else {
            OTS_LOG_DEBUG(testbench().logger())
                .what("FT: row put");
            GetRowRequest req;
            req.mutableQueryCriterion().mutableTable() = testbench().caseName();
            req.mutableQueryCriterion().mutableMaxVersions().reset(1);
            req.mutableQueryCriterion().mutablePrimaryKey() =
                mPutRowRequest.rowChange().primaryKey();
            testbench().client().getRow(req,
                bind(&GetRowTrial::callbackGetRowHit, this, _1, _2, _3));
        }
    }

    void callbackGetRowHit(
        GetRowRequest& req,
        Optional<OTSError>& err,
        GetRowResponse& resp)
    {
        if (err.present()) {
            OTS_LOG_ERROR(testbench().logger())
                ("Error", *err)
                .what("FT: fail to get hit-row");
            moveAssign(mOutError, util::move(err));
            testbench().asyncDeleteTable();
        } else {
            OTS_LOG_DEBUG(testbench().logger())
                .what("FT: hit-row read");
            moveAssign(mGetRowRespHit, util::move(resp));

            GetRowRequest req;
            req.mutableQueryCriterion().mutableTable() = testbench().caseName();
            req.mutableQueryCriterion().mutableMaxVersions().reset(1);
            req.mutableQueryCriterion().mutablePrimaryKey().append() =
                PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(456));
            testbench().client().getRow(req,
                bind(&GetRowTrial::callbackGetRowMiss, this, _1, _2, _3));
        }
    }

    void callbackGetRowMiss(
        GetRowRequest& req,
        Optional<OTSError>& err,
        GetRowResponse& resp)
    {
        if (err.present()) {
            OTS_LOG_ERROR(testbench().logger())
                ("Error", *err)
                .what("FT: fail to get miss-row");
            moveAssign(mOutError, util::move(err));
            testbench().asyncDeleteTable();
        } else {
            OTS_LOG_DEBUG(testbench().logger())
                .what("FT: miss-row read");
            moveAssign(mGetRowRespMiss, util::move(resp));
            testbench().asyncDeleteTable();
        }
    }

private:
    Optional<OTSError>& mOutError;
    GetRowResponse& mGetRowRespHit;
    GetRowResponse& mGetRowRespMiss;
    PutRowRequest& mPutRowRequest;
};

void AsyncGetRow(const string& csname)
{
    Optional<OTSError> err;
    PutRowRequest prReq;
    prReq.mutableRowChange().mutableTable() = csname;
    prReq.mutableRowChange().mutablePrimaryKey().append() =
        PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(123));
    prReq.mutableRowChange().mutableAttributes().append() =
        Attribute("attr", AttributeValue::toStr("abc"));
    GetRowResponse grRespHit;
    GetRowResponse grRespMiss;
    Testbench tb(err, csname);
    GetRowTrial trial(tb, err, grRespHit, grRespMiss, prReq);
    tb.gogogo(trial);

    TESTA_ASSERT(!err.present())
        (*err).issue();
    {
        const Optional<Row>& row = grRespHit.row();
        TESTA_ASSERT(row.present())
            (grRespHit).issue();
        TESTA_ASSERT(row->primaryKey().compare(prReq.rowChange().primaryKey()) == kCR_Equivalent)
            (grRespHit)(prReq).issue();
        TESTA_ASSERT(row->attributes().size() == 1)
            (grRespHit)(prReq).issue();
        TESTA_ASSERT(prReq.rowChange().attributes().size() == 1)
            (grRespHit)(prReq).issue();
        TESTA_ASSERT(row->attributes()[0].name() == prReq.rowChange().attributes()[0].name())
            (grRespHit)(prReq).issue();
        TESTA_ASSERT(row->attributes()[0].value().compare(prReq.rowChange().attributes()[0].value()) == kCR_Equivalent)
            (grRespHit)(prReq).issue();
    }
    {
        TESTA_ASSERT(!grRespMiss.row().present())
            (grRespMiss).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(AsyncGetRow);

class UpdateRowTrial: public Trial
{
public:
    explicit UpdateRowTrial(
        Testbench& tb,
        Optional<OTSError>& outErr,
        deque<Row>& resultRows,
        PutRowRequest& prReq,
        UpdateRowRequest& urReq)
      : Trial(tb),
        mOutError(outErr),
        mResultRows(resultRows),
        mPutRowRequest(prReq),
        mUpdateRowRequest(urReq)
    {}

    void close()
    {
        mReadRowsThread.join();
    }

    void asyncDo()
    {
        testbench().client().putRow(
            mPutRowRequest,
            bind(&UpdateRowTrial::callbackPutRow, this, _1, _2, _3));
    }

    void callbackPutRow(
        PutRowRequest& req,
        Optional<OTSError>& err,
        PutRowResponse& resp)
    {
        moveAssign(mPutRowRequest, util::move(req));
        if (err.present()) {
            OTS_LOG_ERROR(testbench().logger())
                ("Error", *err)
                .what("FT: fail to put row");
            moveAssign(mOutError, util::move(err));
            testbench().asyncDeleteTable();
        } else {
            OTS_LOG_DEBUG(testbench().logger())
                .what("FT: row put");
            testbench().client().updateRow(
                mUpdateRowRequest,
                bind(&UpdateRowTrial::callbackUpdateRow, this, _1, _2, _3));
        }
    }

    void callbackUpdateRow(
        UpdateRowRequest& req,
        Optional<OTSError>& err,
        UpdateRowResponse& resp)
    {
        moveAssign(mUpdateRowRequest, util::move(req));
        if (err.present()) {
            OTS_LOG_ERROR(testbench().logger())
                ("Error", *err)
                .what("FT: fail to update row");
            moveAssign(mOutError, util::move(err));
            testbench().asyncDeleteTable();
        } else {
            OTS_LOG_DEBUG(testbench().logger())
                .what("FT: row updated");
            Thread t(bind(&UpdateRowTrial::readRows, this));
            moveAssign(mReadRowsThread, util::move(t));
        }
    }

    void readRows()
    {
        RangeQueryCriterion cri;
        cri.mutableInclusiveStart().append() =
            PrimaryKeyColumn("pkey", PrimaryKeyValue::toInfMin());
        cri.mutableExclusiveEnd().append() =
            PrimaryKeyColumn("pkey", PrimaryKeyValue::toInfMax());
        cri.mutableTable() = testbench().caseName();
        cri.mutableMaxVersions().reset(1);
        RangeIterator iter(testbench().client(), cri);
        for(;;) {
            Optional<OTSError> e = iter.moveNext();
            if (e.present()) {
                OTS_LOG_DEBUG(testbench().logger())
                    ("Error", *e)
                    .what("RI: Error on range iterator");
                moveAssign(mOutError, util::move(e));
                break;
            }
            if (!iter.valid()) {
                OTS_LOG_DEBUG(testbench().logger())
                    ("RowCount", mResultRows.size())
                    .what("RI: finish on range iterator");
                break;
            }
            mResultRows.push_back(Row());
            moveAssign(mResultRows.back(), util::move(iter.get()));
        }
        testbench().asyncDeleteTable();
    }

private:
    Optional<OTSError>& mOutError;
    deque<Row>& mResultRows;
    PutRowRequest& mPutRowRequest;
    UpdateRowRequest& mUpdateRowRequest;
    Thread mReadRowsThread;
};

void AsyncUpdateRow(const string& csname)
{
    UtcTime ts = UtcTime::fromMsec(UtcTime::now().toMsec());
    Optional<OTSError> err;
    PutRowRequest prReq;
    {
        prReq.mutableRowChange().mutableTable() = csname;
        prReq.mutableRowChange().mutablePrimaryKey().append() =
            PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(123));
        prReq.mutableRowChange().mutableAttributes().append() =
            Attribute(
                "A",
                AttributeValue::toStr("a"),
                ts);
        prReq.mutableRowChange().mutableAttributes().append() =
            Attribute(
                "B",
                AttributeValue::toBlob("b"),
                ts);
    }
    UpdateRowRequest urReq;
    DequeBasedVector<Row> oracle;
    {
        urReq.mutableRowChange().mutableTable() = csname;
        urReq.mutableRowChange().mutablePrimaryKey().append() =
            PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(123));
        RowUpdateChange::Update& a = urReq.mutableRowChange().mutableUpdates()
            .append();
        a.mutableType() = RowUpdateChange::Update::kDelete;
        a.mutableAttrName() = "A";
        a.mutableTimestamp().reset(ts);
        RowUpdateChange::Update& b = urReq.mutableRowChange().mutableUpdates()
            .append();
        b.mutableType() = RowUpdateChange::Update::kDeleteAll;
        b.mutableAttrName() = "B";
        RowUpdateChange::Update& c = urReq.mutableRowChange().mutableUpdates()
            .append();
        c.mutableType() = RowUpdateChange::Update::kPut;
        c.mutableAttrName() = "C";
        c.mutableAttrValue().reset(AttributeValue::toStr("c"));
        c.mutableTimestamp().reset(ts);

        Row& row = oracle.append();
        row.mutablePrimaryKey().append() = urReq.rowChange().primaryKey()[0];
        Attribute& attr = row.mutableAttributes().append();
        attr.mutableName() = urReq.rowChange().updates()[2].attrName();
        attr.mutableValue() = *urReq.rowChange().updates()[2].attrValue();
        attr.mutableTimestamp().reset(*urReq.rowChange().updates()[2].timestamp());
    }
    deque<Row> trialRows;
    Testbench tb(err, csname);
    UpdateRowTrial trial(tb, err, trialRows, prReq, urReq);
    tb.gogogo(trial);
    trial.close();

    TESTA_ASSERT(!err.present())
        (*err).issue();
    int64_t osz = oracle.size();
    int64_t tsz = trialRows.size();
    for(int64_t i = 0; i < osz && i < tsz; ++i) {
        const Row& orc = oracle[i];
        const Row& tri = trialRows[i];
        TESTA_ASSERT(pp::prettyPrint(tri) == pp::prettyPrint(orc))
            (tri)(orc)(i).issue();
    }
    if (osz < tsz) {
        TESTA_ASSERT(osz == tsz)
            (osz)
            (tsz)
            (trialRows[osz])
            .issue("oracle is smaller than trial.");
    } else if (osz > tsz) {
        TESTA_ASSERT(osz == tsz)
            (osz)
            (tsz)
            (oracle[tsz])
            .issue("oracle is larger than trial.");
    }
}
TESTA_DEF_JUNIT_LIKE1(AsyncUpdateRow);

class DeleteRowTrial: public Trial
{
public:
    explicit DeleteRowTrial(
        Testbench& tb,
        Optional<OTSError>& outErr,
        deque<Row>& resultRows,
        PutRowRequest& prReq)
      : Trial(tb),
        mOutError(outErr),
        mResultRows(resultRows),
        mPutRowRequest(prReq)
    {}

    void close()
    {
        mReadRowsThread.join();
    }

    void asyncDo()
    {
        PutRowRequest req(mPutRowRequest);
        testbench().client().putRow(
            req,
            bind(&DeleteRowTrial::callbackPutRow, this, _1, _2, _3));
    }

    void callbackPutRow(
        PutRowRequest& req,
        Optional<OTSError>& err,
        PutRowResponse& resp)
    {
        moveAssign(mPutRowRequest, util::move(req));
        if (err.present()) {
            OTS_LOG_ERROR(testbench().logger())
                ("Error", *err)
                .what("FT: fail to put row");
            moveAssign(mOutError, util::move(err));
            testbench().asyncDeleteTable();
        } else {
            OTS_LOG_DEBUG(testbench().logger())
                .what("FT: row put");
            DeleteRowRequest req;
            req.mutableRowChange().mutableTable() = testbench().caseName();
            req.mutableRowChange().mutablePrimaryKey().append() =
                mPutRowRequest.rowChange().primaryKey()[0];
            testbench().client().deleteRow(
                req,
                bind(&DeleteRowTrial::callbackDeleteRow, this, _1, _2, _3));
        }
    }

    void callbackDeleteRow(
        DeleteRowRequest& req,
        Optional<OTSError>& err,
        DeleteRowResponse& resp)
    {
        if (err.present()) {
            OTS_LOG_ERROR(testbench().logger())
                ("Error", *err)
                .what("FT: fail to update row");
            moveAssign(mOutError, util::move(err));
            testbench().asyncDeleteTable();
        } else {
            OTS_LOG_DEBUG(testbench().logger())
                .what("FT: row updated");
            Thread t(bind(&DeleteRowTrial::readRows, this));
            moveAssign(mReadRowsThread, util::move(t));
        }
    }

    void readRows()
    {
        RangeQueryCriterion cri;
        cri.mutableInclusiveStart().append() =
            PrimaryKeyColumn("pkey", PrimaryKeyValue::toInfMin());
        cri.mutableExclusiveEnd().append() =
            PrimaryKeyColumn("pkey", PrimaryKeyValue::toInfMax());
        cri.mutableTable() = testbench().caseName();
        cri.mutableMaxVersions().reset(1);
        RangeIterator iter(testbench().client(), cri);
        for(;;) {
            Optional<OTSError> e = iter.moveNext();
            if (e.present()) {
                OTS_LOG_DEBUG(testbench().logger())
                    ("Error", *e)
                    .what("RI: Error on range iterator");
                moveAssign(mOutError, util::move(e));
                break;
            }
            if (!iter.valid()) {
                OTS_LOG_DEBUG(testbench().logger())
                    ("RowCount", mResultRows.size())
                    .what("RI: finish on range iterator");
                break;
            }
            mResultRows.push_back(Row());
            moveAssign(mResultRows.back(), util::move(iter.get()));
        }
        testbench().asyncDeleteTable();
    }

private:
    Optional<OTSError>& mOutError;
    deque<Row>& mResultRows;
    PutRowRequest& mPutRowRequest;
    Thread mReadRowsThread;
};

void AsyncDeleteRow(const string& csname)
{
    Optional<OTSError> err;
    PutRowRequest prReq;
    {
        prReq.mutableRowChange().mutableTable() = csname;
        prReq.mutableRowChange().mutablePrimaryKey().append() =
            PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(123));
    }
    deque<Row> trialRows;
    Testbench tb(err, csname);
    DeleteRowTrial trial(tb, err, trialRows, prReq);
    tb.gogogo(trial);
    trial.close();

    TESTA_ASSERT(!err.present())
        (*err).issue();
    TESTA_ASSERT(trialRows.size() == 0)
        (trialRows.size())(trialRows).issue();
}
TESTA_DEF_JUNIT_LIKE1(AsyncDeleteRow);

class BatchGetRowTrial: public Trial
{
public:
    explicit BatchGetRowTrial(
        Testbench& tb,
        Optional<OTSError>& outErr,
        BatchGetRowResponse& bgrResp,
        PutRowRequest& prReq,
        BatchGetRowRequest& bgrReq)
      : Trial(tb),
        mOutError(outErr),
        mBgrResp(bgrResp),
        mPutRowRequest(prReq),
        mBgrRequest(bgrReq)
    {}

    void close()
    {
    }

    void asyncDo()
    {
        PutRowRequest req(mPutRowRequest);
        testbench().client().putRow(
            req,
            bind(&BatchGetRowTrial::callbackPutRow, this, _1, _2, _3));
    }

    void callbackPutRow(
        PutRowRequest& req,
        Optional<OTSError>& err,
        PutRowResponse& resp)
    {
        moveAssign(mPutRowRequest, util::move(req));
        if (err.present()) {
            OTS_LOG_ERROR(testbench().logger())
                ("Error", *err)
                .what("FT: fail to put row");
            moveAssign(mOutError, util::move(err));
            testbench().asyncDeleteTable();
        } else {
            OTS_LOG_DEBUG(testbench().logger())
                .what("FT: row put");
            testbench().client().batchGetRow(
                mBgrRequest,
                bind(&BatchGetRowTrial::callbackBatchGetRow, this, _1, _2, _3));
        }
    }

    void callbackBatchGetRow(
        BatchGetRowRequest& req,
        Optional<OTSError>& err,
        BatchGetRowResponse& resp)
    {
        moveAssign(mBgrRequest, util::move(req));
        if (err.present()) {
            OTS_LOG_ERROR(testbench().logger())
                ("Error", *err)
                .what("FT: fail to batch get rows");
            moveAssign(mOutError, util::move(err));
            testbench().asyncDeleteTable();
        } else {
            OTS_LOG_DEBUG(testbench().logger())
                .what("FT: rows gotten in a batch");
            moveAssign(mBgrResp, util::move(resp));
            testbench().asyncDeleteTable();
        }
    }

private:
    Optional<OTSError>& mOutError;
    BatchGetRowResponse& mBgrResp;
    PutRowRequest& mPutRowRequest;
    BatchGetRowRequest& mBgrRequest;
};

void AsyncBatchGetRow(const string& csname)
{
    Optional<OTSError> err;
    PutRowRequest prReq;
    {
        prReq.mutableRowChange().mutableTable() = csname;
        prReq.mutableRowChange().mutablePrimaryKey().append() =
            PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(123));
    }
    const char kHitRowKey[] = "HitRowKey";
    const char kMissRowKey[] = "MissRowKey";
    BatchGetRowRequest bgrReq;
    {
        MultiPointQueryCriterion& cri = bgrReq.mutableCriteria().append();
        cri.mutableTable() = csname;
        cri.mutableMaxVersions().reset(1);
        {
            PrimaryKey& pkey = cri.mutableRowKeys().append().mutableGet();
            pkey.append() = PrimaryKeyColumn(
                "pkey",
                PrimaryKeyValue::toInteger(123));
            cri.mutableRowKeys().back().mutableUserData() = kHitRowKey;
        }
        {
            PrimaryKey& pkey = cri.mutableRowKeys().append().mutableGet();
            pkey.append() = PrimaryKeyColumn(
                "pkey",
                PrimaryKeyValue::toInteger(456));
            cri.mutableRowKeys().back().mutableUserData() = kMissRowKey;
        }
    }
    BatchGetRowResponse bgrResp;
    Testbench tb(err, csname);
    BatchGetRowTrial trial(tb, err, bgrResp, prReq, bgrReq);
    tb.gogogo(trial);
    trial.close();

    TESTA_ASSERT(!err.present())
        (*err).issue();
    const IVector<BatchGetRowResponse::Result>& results = bgrResp.results();
    TESTA_ASSERT(results.size() == 2)
        (bgrResp).issue();

    TESTA_ASSERT(results[0].get().ok())
        (bgrResp).issue();
    TESTA_ASSERT(results[0].get().okValue().present())
        (bgrResp).issue();
    TESTA_ASSERT(pp::prettyPrint(*results[0].get().okValue()) ==
        "{\"PrimaryKey\":{\"pkey\":123},\"Attributes\":[]}")
        (bgrResp).issue();
    TESTA_ASSERT(results[0].userData() == kHitRowKey).issue();

    TESTA_ASSERT(results[1].get().ok())
        (bgrResp).issue();
    TESTA_ASSERT(!results[1].get().okValue().present())
        (bgrResp).issue();
    TESTA_ASSERT(results[1].userData() == kMissRowKey).issue();
}
TESTA_DEF_JUNIT_LIKE1(AsyncBatchGetRow);

class BatchWriteRowTrial: public Trial
{
public:
    explicit BatchWriteRowTrial(
        Testbench& tb,
        Optional<OTSError>& outErr,
        deque<Row>& resultRows,
        BatchWriteRowResponse& bwrResp,
        PutRowRequest& prReq,
        BatchWriteRowRequest& bwrReq)
      : Trial(tb),
        mOutError(outErr),
        mResultRows(resultRows),
        mBwrResponse(bwrResp),
        mPutRowRequest(prReq),
        mBwrRequest(bwrReq)
    {}

    void close()
    {
        mReadRowsThread.join();
    }

    void asyncDo()
    {
        testbench().client().putRow(
            mPutRowRequest,
            bind(&BatchWriteRowTrial::callbackPutRow, this, _1, _2, _3));
    }

    void callbackPutRow(
        PutRowRequest& req,
        Optional<OTSError>& err,
        PutRowResponse& resp)
    {
        moveAssign(mPutRowRequest, util::move(req));
        if (err.present()) {
            OTS_LOG_ERROR(testbench().logger())
                ("Error", *err)
                .what("FT: fail to put row");
            moveAssign(mOutError, util::move(err));
            testbench().asyncDeleteTable();
        } else {
            OTS_LOG_DEBUG(testbench().logger())
                .what("FT: row put");
            testbench().client().batchWriteRow(
                mBwrRequest,
                bind(&BatchWriteRowTrial::callbackBatchWriteRow, this, _1, _2, _3));
        }
    }

    void callbackBatchWriteRow(
        BatchWriteRowRequest& req,
        Optional<OTSError>& err,
        BatchWriteRowResponse& resp)
    {
        moveAssign(mBwrRequest, util::move(req));
        if (err.present()) {
            OTS_LOG_ERROR(testbench().logger())
                ("Error", *err)
                .what("FT: fail to batch get rows");
            moveAssign(mOutError, util::move(err));
            testbench().asyncDeleteTable();
        } else {
            OTS_LOG_DEBUG(testbench().logger())
                .what("FT: rows gotten in a batch");
            moveAssign(mBwrResponse, util::move(resp));
            Thread t(bind(&BatchWriteRowTrial::readRows, this));
            moveAssign(mReadRowsThread, util::move(t));
        }
    }

    void readRows()
    {
        RangeQueryCriterion cri;
        cri.mutableInclusiveStart().append() =
            PrimaryKeyColumn("pkey", PrimaryKeyValue::toInfMin());
        cri.mutableExclusiveEnd().append() =
            PrimaryKeyColumn("pkey", PrimaryKeyValue::toInfMax());
        cri.mutableTable() = testbench().caseName();
        cri.mutableMaxVersions().reset(1);
        RangeIterator iter(testbench().client(), cri);
        for(;;) {
            Optional<OTSError> e = iter.moveNext();
            if (e.present()) {
                OTS_LOG_DEBUG(testbench().logger())
                    ("Error", *e)
                    .what("RI: Error on range iterator");
                moveAssign(mOutError, util::move(e));
                break;
            }
            if (!iter.valid()) {
                OTS_LOG_DEBUG(testbench().logger())
                    ("RowCount", mResultRows.size())
                    .what("RI: finish on range iterator");
                break;
            }
            mResultRows.push_back(Row());
            moveAssign(mResultRows.back(), util::move(iter.get()));
        }
        testbench().asyncDeleteTable();
    }

private:
    Optional<OTSError>& mOutError;
    deque<Row>& mResultRows;
    BatchWriteRowResponse& mBwrResponse;
    PutRowRequest& mPutRowRequest;
    BatchWriteRowRequest& mBwrRequest;
    Thread mReadRowsThread;
};

void AsyncBatchWriteRow(const string& csname)
{
    UtcTime ts = UtcTime::fromMsec(UtcTime::now().toMsec());
    Optional<OTSError> err;
    PutRowRequest prReq;
    {
        prReq.mutableRowChange().mutableTable() = csname;
        prReq.mutableRowChange().mutablePrimaryKey().append() =
            PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(3));
    }
    const char kPut[] = "kPut";
    const char kUpdate[] = "UPDATE";
    const char kDelete[] = "kDelete";
    DequeBasedVector<Row> oracle;
    BatchWriteRowRequest bwrReq;
    {
        BatchWriteRowRequest::Put& put = bwrReq.mutablePuts().append();
        put.mutableUserData() =  kPut;
        put.mutableGet().mutableTable() = csname;
        put.mutableGet().mutablePrimaryKey().append() =
            PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(1));
        put.mutableGet().mutableReturnType() = RowChange::kRT_PrimaryKey;

        Row& res = oracle.append();
        res.mutablePrimaryKey() = put.get().primaryKey();
    }
    {
        BatchWriteRowRequest::Update& update = bwrReq.mutableUpdates().append();
        update.mutableUserData() =  kUpdate;
        update.mutableGet().mutableTable() = csname;
        update.mutableGet().mutablePrimaryKey().append() =
            PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(2));
        RowUpdateChange::Update& c = update.mutableGet().mutableUpdates()
            .append();
        c.mutableType() = RowUpdateChange::Update::kPut;
        c.mutableAttrName() = "C";
        c.mutableAttrValue().reset(AttributeValue::toStr("c"));
        c.mutableTimestamp().reset(ts);
        update.mutableGet().mutableReturnType() = RowChange::kRT_PrimaryKey;

        Row& res = oracle.append();
        res.mutablePrimaryKey() = update.get().primaryKey();
        res.mutableAttributes().append() = Attribute(c.attrName(), *c.attrValue(), *c.timestamp());
    }
    {
        BatchWriteRowRequest::Delete& del = bwrReq.mutableDeletes().append();
        del.mutableUserData() =  kDelete;
        del.mutableGet().mutableTable() = csname;
        del.mutableGet().mutablePrimaryKey().append() =
            PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(3));
        del.mutableGet().mutableReturnType() = RowChange::kRT_PrimaryKey;
    }
    BatchWriteRowResponse bwrResp;
    deque<Row> trialRows;
    Testbench tb(err, csname);
    BatchWriteRowTrial trial(tb, err, trialRows, bwrResp, prReq, bwrReq);
    tb.gogogo(trial);
    trial.close();

    TESTA_ASSERT(!err.present())
        (*err).issue();

    TESTA_ASSERT(bwrResp.putResults().size() == 1)
        (bwrResp).issue();
    TESTA_ASSERT(bwrResp.putResults()[0].userData() == kPut)
        (bwrResp).issue();
    TESTA_ASSERT(bwrResp.updateResults().size() == 1)
        (bwrResp).issue();
    TESTA_ASSERT(bwrResp.updateResults()[0].userData() == kUpdate)
        (bwrResp).issue();
    TESTA_ASSERT(bwrResp.deleteResults().size() == 1)
        (bwrResp).issue();
    TESTA_ASSERT(bwrResp.deleteResults()[0].userData() == kDelete)
        (bwrResp).issue();

    int64_t osz = oracle.size();
    int64_t tsz = trialRows.size();
    for(int64_t i = 0; i < osz && i < tsz; ++i) {
        const Row& orc = oracle[i];
        const Row& tri = trialRows[i];
        TESTA_ASSERT(pp::prettyPrint(tri) == pp::prettyPrint(orc))
            (tri)(orc)(i).issue();
    }
    if (osz < tsz) {
        TESTA_ASSERT(osz == tsz)
            (osz)
            (tsz)
            (trialRows[osz])
            .issue("oracle is smaller than trial.");
    } else if (osz > tsz) {
        TESTA_ASSERT(osz == tsz)
            (osz)
            (tsz)
            (oracle[tsz])
            .issue("oracle is larger than trial.");
    }
}
TESTA_DEF_JUNIT_LIKE1(AsyncBatchWriteRow);

class ComputeSplitsBySizeTrial: public Trial
{
public:
    explicit ComputeSplitsBySizeTrial(
        Testbench& tb,
        Optional<OTSError>& outErr,
        ComputeSplitsBySizeResponse& resp,
        ComputeSplitsBySizeRequest& req)
      : Trial(tb),
        mOutError(outErr),
        mResp(resp),
        mReq(req)
    {}

    void asyncDo()
    {
        testbench().client().computeSplitsBySize(mReq,
            bind(&ComputeSplitsBySizeTrial::callbackComputeSplitsBySize,
                this, _1, _2, _3));
    }

    void callbackComputeSplitsBySize(
        ComputeSplitsBySizeRequest& req,
        Optional<OTSError>& err,
        ComputeSplitsBySizeResponse& resp)
    {
        moveAssign(mReq, util::move(req));
        if (err.present()) {
            OTS_LOG_ERROR(testbench().logger())
                ("Error", *err)
                .what("FT: fail to put row");
            moveAssign(mOutError, util::move(err));
            testbench().asyncDeleteTable();
        } else {
            OTS_LOG_DEBUG(testbench().logger())
                .what("FT: row put");
            moveAssign(mResp, util::move(resp));
            testbench().asyncDeleteTable();
        }
    }

private:
    Optional<OTSError>& mOutError;
    ComputeSplitsBySizeResponse& mResp;
    ComputeSplitsBySizeRequest& mReq;
};

void AsyncComputeSplitsBySize(const string& csname)
{
    Optional<OTSError> err;
    ComputeSplitsBySizeRequest req;
    req.mutableTable() = csname;
    req.mutableSplitSize() = 1; // 100MB
    ComputeSplitsBySizeResponse resp;
    Testbench tb(err, csname);
    ComputeSplitsBySizeTrial trial(tb, err, resp, req);
    tb.gogogo(trial);

    TESTA_ASSERT(!err.present())
        (*err).issue();
    TESTA_ASSERT(resp.splits().size() == 1)
        (resp).issue();
}
TESTA_DEF_JUNIT_LIKE1(AsyncComputeSplitsBySize);

class BatchWriterTrial : public Trial
{
public:
    explicit BatchWriterTrial(Testbench& tb, deque<PrimaryKeyValue>& rows)
      : Trial(tb),
        mRows(rows),
        mOngoingReqs(0)
    {
        AsyncBatchWriter* w = NULL;
        Optional<OTSError> err = AsyncBatchWriter::create(
            w,
            testbench().client(),
            BatchWriterConfig());
        TESTA_ASSERT(!err.present())
            (*err).issue();
        mWriter.reset(w);
    }

    void asyncDo()
    {
        mOngoingReqs.fetch_add(2, boost::memory_order_acq_rel);
        {
            PutRowRequest req;
            req.mutableRowChange().mutableTable() = testbench().caseName();
            req.mutableRowChange().mutablePrimaryKey().append() =
                PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(0));
            mWriter->putRow(
                req,
                bind(&BatchWriterTrial::writeCallback,
                    this, _1, _2, _3));
        }
        {
            PutRowRequest req;
            req.mutableRowChange().mutableTable() = testbench().caseName();
            req.mutableRowChange().mutablePrimaryKey().append() =
                PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(1));

            mWriter->putRow(
                req,
                bind(&BatchWriterTrial::writeCallback,
                    this, _1, _2, _3));
        }
    }

    void writeCallback(PutRowRequest& req, Optional<OTSError>& err, PutRowResponse& resp)
    {
        if (err.present()) {
            OTS_LOG_ERROR(testbench().logger())
                ("Request", req)
                ("Error", *err);
        } else {
            OTS_LOG_DEBUG(testbench().logger())
                ("Request", req)
                ("Response", resp);
        }
        int64_t ongoing = mOngoingReqs.fetch_sub(1, boost::memory_order_acq_rel) - 1;
        OTS_LOG_DEBUG(testbench().logger())
            ("RequestsOnTheFly", ongoing);
        if (ongoing == 0) {
            mActor.pushBack(bind(&BatchWriterTrial::scanTable, this));
        }
    }

    void scanTable()
    {
        RangeQueryCriterion cri;
        cri.mutableInclusiveStart().append() =
            PrimaryKeyColumn("pkey", PrimaryKeyValue::toInfMin());
        cri.mutableExclusiveEnd().append() =
            PrimaryKeyColumn("pkey", PrimaryKeyValue::toInfMax());
        cri.mutableTable() = testbench().caseName();
        cri.mutableMaxVersions().reset(1);
        try {
            RangeIterator iter(testbench().client(), cri);
            for(;;) {
                Optional<OTSError> e = iter.moveNext();
                if (e.present()) {
                    OTS_LOG_DEBUG(testbench().logger())
                        ("Error", *e)
                        .what("RI: Error on range iterator");
                    TESTA_ASSERT(false)
                        (*e).issue();
                    break;
                }
                if (!iter.valid()) {
                    OTS_LOG_DEBUG(testbench().logger())
                        ("RowCount", mRows.size())
                        .what("RI: finish on range iterator");
                    break;
                }
                const Row& r = iter.get();
                const PrimaryKey& pk = r.primaryKey();
                TESTA_ASSERT(pk.size() == 1)
                    (pk.size()).issue();
                mRows.push_back(pk[0].value());
            }
        }
        catch(const std::logic_error&) {
        }
        testbench().asyncDeleteTable();
    }

private:
    deque<PrimaryKeyValue>& mRows;
    boost::atomic<int64_t> mOngoingReqs;
    auto_ptr<AsyncBatchWriter> mWriter;
    Actor mActor;
};

void AsyncBatchWriter(const string& csname)
{
    Optional<OTSError> err;
    Testbench tb(err, csname);
    deque<PrimaryKeyValue> pkeys;
    BatchWriterTrial trial(tb, pkeys);
    tb.gogogo(trial);

    TESTA_ASSERT(pp::prettyPrint(pkeys) == "[0,1]")
        (pkeys).issue();
}
TESTA_DEF_JUNIT_LIKE1(AsyncBatchWriter);

} // namespace
} // namespace core
} // namespace tablestore
} // namespace aliyun

