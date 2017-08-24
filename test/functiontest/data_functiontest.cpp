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
#include "tablestore/core/range_iterator.hpp"
#include "tablestore/core/client.hpp"
#include "tablestore/core/types.hpp"
#include "tablestore/util/random.hpp"
#include "tablestore/util/logging.hpp"
#include <tr1/functional>
#include <tr1/tuple>
#include <set>
#include <vector>
#include <stdexcept>

using namespace std;
using namespace std::tr1;
using namespace std::tr1::placeholders;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {

namespace {

void Table_tb(
    const string& name,
    function<void(const tuple<SyncClient*, string>&)> cs)
{
    Endpoint ep;
    Credential cr;
    read(ep, cr);
    ep.mutableEndpoint() = "http://" + ep.endpoint();
    ClientOptions opts;
    opts.mutableRequestTimeout() = Duration::fromSec(30);
    opts.mutableMaxConnections() = 2;
    opts.resetLogger(createLogger("/", Logger::kDebug));

    SyncClient* pclient = NULL;
    {
        Optional<OTSError> res = SyncClient::create(pclient, ep, cr, opts);
        TESTA_ASSERT(!res.present())
            (*res)(ep)(cr)(opts).issue();
    }
    auto_ptr<SyncClient> client(pclient);
    for(;;) {
        {
            CreateTableRequest req;
            req.mutableMeta().mutableTableName() = name;
            req.mutableMeta().mutableSchema().append() =
                PrimaryKeyColumnSchema("pkey", kPKT_Integer);
            CreateTableResponse resp;
            Optional<OTSError> err = client->createTable(resp, req);
            if (!err.present()) {
                break;
            }
            if (err->errorCode() != "OTSObjectAlreadyExist") {
                TESTA_ASSERT(false)
                    (*err)(req).issue();
            }
        }
        {
            DeleteTableRequest req;
            req.mutableTable() = name;
            DeleteTableResponse resp;
            Optional<OTSError> res = client->deleteTable(resp, req);
            TESTA_ASSERT(!res.present())
                (*res)(req).issue();
        }
    }
    try {
        cs(make_tuple(pclient, name));
    } catch(const std::logic_error& ex) {
        DeleteTableRequest req;
        req.mutableTable() = name;
        DeleteTableResponse resp;
        Optional<OTSError> res = client->deleteTable(resp, req);
        (void) res;
        throw;
    }
    {
        DeleteTableRequest req;
        req.mutableTable() = name;
        DeleteTableResponse resp;
        Optional<OTSError> res = client->deleteTable(resp, req);
        TESTA_ASSERT(!res.present())
            (*res)(req).issue();
    }
}

PutRowRequest PutRow(const tuple<SyncClient*, string>& in)
{
    PutRowRequest req;
    req.mutableRowChange().mutableTable() = get<1>(in);
    req.mutableRowChange().mutablePrimaryKey().append() =
        PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(123));
    req.mutableRowChange().mutableAttributes().append() =
        Attribute("attr", AttributeValue::toStr("abc"));
    PutRowResponse resp;
    Optional<OTSError> err = get<0>(in)->putRow(resp, req);
    TESTA_ASSERT(!err.present())
        (req)(*err).issue();
    return req;
}

void GetRow(const PutRowRequest& putrow, const tuple<SyncClient*, string>& in)
{
    {
        GetRowRequest req;
        req.mutableQueryCriterion().mutableTable() = get<1>(in);
        req.mutableQueryCriterion().mutableMaxVersions().reset(1);
        req.mutableQueryCriterion().mutablePrimaryKey() = putrow.rowChange().primaryKey();
        GetRowResponse resp;
        Optional<OTSError> err = get<0>(in)->getRow(resp, req);
        TESTA_ASSERT(!err.present())
            (req)(*err).issue();

        const Optional<Row>& row = resp.row();
        TESTA_ASSERT(row.present())
            (req)(resp).issue();
        TESTA_ASSERT(row->primaryKey().compare(putrow.rowChange().primaryKey()) == kCR_Equivalent)
            (resp)(putrow).issue();
        TESTA_ASSERT(row->attributes().size() == 1)
            (resp)(putrow).issue();
        TESTA_ASSERT(putrow.rowChange().attributes().size() == 1)
            (resp)(putrow).issue();
        TESTA_ASSERT(row->attributes()[0].name() == putrow.rowChange().attributes()[0].name())
            (resp)(putrow).issue();
        TESTA_ASSERT(row->attributes()[0].value().compare(putrow.rowChange().attributes()[0].value()) == kCR_Equivalent)
            (resp)(putrow).issue();
    }
    {
        GetRowRequest req;
        req.mutableQueryCriterion().mutableTable() = get<1>(in);
        req.mutableQueryCriterion().mutableMaxVersions().reset(1);
        req.mutableQueryCriterion().mutablePrimaryKey().append() =
            PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(456));
        GetRowResponse resp;
        Optional<OTSError> err = get<0>(in)->getRow(resp, req);
        TESTA_ASSERT(!err.present())
            (req)(*err).issue();

        TESTA_ASSERT(!resp.row().present())
            (req)(resp).issue();
    }
}

} // namespace
TESTA_DEF_VERIFY_WITH_TB(PutRow_GetRow, Table_tb, GetRow, PutRow);

namespace {
void GetRange(const PutRowRequest& putrow, const tuple<SyncClient*, string>& in)
{
    RangeQueryCriterion cri;
    cri.mutableInclusiveStart().append() =
        PrimaryKeyColumn("pkey", PrimaryKeyValue::toInfMin());
    cri.mutableExclusiveEnd().append() =
        PrimaryKeyColumn("pkey", PrimaryKeyValue::toInfMax());
    cri.mutableTable() = get<1>(in);
    cri.mutableMaxVersions().reset(1);
    GetRangeRequest req;
    req.mutableQueryCriterion() = util::move(cri);
    GetRangeResponse resp;
    Optional<OTSError> err = get<0>(in)->getRange(resp, req);
    TESTA_ASSERT(!err.present())
        (req)(*err).issue();

    TESTA_ASSERT(resp.rows().size() == 1)
        (resp)(putrow).issue();
    const Row& row = resp.rows()[0];
    TESTA_ASSERT(row.primaryKey().compare(putrow.rowChange().primaryKey()) == kCR_Equivalent)
        (resp)(putrow).issue();
    TESTA_ASSERT(row.attributes().size() == 1)
        (resp)(putrow).issue();
    TESTA_ASSERT(row.attributes()[0].name() == putrow.rowChange().attributes()[0].name())
        (resp)(putrow).issue();
    TESTA_ASSERT(row.attributes()[0].value().compare(putrow.rowChange().attributes()[0].value()) == kCR_Equivalent)
        (resp)(putrow).issue();
}
} // namespace
TESTA_DEF_VERIFY_WITH_TB(PutRow_GetRange, Table_tb, GetRange, PutRow);

namespace {

void ScanTable(
    const DequeBasedVector<Row>& oracle,
    const tuple<SyncClient*, string>& in)
{
    RangeQueryCriterion cri;
    cri.mutableInclusiveStart().append() =
        PrimaryKeyColumn("pkey", PrimaryKeyValue::toInfMin());
    cri.mutableExclusiveEnd().append() =
        PrimaryKeyColumn("pkey", PrimaryKeyValue::toInfMax());
    cri.mutableTable() = get<1>(in);
    cri.mutableMaxVersions().reset(1000);
    SyncClient& client = *get<0>(in);
    auto_ptr<AsyncClient> aclient(AsyncClient::create(client));
    auto_ptr<RangeIterator> iter(new RangeIterator(*aclient, cri));
    DequeBasedVector<Row> trial;
    for(;;) {
        Optional<OTSError> err = iter->moveNext();
        TESTA_ASSERT(!err.present())
            (*err).issue();
        if (!iter->valid()) {
            break;
        }
        moveAssign(trial.append(), util::move(iter->get()));
    }

    int64_t osz = oracle.size();
    int64_t tsz = trial.size();
    for(int64_t i = 0; i < osz && i < tsz; ++i) {
        const Row& orc = oracle[i];
        const Row& tri = trial[i];
        TESTA_ASSERT(pp::prettyPrint(tri) == pp::prettyPrint(orc))
            (tri)(orc)(i).issue();
    }
    if (osz < tsz) {
        TESTA_ASSERT(osz == tsz)
            (osz)
            (tsz)
            (trial[osz])
            .issue("oracle is smaller than trial.");
    } else if (osz > tsz) {
        TESTA_ASSERT(osz == tsz)
            (osz)
            (tsz)
            (oracle[tsz])
            .issue("oracle is larger than trial.");
    }
}

DequeBasedVector<Row> UpdateRow(const tuple<SyncClient*, string>& in)
{
    UtcTime ts = UtcTime::fromMsec(UtcTime::now().toMsec());
    {
        PutRowRequest req;
        req.mutableRowChange().mutableTable() = get<1>(in);
        req.mutableRowChange().mutablePrimaryKey().append() =
            PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(123));
        req.mutableRowChange().mutableAttributes().append() =
            Attribute(
                "A",
                AttributeValue::toStr("a"),
                ts);
        req.mutableRowChange().mutableAttributes().append() =
            Attribute(
                "B",
                AttributeValue::toBlob("b"),
                ts);
        PutRowResponse resp;
        for(;;) {
            Optional<OTSError> err = get<0>(in)->putRow(resp, req);
            if (!err.present()) {
                break;
            } else if (err->errorCode() == "OTSTableNotReady") {
                sleepFor(Duration::fromSec(1));
            } else {
                TESTA_ASSERT(!err.present())
                    (req)(*err).issue();
            }
        }
    }
    {
        UpdateRowRequest req;
        req.mutableRowChange().mutableTable() = get<1>(in);
        req.mutableRowChange().mutablePrimaryKey().append() =
            PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(123));
        RowUpdateChange::Update& a = req.mutableRowChange().mutableUpdates()
            .append();
        a.mutableType() = RowUpdateChange::Update::kDelete;
        a.mutableAttrName() = "A";
        a.mutableTimestamp().reset(ts);
        RowUpdateChange::Update& b = req.mutableRowChange().mutableUpdates()
            .append();
        b.mutableType() = RowUpdateChange::Update::kDeleteAll;
        b.mutableAttrName() = "B";
        RowUpdateChange::Update& c = req.mutableRowChange().mutableUpdates()
            .append();
        c.mutableType() = RowUpdateChange::Update::kPut;
        c.mutableAttrName() = "C";
        c.mutableAttrValue().reset(AttributeValue::toStr("c"));
        c.mutableTimestamp().reset(ts);
        UpdateRowResponse resp;
        Optional<OTSError> err = get<0>(in)->updateRow(resp, req);
        TESTA_ASSERT(!err.present())
            (*err)(req).issue();

        DequeBasedVector<Row> result;
        Row& row = result.append();
        row.mutablePrimaryKey().append() = req.rowChange().primaryKey()[0];
        Attribute& attr = row.mutableAttributes().append();
        attr.mutableName() = req.rowChange().updates()[2].attrName();
        attr.mutableValue() = *req.rowChange().updates()[2].attrValue();
        attr.mutableTimestamp().reset(*req.rowChange().updates()[2].timestamp());
        return result;
    }
}
} // namespace
TESTA_DEF_VERIFY_WITH_TB(UpdateRow, Table_tb, ScanTable, UpdateRow);

namespace {
DequeBasedVector<Row> DeleteRow(const tuple<SyncClient*, string>& in)
{
    {
        PutRowRequest req;
        req.mutableRowChange().mutableTable() = get<1>(in);
        req.mutableRowChange().mutablePrimaryKey().append() =
            PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(123));
        PutRowResponse resp;
        for(;;) {
            Optional<OTSError> err = get<0>(in)->putRow(resp, req);
            if (!err.present()) {
                break;
            } else if (err->errorCode() == "OTSTableNotReady") {
                sleepFor(Duration::fromSec(1));
            } else {
                TESTA_ASSERT(!err.present())
                    (req)(*err).issue();
            }
        }
    }
    {
        DeleteRowRequest req;
        req.mutableRowChange().mutableTable() = get<1>(in);
        req.mutableRowChange().mutablePrimaryKey().append() =
            PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(123));
        DeleteRowResponse resp;
        Optional<OTSError> err = get<0>(in)->deleteRow(resp, req);
        TESTA_ASSERT(!err.present())
            (*err)(req).issue();

        DequeBasedVector<Row> result;
        return result;
    }
}
} // namespace
TESTA_DEF_VERIFY_WITH_TB(DeleteRow, Table_tb, ScanTable, DeleteRow);

namespace {
void BatchGetRow(const tuple<SyncClient*, string>& in)
{
    {
        PutRowRequest req;
        req.mutableRowChange().mutableTable() = get<1>(in);
        req.mutableRowChange().mutablePrimaryKey().append() =
            PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(123));
        PutRowResponse resp;
        for(;;) {
            Optional<OTSError> err = get<0>(in)->putRow(resp, req);
            if (!err.present()) {
                break;
            } else if (err->errorCode() == "OTSTableNotReady") {
                sleepFor(Duration::fromSec(1));
            } else {
                TESTA_ASSERT(!err.present())
                    (req)(*err).issue();
            }
        }
    }
    {
        const char kHitRowKey[] = "HitRowKey";
        const char kMissRowKey[] = "MissRowKey";
        BatchGetRowRequest req;
        {
            MultiPointQueryCriterion& cri = req.mutableCriteria().append();
            cri.mutableTable() = get<1>(in);
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
        BatchGetRowResponse resp;
        Optional<OTSError> err = get<0>(in)->batchGetRow(resp, req);
        TESTA_ASSERT(!err.present())
            (*err)(req).issue();

        const IVector<BatchGetRowResponse::Result>& results = resp.results();
        TESTA_ASSERT(results.size() == 2)
            (resp)(req).issue();

        TESTA_ASSERT(results[0].get().ok())
            (resp)(req).issue();
        TESTA_ASSERT(results[0].get().okValue().present())
            (resp)(req).issue();
        TESTA_ASSERT(pp::prettyPrint(*results[0].get().okValue()) ==
            "{\"PrimaryKey\":{\"pkey\":123},\"Attributes\":[]}")
            (resp)(req).issue();
        TESTA_ASSERT(results[0].userData() == kHitRowKey).issue();

        TESTA_ASSERT(results[1].get().ok())
            (resp)(req).issue();
        TESTA_ASSERT(!results[1].get().okValue().present())
            (resp)(req).issue();
        TESTA_ASSERT(results[1].userData() == kMissRowKey).issue();
    }
}

void BatchGetRow_verify(const string& csname)
{
    Table_tb(csname, bind(BatchGetRow, _1));
}
} // namespace
TESTA_DEF_JUNIT_LIKE2(BatchGetRow, BatchGetRow_verify);

namespace {
DequeBasedVector<Row> BatchWriteRow(const tuple<SyncClient*, string>& in)
{
    UtcTime ts = UtcTime::fromMsec(UtcTime::now().toMsec());
    {
        PutRowRequest req;
        req.mutableRowChange().mutableTable() = get<1>(in);
        req.mutableRowChange().mutablePrimaryKey().append() =
            PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(3));
        PutRowResponse resp;
        for(;;) {
            Optional<OTSError> err = get<0>(in)->putRow(resp, req);
            if (!err.present()) {
                break;
            } else if (err->errorCode() == "OTSTableNotReady") {
                sleepFor(Duration::fromSec(1));
            } else {
                TESTA_ASSERT(!err.present())
                    (req)(*err).issue();
            }
        }
    }
    {
        const char kPut[] = "kPut";
        const char kUpdate[] = "UPDATE";
        const char kDelete[] = "kDelete";
        DequeBasedVector<Row> result;

        BatchWriteRowRequest req;
        {
            BatchWriteRowRequest::Put& put = req.mutablePuts().append();
            put.mutableUserData() =  kPut;
            put.mutableGet().mutableTable() = get<1>(in);
            put.mutableGet().mutablePrimaryKey().append() =
                PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(1));
            put.mutableGet().mutableReturnType() = RowChange::kRT_PrimaryKey;

            Row& res = result.append();
            res.mutablePrimaryKey() = put.get().primaryKey();
        }
        {
            BatchWriteRowRequest::Update& update = req.mutableUpdates().append();
            update.mutableUserData() =  kUpdate;
            update.mutableGet().mutableTable() = get<1>(in);
            update.mutableGet().mutablePrimaryKey().append() =
                PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(2));
            RowUpdateChange::Update& c = update.mutableGet().mutableUpdates()
                .append();
            c.mutableType() = RowUpdateChange::Update::kPut;
            c.mutableAttrName() = "C";
            c.mutableAttrValue().reset(AttributeValue::toStr("c"));
            c.mutableTimestamp().reset(ts);
            update.mutableGet().mutableReturnType() = RowChange::kRT_PrimaryKey;

            Row& res = result.append();
            res.mutablePrimaryKey() = update.get().primaryKey();
            res.mutableAttributes().append() = Attribute(c.attrName(), *c.attrValue(), *c.timestamp());
        }
        {
            BatchWriteRowRequest::Delete& del = req.mutableDeletes().append();
            del.mutableUserData() =  kDelete;
            del.mutableGet().mutableTable() = get<1>(in);
            del.mutableGet().mutablePrimaryKey().append() =
                PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(3));
            del.mutableGet().mutableReturnType() = RowChange::kRT_PrimaryKey;
        }
        BatchWriteRowResponse resp;
        Optional<OTSError> err = get<0>(in)->batchWriteRow(resp, req);
        TESTA_ASSERT(!err.present())
            (*err)(req).issue();
        TESTA_ASSERT(resp.putResults().size() == 1)
            (resp)(req).issue();
        TESTA_ASSERT(resp.putResults()[0].userData() == kPut)
            (resp)(req).issue();
        TESTA_ASSERT(resp.updateResults().size() == 1)
            (resp)(req).issue();
        TESTA_ASSERT(resp.updateResults()[0].userData() == kUpdate)
            (resp)(req).issue();
        TESTA_ASSERT(resp.deleteResults().size() == 1)
            (resp)(req).issue();
        TESTA_ASSERT(resp.deleteResults()[0].userData() == kDelete)
            (resp)(req).issue();

        return result;
    }
}
} // namespace
TESTA_DEF_VERIFY_WITH_TB(BatchWriteRow, Table_tb, ScanTable, BatchWriteRow);

namespace {

ComputeSplitsBySizeResponse computeSplitsBySize(const tuple<SyncClient*, string>& in)
{
    ComputeSplitsBySizeRequest req;
    req.mutableTable() = get<1>(in);
    req.mutableSplitSize() = 1; // 100MB
    ComputeSplitsBySizeResponse resp;
    Optional<OTSError> err = get<0>(in)->computeSplitsBySize(resp, req);
    TESTA_ASSERT(!err.present())
        (*err)
        (req).issue();
    return resp;
}

void verifyComputeSplitsBySize(
    const ComputeSplitsBySizeResponse& result,
    const tuple<SyncClient*, string>& in)
{
    TESTA_ASSERT(result.splits().size() == 1)
        (result).issue();
}
} // namespace
TESTA_DEF_VERIFY_WITH_TB(ComputeSplitsBySize, Table_tb, verifyComputeSplitsBySize, computeSplitsBySize);


} // namespace core
} // namespace tablestore
} // namespace aliyun
