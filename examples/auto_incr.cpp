#include "tablestore/core/types.hpp"
#include "tablestore/core/client.hpp"
#include "tablestore/core/range_iterator.hpp"
#include "tablestore/util/logging.hpp"
#include <iostream>
#include <memory>
#include <string>
#include <cassert>
extern "C" {
#include <unistd.h>
}

using namespace std;
using namespace aliyun::tablestore;
using namespace aliyun::tablestore::util;
using namespace aliyun::tablestore::core;

SyncClient* initOtsClient()
{
    Endpoint ep("YourEndpoint", "YourInstance");
    Credential cr("AccessKeyId", "AccessKeySecret");
    ClientOptions opts;
    SyncClient* pclient = NULL;
    {
        Optional<OTSError> res = SyncClient::create(pclient, ep, cr, opts);
        assert(!res.present());
    }
    sleep(30); // wait a while for connection ready
    return pclient;
}

const char kTableName[] = "auto_incr";

void createTable(SyncClient& client)
{
    CreateTableRequest req;
    {
        // immutable configurations of the table
        TableMeta& meta = req.mutableMeta();
        meta.mutableTableName() = kTableName;
        Schema& schema = meta.mutableSchema();
        {
            PrimaryKeyColumnSchema& pkColSchema = schema.append();
            pkColSchema.mutableName() = "ShardKey";
            pkColSchema.mutableType() = kPKT_String;
        }
        {
            PrimaryKeyColumnSchema& pkColSchema = schema.append();
            pkColSchema.mutableName() = "AutoIncrKey";
            pkColSchema.mutableType() = kPKT_Integer;
            pkColSchema.mutableOption().reset(PrimaryKeyColumnSchema::AutoIncrement);
        }
    }
    CreateTableResponse resp;
    Optional<OTSError> res = client.createTable(resp, req);
    cout << "create table \"" << kTableName << "\" ";
    if (res.present()) {
        cout << "error" << endl
             << "  error code: " << res->errorCode() << endl
             << "  message: " << res->message() << endl
             << "  HTTP status: " << res->httpStatus() << endl
             << "  request id: " << res->requestId() << endl
             << "  trace id: " << res->traceId() << endl;
    } else {
        cout << "OK" << endl
             << "  request id: " << resp.requestId() << endl
             << "  trace id: " << resp.traceId() << endl;
    }
}

void deleteTable(SyncClient& client)
{
    DeleteTableRequest req;
    req.mutableTable() = kTableName;
    DeleteTableResponse resp;
    Optional<OTSError> res = client.deleteTable(resp, req);
    cout << "delete table \"" << kTableName << "\" ";
    if (res.present()) {
        cout << "error" << endl
             << "  error code: " << res->errorCode() << endl
             << "  message: " << res->message() << endl
             << "  HTTP status: " << res->httpStatus() << endl
             << "  request id: " << res->requestId() << endl
             << "  trace id: " << res->traceId() << endl;
    } else {
        cout << "OK" << endl
             << "  request id: " << resp.requestId() << endl
             << "  trace id: " << resp.traceId() << endl;
    }
}

void writeOneRow(SyncClient& client)
{
    PutRowRequest req;
    {
        RowPutChange& chg = req.mutableRowChange();
        chg.mutableTable() = kTableName;
        chg.mutableReturnType() = RowChange::kRT_PrimaryKey;
        PrimaryKey& pkey = chg.mutablePrimaryKey();
        pkey.append() = PrimaryKeyColumn(
            "ShardKey",
            PrimaryKeyValue::toStr("shard0"));
        pkey.append() = PrimaryKeyColumn(
            "AutoIncrKey",
            PrimaryKeyValue::toAutoIncrement());
    }
    PutRowResponse resp;
    Optional<OTSError> res = client.putRow(resp, req);
    cout << "put row \"" << pp::prettyPrint(req.rowChange().primaryKey()) << "\" ";
    if (res.present()) {
        cout << "error" << endl
             << "  error code: " << res->errorCode() << endl
             << "  message: " << res->message() << endl
             << "  HTTP status: " << res->httpStatus() << endl
             << "  request id: " << res->requestId() << endl
             << "  trace id: " << res->traceId() << endl;
    } else {
        cout << "OK" << endl
             << "  "
             << pp::prettyPrint(resp) << endl;
    }
}

void scan(SyncClient& client)
{
    RangeQueryCriterion query;
    query.mutableTable() = kTableName;
    query.mutableMaxVersions().reset(1);
    {
        PrimaryKey& start = query.mutableInclusiveStart();
        start.append() = PrimaryKeyColumn(
            "ShardKey",
            PrimaryKeyValue::toInfMin());
        start.append() = PrimaryKeyColumn(
            "AutoIncrKey",
            PrimaryKeyValue::toInfMin());
    }
    {
        PrimaryKey& end = query.mutableExclusiveEnd();
        end.append() = PrimaryKeyColumn(
            "ShardKey",
            PrimaryKeyValue::toInfMax());
        end.append() = PrimaryKeyColumn(
            "AutoIncrKey",
            PrimaryKeyValue::toInfMax());
    }
    auto_ptr<AsyncClient> aclient(AsyncClient::create(client));
    RangeIterator iter(*aclient, query);
    cout << "start scanning" << endl;
    for(;;) {
        Optional<OTSError> err = iter.moveNext();
        if (err.present()) {
            cout << "error" << endl
                 << "  error code: " << err->errorCode() << endl
                 << "  message: " << err->message() << endl
                 << "  HTTP status: " << err->httpStatus() << endl
                 << "  request id: " << err->requestId() << endl
                 << "  trace id: " << err->traceId() << endl;
            break;
        }
        if (!iter.valid()) {
            break;
        }
        const Row& row = iter.get();
        cout << pp::prettyPrint(row) << endl;
    }
    cout << "end of scanning" << endl;
}

int main() {
    auto_ptr<SyncClient> client(initOtsClient());
    createTable(*client);
    writeOneRow(*client);
    writeOneRow(*client);
    scan(*client);
    deleteTable(*client);
    return 0;
}

