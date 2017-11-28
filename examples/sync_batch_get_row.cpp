#include "tablestore/core/types.hpp"
#include "tablestore/core/client.hpp"
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

const char kTableName[] = "batch_get_row";
const char kExists[] = "exists";
const char kNonexists[] = "nonexists";


void createTable(SyncClient& client)
{
    CreateTableRequest req;
    {
        // immutable configurations of the table
        TableMeta& meta = req.mutableMeta();
        meta.mutableTableName() = kTableName;
        {
            // with exactly one integer primary key column
            Schema& schema = meta.mutableSchema();
            PrimaryKeyColumnSchema& pkColSchema = schema.append();
            pkColSchema.mutableName() = "pkey";
            pkColSchema.mutableType() = kPKT_String;
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
        {
            // set primary key of the row to put
            PrimaryKey& pkey = chg.mutablePrimaryKey();
            pkey.append() = PrimaryKeyColumn(
                "pkey",
                PrimaryKeyValue::toStr("exist"));
        }
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

void batchGetRow(SyncClient& client)
{
    BatchGetRowRequest req;
    {
        MultiPointQueryCriterion& criterion = req.mutableCriteria().append();
        IVector<MultiPointQueryCriterion::RowKey>& rowkeys = criterion.mutableRowKeys();
        {
            MultiPointQueryCriterion::RowKey& exist = rowkeys.append();
            exist.mutableGet().append() = PrimaryKeyColumn(
                "pkey",
                PrimaryKeyValue::toStr(kExists));
            exist.mutableUserData() = kExists;
        }
        {
            MultiPointQueryCriterion::RowKey& exist = rowkeys.append();
            exist.mutableGet().append() = PrimaryKeyColumn(
                "pkey",
                PrimaryKeyValue::toStr(kNonexists));
            exist.mutableUserData() = kNonexists;
        }
        criterion.mutableTable() = kTableName;
        criterion.mutableMaxVersions().reset(1);
    }
    BatchGetRowResponse resp;
    Optional<OTSError> res = client.batchGetRow(resp, req);
    cout << "batch get rows: ";
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

int main() {
    auto_ptr<SyncClient> client(initOtsClient());
    createTable(*client);
    writeOneRow(*client);
    batchGetRow(*client);
    deleteTable(*client);
    return 0;
}

