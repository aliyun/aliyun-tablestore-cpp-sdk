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
    sleep(5); // wait a while for connection ready
    return pclient;
}

const char kTableName[] = "batch_write_row";

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

void batchWrites(SyncClient& client)
{
    static const char kPutRow[] = "PutRow";
    static const char kUpdateRow[] = "UpdateRow";
    static const char kDeleteRow[] = "DeleteRow";

    BatchWriteRowRequest req;
    {
        // put row
        BatchWriteRowRequest::Put& put = req.mutablePuts().append();
        put.mutableUserData() = kPutRow;
        put.mutableGet().mutableTable() = kTableName;
        PrimaryKey& pkey = put.mutableGet().mutablePrimaryKey();
        pkey.append() = PrimaryKeyColumn(
            "pkey",
            PrimaryKeyValue::toStr("row to put"));
    }
    {
        // update row
        BatchWriteRowRequest::Update& update = req.mutableUpdates().append();
        update.mutableUserData() = kUpdateRow;
        update.mutableGet().mutableTable() = kTableName;
        PrimaryKey& pkey = update.mutableGet().mutablePrimaryKey();
        pkey.append() = PrimaryKeyColumn(
            "pkey",
            PrimaryKeyValue::toStr("row to update"));
        RowUpdateChange::Update& attr = update.mutableGet().mutableUpdates().append();
        attr.mutableType() = RowUpdateChange::Update::kPut;
        attr.mutableAttrName() = "attr0";
        attr.mutableAttrValue().reset(AttributeValue::toStr("some value"));
    }
    {
        // delete row
        BatchWriteRowRequest::Delete& del = req.mutableDeletes().append();
        del.mutableUserData() = kDeleteRow;
        del.mutableGet().mutableTable() = kTableName;
        PrimaryKey& pkey = del.mutableGet().mutablePrimaryKey();
        pkey.append() = PrimaryKeyColumn(
            "pkey",
            PrimaryKeyValue::toStr("row to delete"));
    }
    BatchWriteRowResponse resp;
    Optional<OTSError> res = client.batchWriteRow(resp, req);
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

void waitForTableReady(SyncClient& client)
{
    GetRowRequest req;
    {
        PointQueryCriterion& query = req.mutableQueryCriterion();
        {
            PrimaryKey& pkey = query.mutablePrimaryKey();
            pkey.append() = PrimaryKeyColumn(
                "pkey",
                PrimaryKeyValue::toStr("whatever"));
        }
        query.mutableTable() = kTableName;
        query.mutableMaxVersions().reset(1);
    }
    GetRowResponse resp;
    Optional<OTSError> res = client.getRow(resp, req);
}

int main() {
    auto_ptr<SyncClient> client(initOtsClient());
    createTable(*client);
    waitForTableReady(*client);
    batchWrites(*client);
    deleteTable(*client);
    return 0;
}

