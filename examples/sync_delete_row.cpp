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
    sleep(30); // wait a while for connection ready
    return pclient;
}

const char kTableName[] = "delete_row";

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

void writeRow(SyncClient& client)
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
                PrimaryKeyValue::toStr("exists"));
        }
    }
    PutRowResponse resp;
    Optional<OTSError> res = client.putRow(resp, req);
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

void deleteRow(SyncClient& client)
{
    {
        DeleteRowRequest req;
        {
            RowDeleteChange& chg = req.mutableRowChange();
            chg.mutableTable() = kTableName;
            {
                // set primary key of the row to delete
                PrimaryKey& pkey = chg.mutablePrimaryKey();
                pkey.append() = PrimaryKeyColumn(
                    "pkey",
                    PrimaryKeyValue::toStr("exists"));
            }
        }
        DeleteRowResponse resp;
        Optional<OTSError> res = client.deleteRow(resp, req);
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
    {
        DeleteRowRequest req;
        {
            RowDeleteChange& chg = req.mutableRowChange();
            chg.mutableTable() = kTableName;
            {
                // set primary key of the row to delete
                PrimaryKey& pkey = chg.mutablePrimaryKey();
                pkey.append() = PrimaryKeyColumn(
                    "pkey",
                    PrimaryKeyValue::toStr("nonexists"));
            }
        }
        DeleteRowResponse resp;
        Optional<OTSError> res = client.deleteRow(resp, req);
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
}

int main() {
    auto_ptr<SyncClient> client(initOtsClient());
    createTable(*client);
    writeRow(*client);
    deleteRow(*client);
    deleteTable(*client);
    return 0;
}

