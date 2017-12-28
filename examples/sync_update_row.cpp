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

const char kTableName[] = "update_row";

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

void updateRow(SyncClient& client)
{
    UpdateRowRequest req;
    {
        RowUpdateChange& chg = req.mutableRowChange();
        chg.mutableTable() = kTableName;
        {
            // set primary key of the row to put
            PrimaryKey& pkey = chg.mutablePrimaryKey();
            pkey.append() = PrimaryKeyColumn(
                "pkey",
                PrimaryKeyValue::toStr("pkey"));
        }
        {
            // insert a value without specifying version
            RowUpdateChange::Update& up = chg.mutableUpdates().append();
            up.mutableType() = RowUpdateChange::Update::kPut;
            up.mutableAttrName() = "attr0";
            up.mutableAttrValue().reset(AttributeValue::toStr("new value without specifying version"));
        }
        {
            // insert a value with version
            RowUpdateChange::Update& up = chg.mutableUpdates().append();
            up.mutableType() = RowUpdateChange::Update::kPut;
            up.mutableAttrName() = "attr1";
            up.mutableAttrValue().reset(AttributeValue::toStr("new value with version"));
            up.mutableTimestamp().reset(UtcTime::now());
        }
        {
            // delete a value with specific version
            RowUpdateChange::Update& up = chg.mutableUpdates().append();
            up.mutableType() = RowUpdateChange::Update::kDelete;
            up.mutableAttrName() = "attr2";
            up.mutableTimestamp().reset(UtcTime::now());
        }
        {
            // delete all values of a attribute column
            RowUpdateChange::Update& up = chg.mutableUpdates().append();
            up.mutableType() = RowUpdateChange::Update::kDeleteAll;
            up.mutableAttrName() = "attr3";
        }
    }
    UpdateRowResponse resp;
    Optional<OTSError> res = client.updateRow(resp, req);
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
    updateRow(*client);
    deleteTable(*client);
    return 0;
}

