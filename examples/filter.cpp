#include "tablestore/core/types.hpp"
#include "tablestore/core/client.hpp"
#include "tablestore/core/range_iterator.hpp"
#include "tablestore/util/logging.hpp"
#include <tr1/memory>
#include <iostream>
#include <memory>
#include <string>
#include <cassert>
extern "C" {
#include <unistd.h>
}

using namespace std;
using namespace std::tr1;
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

const char kTableName[] = "filter";

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
            pkColSchema.mutableType() = kPKT_Integer;
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

void writeRows(SyncClient& client)
{
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
                    PrimaryKeyValue::toInteger(1));
            }
            {
                // set attributes of the row to put
                IVector<Attribute>& attrs = chg.mutableAttributes();
                attrs.append() = Attribute(
                    "attr",
                    AttributeValue::toStr("A"));
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
                    PrimaryKeyValue::toInteger(2));
            }
            {
                // set attributes of the row to put
                IVector<Attribute>& attrs = chg.mutableAttributes();
                attrs.append() = Attribute(
                    "attr",
                    AttributeValue::toStr("B"));
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
                    PrimaryKeyValue::toInteger(3));
            }
            {
                // set attributes of the row to put
                IVector<Attribute>& attrs = chg.mutableAttributes();
                attrs.append() = Attribute(
                    "attr",
                    AttributeValue::toStr("A"));
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
}

void scan(SyncClient& client)
{
    RangeQueryCriterion query;
    query.mutableTable() = kTableName;
    query.mutableMaxVersions().reset(1);
    {
        PrimaryKey& start = query.mutableInclusiveStart();
        start.append() = PrimaryKeyColumn(
            "pkey",
            PrimaryKeyValue::toInfMin());
    }
    {
        PrimaryKey& end = query.mutableExclusiveEnd();
        end.append() = PrimaryKeyColumn(
            "pkey",
            PrimaryKeyValue::toInfMax());
    }
    {
        // set filter
        shared_ptr<ColumnCondition> pkeyCond(
            new SingleColumnCondition(
                "pkey",
                SingleColumnCondition::kLarger,
                AttributeValue::toInteger(1)));
        shared_ptr<ColumnCondition> attrCond(
            new SingleColumnCondition(
                "attr",
                SingleColumnCondition::kEqual,
                AttributeValue::toStr("A")));
        shared_ptr<CompositeColumnCondition> top(new CompositeColumnCondition());
        top->mutableOp() = CompositeColumnCondition::kAnd;
        top->mutableChildren().append() = pkeyCond;
        top->mutableChildren().append() = attrCond;
        query.mutableFilter() = top;
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
    writeRows(*client);
    scan(*client);
    deleteTable(*client);
    return 0;
}

