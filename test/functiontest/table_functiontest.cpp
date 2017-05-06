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
#include "tablestore/core/types.hpp"
#include "tablestore/core/client.hpp"
#include "tablestore/util/logging.hpp"
#include <tr1/functional>
#include <tr1/tuple>
#include <set>
#include <stdexcept>

using namespace std;
using namespace std::tr1;
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
        Optional<Error> res = SyncClient::create(pclient, ep, cr, opts);
        TESTA_ASSERT(!res.present())
            (*res)(ep)(cr)(opts).issue();
    }
    auto_ptr<SyncClient> client(pclient);
    try {
        {
            CreateTableRequest req;
            req.mutableMeta().mutableTableName() = name;
            req.mutableMeta().mutableSchema().append() =
                PrimaryKeyColumnSchema("pkey", kPKT_Integer);
            CreateTableResponse resp;
            Optional<Error> res = client->createTable(resp, req);
            TESTA_ASSERT(!res.present())
                (*res)(req).issue();
        }
        cs(make_tuple(pclient, name));
        {
            DeleteTableRequest req;
            req.mutableTable() = name;
            DeleteTableResponse resp;
            Optional<Error> res = client->deleteTable(resp, req);
            TESTA_ASSERT(!res.present())
                (*res)(req).issue();
        }
    } catch(const std::logic_error& ex) {
        DeleteTableRequest req;
        req.mutableTable() = name;
        DeleteTableResponse resp;
        Optional<Error> res = client->deleteTable(resp, req);
        throw;
    }
}

void ListTable_verify(const set<string>& tables, const tuple<SyncClient*, string>& in)
{
    string csname = get<1>(in);
    TESTA_ASSERT(tables.find(csname) != tables.end())
        (tables)(csname).issue();
}

set<string> ListTable(const tuple<SyncClient*, string>& in)
{
    SyncClient* client = get<0>(in);
    ListTableRequest req;
    ListTableResponse resp;
    Optional<Error> res = client->listTable(resp, req);
    TESTA_ASSERT(!res.present())
        (*res).issue();
    set<string> tables;
    const IVector<string>& xs = resp.tables();
    for(int64_t i = 0; i < xs.size(); ++i) {
        tables.insert(xs[i]);
    }
    return tables;
}

} // namespace
TESTA_DEF_VERIFY_WITH_TB(ListTable, Table_tb, ListTable_verify, ListTable);

namespace {

DescribeTableResponse DescribeTable(const tuple<SyncClient*, string>& in)
{
    DescribeTableRequest req;
    req.mutableTable() = get<1>(in);
    DescribeTableResponse resp;
    Optional<Error> err = get<0>(in)->describeTable(resp, req);
    TESTA_ASSERT(!err.present())
        (*err)(req).issue();
    return resp;
}

void DescribeTable_verify(
    const DescribeTableResponse& resp,
    const tuple<SyncClient*, string>& in)
{
    const TableMeta& meta = resp.meta();
    TESTA_ASSERT(meta.tableName() == get<1>(in))
        (get<1>(in))(resp).issue();
    TESTA_ASSERT(pp::prettyPrint(meta.schema()) == "[{\"pkey\":kPKT_Integer}]")
        (resp).issue();
    const TableOptions& opts = resp.options();
    TESTA_ASSERT(pp::prettyPrint(opts) == "{"
        "\"ReservedThroughput\":{\"Read\":0,\"Write\":0},"
        "\"TimeToLive\":-1,\"MaxVersions\":1,\"MaxTimeDeviation\":86400}")
        (resp).issue();
    TESTA_ASSERT(resp.status() == kTS_Active)
        (resp).issue();
    TESTA_ASSERT(resp.shardSplitPoints().size() == 0)
        (resp).issue();
}

} // namespace
TESTA_DEF_VERIFY_WITH_TB(DescribeTable, Table_tb, DescribeTable_verify, DescribeTable);

namespace {

UpdateTableRequest UpdateTable(const tuple<SyncClient*, string>& in)
{
    UpdateTableRequest req;
    req.mutableTable() = get<1>(in);
    req.mutableOptions().mutableMaxVersions().reset(2);
    UpdateTableResponse resp;
    Optional<Error> err = get<0>(in)->updateTable(resp, req);
    TESTA_ASSERT(!err.present())
        (req).issue();
    return req;
}

void UpdateTable_verify(
    const UpdateTableRequest& req,
    const tuple<SyncClient*, string>& in)
{
    DescribeTableRequest dreq;
    dreq.mutableTable() = get<1>(in);
    DescribeTableResponse resp;
    Optional<Error> err = get<0>(in)->describeTable(resp, dreq);
    TESTA_ASSERT(!err.present())
        (*err)(dreq).issue();
    TESTA_ASSERT(resp.options().maxVersions().present())
        (req)(resp).issue();
    TESTA_ASSERT(*resp.options().maxVersions() == *req.options().maxVersions())
        (req)(resp).issue();
}

} // namespace
TESTA_DEF_VERIFY_WITH_TB(UpdateTable, Table_tb, UpdateTable_verify, UpdateTable);

} // namespace core
} // namespace tablestore
} // namespace aliyun
