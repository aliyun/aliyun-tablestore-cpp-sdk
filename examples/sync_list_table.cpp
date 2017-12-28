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

void syncListTable()
{
    Endpoint ep("YourEndpoint", "YourInstance");
    Credential cr("AccessKeyId", "AccessKeySecret");
    ClientOptions opts;
    SyncClient* pclient = NULL;
    {
        Optional<OTSError> res = SyncClient::create(pclient, ep, cr, opts);
        assert(!res.present());
    }
    auto_ptr<SyncClient> client(pclient);
    sleep(30); // wait a while for connection ready
    ListTableRequest req;
    ListTableResponse resp;
    Optional<OTSError> res = client->listTable(resp, req);
    assert(!res.present());
    const IVector<string>& xs = resp.tables();
    for(int64_t i = 0; i < xs.size(); ++i) {
        cout << xs[i] << endl;
    }
}

int main() {
    syncListTable();
    return 0;
}

