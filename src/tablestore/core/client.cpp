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
#include "client.hpp"
#include "impl/sync_client.hpp"
#include "impl/async_client.hpp"
#include "impl/async_client_base.hpp"
#include "tablestore/util/try.hpp"
#include "tablestore/util/assert.hpp"

using namespace std;
using namespace std::tr1;
using namespace std::tr1::placeholders;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {

Optional<OTSError> SyncClient::create(
    SyncClient*& result,
    Endpoint& ep, Credential& cr, ClientOptions& opts)
{
    impl::AsyncClientBase* ac = NULL;
    TRY(impl::AsyncClientBase::create(ac, ep, cr, opts));
    result = new impl::SyncClient(ac);

    return Optional<OTSError>();
}

SyncClient* SyncClient::create(AsyncClient& a)
{
    impl::AsyncClient* c = dynamic_cast<impl::AsyncClient*>(&a);
    OTS_ASSERT(c != NULL)
        .what("It is only for a production-use asynchronous client "
            "to share its backbone with a synchronous one");
    return new impl::SyncClient(*c);
}

Optional<OTSError> AsyncClient::create(
    AsyncClient*& result,
    Endpoint& ep,
    Credential& cr,
    ClientOptions& opts)
{
    impl::AsyncClientBase* ac = NULL;
    TRY(impl::AsyncClientBase::create(ac, ep, cr, opts));
    result = new impl::AsyncClient(ac);

    return Optional<OTSError>();
}

AsyncClient* AsyncClient::create(SyncClient& a)
{
    impl::SyncClient* c = dynamic_cast<impl::SyncClient*>(&a);
    OTS_ASSERT(c != NULL)
        .what("It is only for a production-use synchronous client "
            "to share its backbone with an asynchronous one");
    return new impl::AsyncClient(*c);
}

} // namespace core
} // namespace tablestore
} // namespace aliyun
