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
#include "async_client_base.hpp"
#include "serde.hpp"
#include "api_traits.hpp"
#include "ots_constants.hpp"
#include "ots_helper.hpp"
#include "../http/client.hpp"
#include "../http/asio.hpp"
#include "tablestore/core/types.hpp"
#include "tablestore/core/retry.hpp"
#include "tablestore/util/security.hpp"
#include "tablestore/util/mempool.hpp"
#include "tablestore/util/logging.hpp"
#include "tablestore/util/timestamp.hpp"
#include "tablestore/util/foreach.hpp"
#include "tablestore/util/try.hpp"
#include <boost/ref.hpp>

using namespace std;
using namespace std::tr1;
using namespace std::tr1::placeholders;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {
namespace impl {

Optional<OTSError> AsyncClientBase::create(
    AsyncClientBase*& result,
    Endpoint& ep, Credential& cr, ClientOptions& opts)
{
    TRY(ep.validate());
    TRY(cr.validate());
    TRY(opts.validate());
    http::Endpoint hep;
    {
        Optional<string> err = http::Endpoint::parse(hep, ep.endpoint());
        if (err.present()) {
            OTSError e(OTSError::kPredefined_OTSParameterInvalid);
            e.mutableMessage() = *err;
            return Optional<OTSError>(util::move(e));
        }
    }

    result = new AsyncClientBase(hep, ep.instanceName(), cr, opts);

    return Optional<OTSError>();
}

namespace {

http::Client* defaultHttpClient(
    Logger& logger,
    MemPool& mpool,
    const http::Endpoint& ep,
    const http::Headers& fixedHeaders,
    http::Asio& asio,
    const deque<shared_ptr<Actor> >& actors)
{
    return http::Client::create(
        logger,
        mpool,
        ep,
        fixedHeaders,
        asio,
        actors);
}

} // namespace

AsyncClientBase::AsyncClientBase(
    const http::Endpoint& ep,
    const string& inst,
    Credential& cr,
    ClientOptions& opts)
  : mClose(false),
    mLogger(opts.releaseLogger()),
    mHttpLogger(mLogger->spawn("http")),
    mCredential(util::move(cr)),
    mAsio(
        http::Asio::create(
            *mHttpLogger,
            opts.maxConnections(),
            opts.connectTimeout(),
            ep,
            opts.actors())),
    mMemPool(new IncrementalMemPool()),
    mRequestTimeout(opts.requestTimeout()),
    mOngoingRequests(0),
    mRetryStrategy(opts.releaseRetryStrategy()),
    mActors(opts.actors())
{
    init(inst,
        bind(defaultHttpClient,
            boost::ref(*mHttpLogger),
            boost::ref(*mMemPool),
            boost::cref(ep),
            _1,
            boost::ref(*mAsio),
            boost::cref(mActors)));
}

AsyncClientBase::AsyncClientBase(
    http::Asio* asio,
    const function<http::Client*(const http::Headers&)>& httpClientFactory,
    Credential& cr,
    ClientOptions& opts)
  : mClose(false),
    mLogger(opts.releaseLogger()),
    mCredential(util::move(cr)),
    mAsio(asio),
    mMemPool(new IncrementalMemPool()),
    mRequestTimeout(opts.requestTimeout()),
    mOngoingRequests(0),
    mRetryStrategy(opts.releaseRetryStrategy()),
    mActors(opts.actors())
{
    init("testinstance", httpClientFactory);
}

void AsyncClientBase::init(
    const string& inst,
    const function<http::Client*(const http::Headers&)>& httpClientFactory)
{
    OTS_ASSERT(!mActors.empty())
        (mActors.size())
        .what("AsyncClient: at least one Actor is required.");
    OTS_ASSERT(mRetryStrategy.get() != NULL)
        .what("AsyncClient: retry strategy is required.");

    mFixedHeaders[kOTSAPIVersion] = kAPIVersion;
    mFixedHeaders[kOTSAccessKeyId] = mCredential.accessKeyId();
    mFixedHeaders[kOTSInstanceName] = inst;
    mFixedHeaders[kUserAgent] = kSDKUserAgent;
    mFixedHeaders[kHttpContentType] = kMimeType;
    mFixedHeaders[kHttpAccept] = kMimeType;
    if (!mCredential.securityToken().empty()) {
        mFixedHeaders[kOTSStsToken] = mCredential.securityToken();
    }
    mHttp.reset(httpClientFactory(mFixedHeaders));
}

AsyncClientBase::~AsyncClientBase()
{
    mClose.store(true, boost::memory_order_release);

    countDown(*mLogger, mOngoingRequests,
        "AsyncClient: wait for finishing all request.",
        "AsyncClient: all requests were finished.");
    mHttp->close();
    mAsio->close();
}

string AsyncClientBase::sign(
    const string& path,
    const http::InplaceHeaders& add)
{
    MemPiece prefix = MemPiece::from(kOTSHeaderPrefix);
    map<MemPiece, MemPiece, LexicographicLess<MemPiece> > headers;
    FOREACH_ITER(i, mFixedHeaders) {
        if (MemPiece::from(i->first).startsWith(prefix)) {
            headers[MemPiece::from(i->first)] = MemPiece::from(i->second);
        }
    }
    FOREACH_ITER(i, add) {
        if (i->first.startsWith(prefix)) {
            headers[i->first] = i->second;
        }
    }

    HmacSha1 hmac(MemPiece::from(mCredential.accessKeySecret()));
    hmac.update(MemPiece::from(path));
    hmac.update(MemPiece::from("\nPOST\n\n"));

    FOREACH_ITER(i, headers) {
        hmac.update(i->first);
        hmac.update(MemPiece::from(":"));
        hmac.update(i->second);
        hmac.update(MemPiece::from("\n"));
    }

    uint8_t digest[HmacSha1::kLength];
    hmac.finalize(MutableMemPiece(digest, sizeof(digest)));
    Base64Encoder b64;
    b64.update(MemPiece::from(digest));
    b64.finalize();
    return b64.base64().to<string>();
}

Optional<OTSError> AsyncClientBase::validateResponse(
    const Tracker& tracker,
    const http::InplaceHeaders& headers,
    const deque<MemPiece>& body)
{
    http::InplaceHeaders::const_iterator it = headers.find(MemPiece::from(kOTSContentMD5));
    if (it == headers.end()) {
        return Optional<OTSError>();
    }

    MemPiece expect = it->second;
    string real = md5(body);
    if (expect != MemPiece::from(real)) {
        OTS_LOG_DEBUG(*mLogger)
            ("Tracker", tracker)
            ("Expect", expect)
            ("Real", real)
            .what("AsyncClient: response digest mismatches.");
        OTSError e(OTSError::kPredefined_CorruptedResponse);
        string msg = "response digest mismatches: expect=";
        msg.append((char*) expect.data(), expect.length());
        msg.append(", real=");
        msg.append(real);
        e.mutableMessage() = msg;
        return Optional<OTSError>(util::move(e));
    }
    return Optional<OTSError>();
}

} // namespace impl
} // namespace core
} // namespace tablestore
} // namespace aliyun
