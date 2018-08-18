#pragma once
#ifndef TABLESTORE_CORE_IMPL_ASYNC_CLIENT_BASE_HPP
#define TABLESTORE_CORE_IMPL_ASYNC_CLIENT_BASE_HPP
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
#include "api_traits.hpp"
#include "serde.hpp"
#include "ots_constants.hpp"
#include "../http/types.hpp"
#include "../http/asio.hpp"
#include "../http/client.hpp"
#include "tablestore/core/types.hpp"
#include "tablestore/core/retry.hpp"
#include "tablestore/util/security.hpp"
#include "tablestore/util/timestamp.hpp"
#include "tablestore/util/mempool.hpp"
#include "tablestore/util/logging.hpp"
#include "tablestore/util/try.hpp"
#include <boost/atomic.hpp>
#include <tr1/functional>
#include <tr1/memory>
#include <memory>
#include <string>

namespace aliyun {
namespace tablestore {
namespace core {
namespace impl {

class AsyncClientBase
{
public:
    static util::Optional<OTSError> create(
        AsyncClientBase*&,
        Endpoint&,
        Credential&,
        ClientOptions&);

    explicit AsyncClientBase(
        const http::Endpoint&,
        const std::string& instance,
        Credential&,
        ClientOptions&);
    // for testing only
    explicit AsyncClientBase(
        http::Asio*,
        const std::tr1::function<http::Client*(const http::Headers&)>&,
        Credential&,
        ClientOptions&);
    ~AsyncClientBase();

public:
    template<Action kAction>
    struct Context
    {
        typedef typename ApiTraits<kAction>::ApiRequest ApiRequest;
        typedef typename ApiTraits<kAction>::ApiResponse ApiResponse;

        explicit Context(AsyncClientBase& base, const Tracker& tracker);
        virtual ~Context();
        util::Optional<OTSError> build(
            const ApiRequest&,
            const std::tr1::function<void(util::Optional<OTSError>&, ApiResponse&)>&);
        void issue();

    private:
        void retryAfter(util::Duration);
        void response(
            const Tracker&,
            util::Optional<OTSError>&,
            http::InplaceHeaders&,
            std::deque<util::MemPiece>&);
        void putHeader(const std::string& key, const std::string& val);
        bool retry(util::Optional<OTSError>&, const http::InplaceHeaders&);
        bool _response(
            util::Optional<OTSError>&,
            ApiResponse&,
            http::InplaceHeaders&,
            std::deque<util::MemPiece>&);

        template<class T>
        void fillRequestId(T& out, const http::InplaceHeaders& headers)
        {
            http::InplaceHeaders::const_iterator it =
                headers.find(util::MemPiece::from(kOTSRequestId));
            if (it != headers.end()) {
                out.mutableRequestId() = it->second.to<std::string>();
            }
        }

        template<class T>
        void fillTraceId(T& out)
        {
            out.mutableTraceId() = kTracker.traceId();
        }

    private:
        AsyncClientBase& mBase;
        util::Actor& mActor;
        http::Timer* mRetryTimer; // not own

    public:
        const std::string& kPath;
        const Tracker kTracker;
        std::deque<std::string> mBuffer;
        http::InplaceHeaders mHeaders;
        std::deque<util::MemPiece> mBody;
        util::MonotonicTime mDeadline;
        std::tr1::function<void(util::Optional<OTSError>&, ApiResponse&)> mCallback;
        Serde<kAction> mSerde;
        std::auto_ptr<RetryStrategy> mRetryStrategy;
    };

public:
    util::Logger& mutableLogger();
    const std::deque<std::tr1::shared_ptr<util::Actor> >& actors() const;
    const RetryStrategy& retryStrategy() const;

private:
    void init(
        const std::string& instance,
        const std::tr1::function<http::Client*(const http::Headers&)>&);
    std::string sign(const std::string& path, const http::InplaceHeaders&);
    util::Optional<OTSError> validateResponse(
        const Tracker&,
        const http::InplaceHeaders&,
        const std::deque<util::MemPiece>&);

private:
    boost::atomic<bool> mClose;
    std::auto_ptr<util::Logger> mLogger;
    std::auto_ptr<util::Logger> mHttpLogger;
    const Credential mCredential;
    std::auto_ptr<http::Asio> mAsio;
    std::auto_ptr<util::MemPool> mMemPool;
    std::auto_ptr<util::StrPool> mStrPool;
    std::auto_ptr<http::Client> mHttp;
    http::Headers mFixedHeaders;
    util::Duration mRequestTimeout;
    boost::atomic<int64_t> mOngoingRequests;
    std::auto_ptr<RetryStrategy> mRetryStrategy;
    std::deque<std::tr1::shared_ptr<util::Actor> > mActors;
};

template<Action kAction>
AsyncClientBase::Context<kAction>::Context(
    AsyncClientBase& base,
    const Tracker& tracker)
  : mBase(base),
    mActor(*base.mActors[tracker.traceHash() % base.mActors.size()]),
    mRetryTimer(NULL),
    kPath(ApiTraits<kAction>::kPath),
    kTracker(tracker),
    mSerde(*base.mMemPool, *base.mStrPool),
    mRetryStrategy(base.mRetryStrategy->clone())
{
    OTS_ASSERT(!mBase.mClose.load(boost::memory_order_acquire))
        .what("Async: cannot issue requests on a closed client.");
    int64_t x = mBase.mOngoingRequests.fetch_add(1, boost::memory_order_acq_rel);
    OTS_ASSERT(x >= 0)(x);
    OTS_LOG_DEBUG(*mBase.mLogger)
        ("Tracker", kTracker)
        ("OngoingRequests", x + 1)
        ("Path", ApiTraits<kAction>::kPath)
        .what("AsyncClient: issue a request.");
}

template<Action kAction>
AsyncClientBase::Context<kAction>::~Context()
{
    if (mRetryTimer != NULL) {
        OTS_LOG_DEBUG(*mBase.mLogger)
            ("Tracker", kTracker)
            .what("AsyncClient: cancel retry timer in dtor.");
        mRetryTimer->cancel();
        mRetryTimer = NULL;
    }
    int64_t x = mBase.mOngoingRequests.fetch_sub(1, boost::memory_order_acq_rel);
    OTS_ASSERT(x >= 1)(x);
    OTS_LOG_DEBUG(*mBase.mLogger)
        ("Tracker", kTracker)
        ("OngoingRequests", x - 1)
        ("Path", kPath)
        .what("AsyncClient: finish a request.");
    mRetryStrategy.reset();
}

template<Action kAction>
void AsyncClientBase::Context<kAction>::retryAfter(
    util::Duration intv)
{
    if (mRetryTimer != NULL) {
        OTS_LOG_DEBUG(*mBase.mLogger)
            ("Tracker", kTracker)
            .what("AsyncClient: cancel retry timer in retrying.");
        mRetryTimer->cancel();
    }
    mRetryTimer = &mBase.mAsio->startTimer(
        kTracker,
        util::MonotonicTime::now() + intv,
        mActor,
        std::tr1::bind(&AsyncClientBase::Context<kAction>::issue, this));
}

template<Action kAction>
util::Optional<OTSError> AsyncClientBase::Context<kAction>::build(
    const ApiRequest& req,
    const std::tr1::function<void(util::Optional<OTSError>&, ApiResponse&)>& cb)
{
    TRY(req.validate());
    OTS_LOG_DEBUG(*mBase.mLogger)
        ("Tracker", kTracker)
        .what("AsyncClient: issue a request for " + kPath);

    mDeadline = util::MonotonicTime::now() + mBase.mRequestTimeout;
    mCallback = cb;
    TRY(mSerde.serialize(mBody, req));

    mHeaders[util::MemPiece::from(kOTSTraceId)] =
        util::MemPiece::from(kTracker.traceId());
    putHeader(kOTSDate, util::UtcTime::now().toIso8601());
    putHeader(kOTSContentMD5, util::md5(mBody));
    putHeader(kHttpContentLength, pp::prettyPrint(util::totalLength(mBody)));
    putHeader(kOTSSignature, mBase.sign(kPath, mHeaders));

    return util::Optional<OTSError>();
}

template<Action kAction>
void AsyncClientBase::Context<kAction>::putHeader(
    const std::string& key,
    const std::string& val)
{
    mBuffer.push_back(val);
    mHeaders[util::MemPiece::from(key)] = util::MemPiece::from(mBuffer.back());
}


template<Action kAction>
void AsyncClientBase::Context<kAction>::issue()
{
    mRetryTimer = NULL;
    std::deque<util::MemPiece> body = mBody;
    http::InplaceHeaders headers = mHeaders;
    mBase.mHttp->issue(
        kTracker,
        std::tr1::bind(&AsyncClientBase::Context<kAction>::response,
            this,
            std::tr1::placeholders::_1,
            std::tr1::placeholders::_2,
            std::tr1::placeholders::_3,
            std::tr1::placeholders::_4),
        mDeadline,
        kPath,
        headers,
        body);
}

template<Action kAction>
void AsyncClientBase::Context<kAction>::response(
    const Tracker& tracker,
    util::Optional<OTSError>& err,
    http::InplaceHeaders& headers,
    std::deque<util::MemPiece>& body)
{
    ApiResponse resp;
    if (_response(err, resp, headers, body)) {
        retryAfter(mRetryStrategy->nextPause());
    } else {
        mCallback(err, resp);
        delete this;
    }
}

template<Action kAction>
bool AsyncClientBase::Context<kAction>::_response(
    util::Optional<OTSError>& err,
    ApiResponse& resp,
    http::InplaceHeaders& headers,
    std::deque<util::MemPiece>& body)
{
    if (err.present()) {
        if (err->errorCode().empty()) {
            util::Optional<OTSError> err2 = deserialize(*err, body);
            (void) err2;
        }
        return retry(err, headers);
    }
    err = mBase.validateResponse(kTracker, headers, body);
    if (err.present()) {
        return retry(err, headers);
    }
    err = mSerde.deserialize(resp, body);
    if (err.present()) {
        return retry(err, headers);
    }
    fillRequestId(resp, headers);
    fillTraceId(resp);
    OTS_LOG_DEBUG(*mBase.mLogger)
        ("Tracker", kTracker)
        ("Retries", mRetryStrategy->retries())
        ("Response", resp)
        .what("AsyncClient: receiving a response correctly.");
    return false;
}

template<Action kAction>
bool AsyncClientBase::Context<kAction>::retry(
    util::Optional<OTSError>& err,
    const http::InplaceHeaders& headers)
{
    OTS_ASSERT(err.present());
    fillRequestId(*err, headers);
    fillTraceId(*err);
    if (mRetryStrategy->shouldRetry(kAction, *err)) {
        OTS_LOG_INFO(*mBase.mLogger)
            ("Tracker", kTracker)
            ("Retries", mRetryStrategy->retries())
            ("Error", *err)
            .what("AsyncClient: a retriable error occurs.");
        return true;
    } else {
        OTS_LOG_INFO(*mBase.mLogger)
            ("Tracker", kTracker)
            ("Retries", mRetryStrategy->retries())
            ("Error", *err)
            .what("AsyncClient: an unretriable error occurs.");
        return false;
    }
}

} // namespace impl
} // namespace core
} // namespace tablestore
} // namespace aliyun
#endif
