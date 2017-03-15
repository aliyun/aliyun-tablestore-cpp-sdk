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
#include "core/retry.hpp"
#include "core/error.hpp"
#include "util/random.hpp"
#include <algorithm>

using namespace std;
using namespace std::tr1;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {

DefaultRetryStrategy::RetryCategory DefaultRetryStrategy::retriable(const Error& err)
{
    if (!isTemporary(err)) {
        return UNRETRIABLE;
    } else if (isCurlError(err)) {
        return DEPENDS;
    } else if (err.httpStatus() >= 500 && err.httpStatus() <= 599) {
        MemPiece code = MemPiece::from(err.errorCode());
        if (code == MemPiece::from("OTSServerBusy")) {
            return RETRIABLE;
        } else if (code == MemPiece::from("OTSPartitionUnavailable")) {
            return RETRIABLE;
        } else {
            return DEPENDS;
        }
    } else if (err.httpStatus() >= 400 && err.httpStatus() <= 499) {
        MemPiece code = MemPiece::from(err.errorCode());
        MemPiece msg = MemPiece::from(err.message());
        if (code == MemPiece::from("OTSQuotaExhausted") && msg == MemPiece::from("Too frequent table operations.")) {
            return RETRIABLE;
        } else if (code == MemPiece::from("OTSRowOperationConflict")) {
            return RETRIABLE;
        } else if (code == MemPiece::from("OTSTableNotReady")) {
            return RETRIABLE;
        } else if (code == MemPiece::from("OTSTooFrequentReservedThroughputAdjustment")) {
            return RETRIABLE;
        } else if (code == MemPiece::from("OTSCapacityUnitExhausted")) {
            return RETRIABLE;
        } else if (code == MemPiece::from("OTSRequestTimeout")) {
            return DEPENDS;
        } else {
            return UNRETRIABLE;
        }
    } else {
        return UNRETRIABLE;
    }
}

namespace {

bool idempotent(Action act)
{
    return act == API_LIST_TABLE || act == API_DESCRIBE_TABLE
        || act == API_COMPUTE_SPLIT_POINTS_BY_SIZE
        || act == API_GET_ROW || act == API_BATCH_GET_ROW || act == API_GET_RANGE;
}

} // namespace

bool DefaultRetryStrategy::retriable(Action act, const Error& err)
{
    RetryCategory cat = retriable(err);
    if (cat == RETRIABLE) {
        return true;
    } else if (cat == UNRETRIABLE) {
        return false;
    } else {
        return idempotent(act);
    }
}


const Duration DefaultRetryStrategy::kMaxPauseBase = Duration::fromMsec(400);

DefaultRetryStrategy::DefaultRetryStrategy(random::IRandom* rnd, Duration timeout)
  : mRandom(rnd),
    mTimeout(timeout),
    mPauseBase(Duration::fromMsec(1)),
    mRetries(0),
    mDeadline(MonotonicTime::now() + mTimeout)
{}

DefaultRetryStrategy* DefaultRetryStrategy::clone() const
{
    return new DefaultRetryStrategy(mRandom, mTimeout);
}

int64_t DefaultRetryStrategy::retries() const throw()
{
    return mRetries;
}

bool DefaultRetryStrategy::shouldRetry(Action act, const Error& err) const
{
    if (MonotonicTime::now() >= mDeadline) {
        return false;
    }
    return retriable(act, err);
}

Duration DefaultRetryStrategy::nextPause()
{
    ++mRetries;
    mPauseBase = std::min(mPauseBase * 2, kMaxPauseBase);
    int64_t duration = mPauseBase.toUsec();
    return Duration::fromUsec(random::nextInt(mRandom, duration / 2, duration));
}

} // namespace core
} // namespace tablestore
} // namespace aliyun
