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
#include "tablestore/core/retry.hpp"
#include "tablestore/core/error.hpp"
#include "tablestore/util/random.hpp"
#include <algorithm>

using namespace std;
using namespace std::tr1;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {

RetryStrategy::RetryCategory RetryStrategy::retriable(const OTSError& err)
{
    if (!isTemporary(err)) {
        return UNRETRIABLE;
    }
    int64_t status = err.httpStatus();
    MemPiece code = MemPiece::from(err.errorCode());
    if (status == OTSError::kHttpStatus_CouldntResolveHost
        || code == MemPiece::from(OTSError::kErrorCode_CouldntResolveHost))
    {
        return RETRIABLE;
    }
    if (status == OTSError::kHttpStatus_CouldntConnect
        || code == MemPiece::from(OTSError::kErrorCode_CouldntConnect))
    {
        return RETRIABLE;
    }
    if (status == OTSError::kHttpStatus_OperationTimeout
        || code == MemPiece::from(OTSError::kErrorCode_OTSRequestTimeout))
    {
        return DEPENDS;
    }
    if (status == OTSError::kHttpStatus_WriteRequestFail
        || code == MemPiece::from(OTSError::kErrorCode_WriteRequestFail))
    {
        return DEPENDS;
    }
    if (status == OTSError::kHttpStatus_CorruptedResponse
        || code == MemPiece::from(OTSError::kErrorCode_CorruptedResponse))
    {
        return DEPENDS;
    }
    if (status == OTSError::kHttpStatus_NoAvailableConnection
        || code == MemPiece::from(OTSError::kErrorCode_NoAvailableConnection))
    {
        return RETRIABLE;
    }
    if (status >= 500 && status <= 599) {
        if (code == MemPiece::from(OTSError::kErrorCode_OTSServerBusy)) {
            return RETRIABLE;
        } else if (code == MemPiece::from(OTSError::kErrorCode_OTSPartitionUnavailable)) {
            return RETRIABLE;
        } else {
            return DEPENDS;
        }
    }
    if (status >= 400 && status <= 499) {
        MemPiece msg = MemPiece::from(err.message());
        if (code == MemPiece::from(OTSError::kErrorCode_OTSQuotaExhausted)
            && msg == MemPiece::from("Too frequent table operations."))
        {
            return RETRIABLE;
        } else if (code == MemPiece::from(OTSError::kErrorCode_OTSRowOperationConflict)) {
            return RETRIABLE;
        } else if (code == MemPiece::from(OTSError::kErrorCode_OTSTableNotReady)) {
            return RETRIABLE;
        } else if (code == MemPiece::from(OTSError::kErrorCode_OTSTooFrequentReservedThroughputAdjustment)) {
            return RETRIABLE;
        } else if (code == MemPiece::from(OTSError::kErrorCode_OTSCapacityUnitExhausted)) {
            return RETRIABLE;
        } else if (code == MemPiece::from(OTSError::kErrorCode_OTSTimeout)) {
            return DEPENDS;
        } else {
            return UNRETRIABLE;
        }
    }
    return UNRETRIABLE;
}

namespace {

bool idempotent(Action act)
{
    switch(act) {
    case kApi_ListTable: case kApi_DescribeTable: case kApi_DeleteTable:
    case kApi_CreateTable: case kApi_ComputeSplitsBySize:
    case kApi_GetRow: case kApi_BatchGetRow: case kApi_GetRange:
    case kApi_DeleteRow:
        return true;
    case kApi_UpdateTable: case kApi_PutRow: case kApi_UpdateRow: case kApi_BatchWriteRow:
        return false;
    }
    OTS_ASSERT(false)((int) act);
    return false;
}

} // namespace

bool RetryStrategy::retriable(Action act, const OTSError& err)
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


const Duration DeadlineRetryStrategy::kMaxPauseBase = Duration::fromMsec(400);

DeadlineRetryStrategy::DeadlineRetryStrategy(
    const shared_ptr<random::Random>& rng,
    Duration timeout)
  : mRandom(rng),
    mTimeout(timeout),
    mPauseBase(Duration::fromMsec(1)),
    mRetries(0),
    mDeadline(MonotonicTime::now() + mTimeout)
{}

DeadlineRetryStrategy* DeadlineRetryStrategy::clone() const
{
    return new DeadlineRetryStrategy(mRandom, mTimeout);
}

int64_t DeadlineRetryStrategy::retries() const throw()
{
    return mRetries;
}

bool DeadlineRetryStrategy::shouldRetry(Action act, const OTSError& err) const
{
    if (MonotonicTime::now() >= mDeadline) {
        return false;
    }
    return retriable(act, err);
}

Duration DeadlineRetryStrategy::nextPause()
{
    ++mRetries;
    mPauseBase = std::min(mPauseBase * 2, kMaxPauseBase);
    int64_t duration = mPauseBase.toUsec();
    return Duration::fromUsec(random::nextInt(*mRandom, duration / 2, duration));
}


CountingRetryStrategy::CountingRetryStrategy(
    const shared_ptr<util::random::Random>& rng,
    int64_t n,
    Duration interval)
  : mRandom(rng),
    mInterval(interval),
    mMaxRetries(n),
    mRetries(0)
{
    OTS_ASSERT(interval >= Duration::fromUsec(100))
        (interval);
}

CountingRetryStrategy* CountingRetryStrategy::clone() const
{
    return new CountingRetryStrategy(mRandom, mMaxRetries, mInterval);
}

int64_t CountingRetryStrategy::retries() const throw()
{
    return mRetries;
}

bool CountingRetryStrategy::shouldRetry(Action act, const OTSError& err) const
{
    if (mRetries >= mMaxRetries) {
        return false;
    }
    return retriable(act, err);
}

Duration CountingRetryStrategy::nextPause()
{
    ++mRetries;
    int64_t intv = mInterval.toUsec();
    return Duration::fromUsec(random::nextInt(*mRandom, 100, intv + 1));
}


NoRetry::NoRetry()
{}

NoRetry* NoRetry::clone() const
{
    return new NoRetry();
}

int64_t NoRetry::retries() const throw()
{
    return 0;
}

bool NoRetry::shouldRetry(Action, const OTSError&) const
{
    return false;
}

Duration NoRetry::nextPause()
{
    OTS_ASSERT(false);
    return Duration::fromMsec(10);
}

} // namespace core
} // namespace tablestore
} // namespace aliyun
