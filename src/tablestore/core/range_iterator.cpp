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
#include "range_iterator.hpp"
#include "impl/ots_helper.hpp"
#include "tablestore/core/client.hpp"
#include "tablestore/util/move.hpp"

using namespace std;
using namespace std::tr1;
using namespace std::tr1::placeholders;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {

RangeIterator::RangeIterator(
    AsyncClient& client,
    RangeQueryCriterion& cri,
    int64_t watermark)
  : mWatermark(watermark),
    mClient(client),
    mFirstMove(true),
    mNotify(0),
    mOngoing(0)
{
    OTS_ASSERT(watermark >= 0)(watermark);
    moveAssign(mRangeQuery, util::move(cri));
    mInclusiveStart.reset(util::move(mRangeQuery.mutableInclusiveStart()));
    issue();
}

RangeIterator::~RangeIterator()
{
    for(;;) {
        int64_t x = mOngoing.load(boost::memory_order_acquire);
        if (x == 0) {
            break;
        }
        sleepFor(Duration::fromMsec(1));
    }
}

bool RangeIterator::valid() const throw()
{
    ScopedLock lock(mMutex);
    return !mBufferedRows.empty();
}

Row& RangeIterator::get() throw()
{
    ScopedLock lock(mMutex);
    return mBufferedRows.front();
}

Optional<Error> RangeIterator::moveNext()
{
    for(;;) {
        int64_t size = 0;
        {
            ScopedLock lock(mMutex);
            if (!mBufferedRows.empty()) {
                if (mFirstMove) {
                    mFirstMove = false;
                } else {
                    mBufferedRows.pop_front();
                }
            }
            size = mBufferedRows.size();
            if (!mBufferedRows.empty()) {
                return Optional<Error>();
            }
            if (mError.present()) {
                return mError;
            }
            if (!mInclusiveStart.present()) {
                return Optional<Error>(); 
            }
        }
        if (size <= mWatermark) {
            issue();
        }
        mNotify.wait();
    }
}

void RangeIterator::issue()
{
    int64_t ongoing = mOngoing.fetch_add(1, boost::memory_order_acq_rel);
    if (ongoing > 0) {
        mOngoing.fetch_sub(1, boost::memory_order_relaxed);
        return;
    }

    ScopedLock lock(mMutex);
    if (!mInclusiveStart.present()) {
        return;
    }
    
    GetRangeRequest req;
    req.mutableQueryCriterion() = mRangeQuery;
    req.mutableQueryCriterion().mutableInclusiveStart() = *mInclusiveStart;
    mClient.getRange(req, bind(&RangeIterator::callback, this, _1, _2));
}

void RangeIterator::callback(
    Optional<Error>& err,
    GetRangeResponse& resp)
{
    if (err.present()) {
        ScopedLock lock(mMutex);
        moveAssign(mError, util::move(err));
    } else {
        IVector<Row>& rows = resp.mutableRows();
        {
            ScopedLock lock(mMutex);
            for(int64_t i = 0, sz = rows.size(); i < sz; ++i) {
                mBufferedRows.push_back(Row());
                moveAssign(mBufferedRows.back(), util::move(rows[i]));
            }
            moveAssign(mInclusiveStart, util::move(resp.mutableNextStart()));
        }
        mNotify.post();
    }
    mOngoing.fetch_sub(1, boost::memory_order_relaxed);
}

} // namespace core
} // namespace tablestore
} // namespace aliyun
