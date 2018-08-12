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
#include "tablestore/util/logging.hpp"
#include "tablestore/util/timestamp.hpp"
#include <boost/ref.hpp>
#include <boost/lockfree/queue.hpp>

using namespace std;
using namespace std::tr1;
using namespace std::tr1::placeholders;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {

namespace impl {

class RowQueue
{
    typedef Result<Row, OTSError> Item;

public:
    explicit RowQueue(int64_t size)
      : mQueue(size)
    {}

    ~RowQueue()
    {
        for(;;) {
            Item* item = NULL;
            bool ok = mQueue.pop(item);
            if (!ok) {
                break;
            }
            delete item;
        }
    }

    bool push(Item& item)
    {
        Item* x = new Item;
        moveAssign(*x, util::move(item));
        bool ok = mQueue.push(x);
        if (ok) {
            return true;
        }
        // rollback
        moveAssign(item, util::move(*x));
        delete x;
        return false;
    }

    bool pop(Item& item)
    {
        Item* x = NULL;
        bool ok = mQueue.pop(x);
        if (!ok) {
            return false;
        }
        moveAssign(item, util::move(*x));
        delete x;
        return true;
    }

private:
    boost::lockfree::queue<
        Item*,
        boost::lockfree::fixed_sized<true> > mQueue;
};

} // namespace impl

RangeIterator::RangeIterator(
    SyncClient& client,
    RangeQueryCriterion& cri,
    int64_t watermark)
  : mLogger(client.mutableLogger().spawn("range_iterator")),
    mClient(client),
    mStop(false),
    mRowQueue(new impl::RowQueue(watermark)),
    mStage(kInit)
{
    Thread t(bind(&RangeIterator::bgloop, this, boost::ref(cri)));
    moveAssign(mBgLoopThread, util::move(t));
}

RangeIterator::~RangeIterator()
{
    OTS_LOG_DEBUG(*mLogger)
        .what("Destorying RangeIterator.");
    mStop.store(true, boost::memory_order_relaxed);
    mBgLoopThread.join();
}

bool RangeIterator::valid() const throw()
{
    OTS_ASSERT(mStage != kInit);
    return mStage == kRowsReady;
}

Row& RangeIterator::get() throw()
{
    OTS_ASSERT(mStage != kInit);
    return mCurrentRow;
}

Optional<OTSError> RangeIterator::moveNext()
{
    OTS_ASSERT(mStage == kInit || mStage == kRowsReady)
        (static_cast<size_t>(mStage));
    for(;;) {
        Result<Row, OTSError> result;
        bool ready = mRowQueue->pop(result);
        if (!ready) {
            OTS_LOG_DEBUG(*mLogger)
                .what("Not ready for moveNext().");
            sleepFor(Duration::fromUsec(10));
            continue;
        }

        if (!result.ok()) {
            OTS_LOG_INFO(*mLogger)
                ("Error", result.errValue())
                .what("iteration meets an error.");
            return Optional<OTSError>(util::move(result.mutableErrValue()));
        }

        moveAssign(mCurrentRow, util::move(result.mutableOkValue()));
        if (mCurrentRow.primaryKey().size() == 0) {
            // a row with no primary-key columns means the end
            OTS_LOG_INFO(*mLogger)
                .what("The end of iteration.");
            mStage = kNoMoreRows;
        } else {
            OTS_LOG_DEBUG(*mLogger)
                ("Row", result.okValue());
            mStage = kRowsReady;
        }
        break;
    }
    return Optional<OTSError>();
}

void RangeIterator::bgloop(RangeQueryCriterion& cri)
{
    OTS_LOG_INFO(*mLogger)
        .what("Start iteration.");
    GetRangeRequest req;
    moveAssign(req.mutableQueryCriterion(), util::move(cri));
    for(;;) {
        GetRangeResponse resp;
        if (mStop.load(boost::memory_order_relaxed)) {
            break;
        }
        Optional<OTSError> err = mClient.getRange(resp, req);
        if (mStop.load(boost::memory_order_relaxed)) {
            break;
        }

        if (err.present()) {
            Result<Row, OTSError> result;
            moveAssign(result.mutableErrValue(), util::move(*err));
            push(result);
            break;
        }

        IVector<Row>& rows = resp.mutableRows();
        OTS_LOG_DEBUG(*mLogger)
            ("RowSize", rows.size());
        for(int64_t i = 0, sz = rows.size(); i < sz; ++i) {
            Result<Row, OTSError> result;
            moveAssign(result.mutableOkValue(), util::move(rows[i]));
            push(result);
        }

        bool limited = false;
        if (req.queryCriterion().limit().present()) {
            OTS_ASSERT(rows.size() <= *req.queryCriterion().limit())
                (rows.size())
                (*req.queryCriterion().limit());
            *req.mutableQueryCriterion().mutableLimit() -= rows.size();
            limited = (*req.queryCriterion().limit() == 0);
        }
        if (!resp.nextStart().present() || limited) {
            // a row with no primary-key column means the end
            OTS_LOG_INFO(*mLogger)
                .what("No more rows in the server-side.");
            Result<Row, OTSError> result;
            push(result);
            break;
        } else {
            OTS_LOG_DEBUG(*mLogger)
                ("NextStart", *resp.mutableNextStart())
                .what("Try a next request.");
            moveAssign(
                req.mutableQueryCriterion().mutableInclusiveStart(),
                util::move(*resp.mutableNextStart()));
        }
    }
}

void RangeIterator::push(Result<Row, OTSError>& item)
{
    for(;;) {
        bool ok = mRowQueue->push(item);
        if (ok) {
            break;
        }
        sleepFor(Duration::fromUsec(100));
    }
}

} // namespace core
} // namespace tablestore
} // namespace aliyun
