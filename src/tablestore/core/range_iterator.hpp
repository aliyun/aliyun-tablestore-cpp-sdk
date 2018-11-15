#pragma once
#ifndef TABLESTORE_CORE_RANGE_ITERATOR_HPP
#define TABLESTORE_CORE_RANGE_ITERATOR_HPP
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
#include "tablestore/core/client.hpp"
#include "tablestore/core/types.hpp"
#include "tablestore/core/error.hpp"
#include "tablestore/util/logger.hpp"
#include "tablestore/util/optional.hpp"
#include "tablestore/util/iterator.hpp"
#include "tablestore/util/threading.hpp"
#include "tablestore/util/result.hpp"
#include <boost/atomic.hpp>
#include <deque>
#include <stdint.h>

namespace aliyun {
namespace tablestore {
namespace core {

namespace impl {
class RowQueue;
} // namespace

/**
 * An iterator on rows between a range,
 * which wraps complicated GetRange request-response turnovers.
 *
 * Caveats:
 * - See util::Iterator for its usage.
 */
class RangeIterator: public util::Iterator<Row&, OTSError>
{
public:
    explicit RangeIterator(
        SyncClient&, RangeQueryCriterion&,
        int64_t watermark = 10000);
    ~RangeIterator();

    bool valid() const throw();
    Row& get() throw();
    util::Optional<OTSError> moveNext();
    util::Optional<PrimaryKey> nextStart();
    CapacityUnit consumedCapacity();

private:
    void bgloop(RangeQueryCriterion&);
    bool push(util::Result<Row, OTSError>&);

private:
    enum Stage
    {
        kInit,
        kRowsReady,
        kNoMoreRows,
    };

    std::auto_ptr<util::Logger> mLogger;
    SyncClient& mClient;
    boost::atomic<bool> mStop;
    std::auto_ptr<impl::RowQueue> mRowQueue;
    Stage mStage;
    Row mCurrentRow;
    util::Thread mBgLoopThread;
    util::Mutex mMutex;
    util::Optional<PrimaryKey> mNextStart;
    CapacityUnit mConsumedCapacity;
};

} // namespace core
} // namespace tablestore
} // namespace aliyun
#endif
