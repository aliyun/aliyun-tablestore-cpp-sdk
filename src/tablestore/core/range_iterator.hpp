#pragma once
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
#include "tablestore/core/types.hpp"
#include "tablestore/core/error.hpp"
#include "tablestore/util/optional.hpp"
#include "tablestore/util/iterator.hpp"
#include "tablestore/util/threading.hpp"
#include <boost/atomic.hpp>
#include <deque>
#include <stdint.h>

namespace aliyun {
namespace tablestore {
namespace core {
class AsyncClient;

/**
 * An iterator on rows between a range,
 * which wraps complicated GetRange request-response turnovers.
 *
 * Caveats:
 * - Although it depends on AsyncClient for TableStore,
 *   the iterator itself is synchronous and blocking.
 * - See util::Iterator for its usage.
 */
class RangeIterator: public util::Iterator<Row&, Error>
{
public:
    explicit RangeIterator(
        AsyncClient&, RangeQueryCriterion&,
        int64_t watermark = 10000);
    ~RangeIterator();

    bool valid() const throw();
    Row& get() throw();
    util::Optional<Error> moveNext();

private:
    void issue();
    void callback(util::Optional<Error>&, GetRangeResponse&);

private:
    const int64_t mWatermark;
    AsyncClient& mClient;
    util::Optional<PrimaryKey> mInclusiveStart;
    RangeQueryCriterion mRangeQuery;
    bool mFirstMove;

    util::Semaphore mNotify;
    mutable util::Mutex mMutex;
    boost::atomic<int64_t> mOngoing;
    std::deque<Row> mBufferedRows;
    util::Optional<Error> mError;
};

} // namespace core
} // namespace tablestore
} // namespace aliyun
