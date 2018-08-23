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
#include "tablestore/util/mempool.hpp"
#include "tablestore/util/random.hpp"
#include "tablestore/util/timestamp.hpp"
#include "testa/testa.hpp"
#include <boost/thread/thread.hpp>
#include <boost/atomic.hpp>
#include <boost/ref.hpp>
#include <tr1/functional>
#include <string>
#include <stdint.h>

using namespace std;
using namespace std::tr1;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {

void IncrementalMemPool_1thread(const string&)
{
    IncrementalMemPool mp;
    {
        MemPool::Stats stats = mp.stats();
        TESTA_ASSERT(stats.mBorrowedBlocks == 0)
            (stats.mBorrowedBlocks).issue();
    }
    deque<MemPool::Block*> blks;
    for(int64_t i = 0; i < 128; ++i) {
        MemPool::BlockHolder blk(mp.borrow());
        blks.push_back(blk.transfer());

        MemPool::Stats stats = mp.stats();
        TESTA_ASSERT(stats.mBorrowedBlocks == (int64_t) blks.size())
            (stats.mBorrowedBlocks)
            (blks.size())
            .issue();
        TESTA_ASSERT(stats.mTotalBlocks >= (int64_t) blks.size())
            (stats.mTotalBlocks)
            (blks.size())
            .issue();
    }

    for(; !blks.empty(); blks.pop_back()) {
        MemPool::BlockHolder blk(blks.back());
        blk.giveBack();
    }

    {
        MemPool::Stats stats = mp.stats();
        TESTA_ASSERT(stats.mBorrowedBlocks == 0)
            (stats.mBorrowedBlocks)
            .issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(IncrementalMemPool_1thread);

void IncrementalStrPool_1thread(const string&)
{
    StrPool sp;
    {
        StrPool::Stats stats = sp.stats();
        TESTA_ASSERT(stats.mBorrowed == 0)
            (stats.mBorrowed).issue();
    }
    deque<string*> strs;
    for(int64_t i = 0; i < 128; ++i) {
        strs.push_back(sp.borrow());

        StrPool::Stats stats = sp.stats();
        TESTA_ASSERT(stats.mBorrowed == (int64_t) strs.size())
            (stats.mBorrowed)
            (strs.size())
            .issue();
        TESTA_ASSERT(stats.mTotal >= (int64_t) strs.size())
            (stats.mTotal)
            (strs.size())
            .issue();
    }

    int64_t total = strs.size();
    for(; !strs.empty();) {
        sp.giveBack(strs.back());
        strs.pop_back();

        StrPool::Stats stats = sp.stats();
        TESTA_ASSERT(stats.mBorrowed == (int64_t) strs.size())
            (stats.mBorrowed)
            (strs.size())
            .issue();
        TESTA_ASSERT(stats.mTotal == total)
            (stats.mTotal)
            (total)
            .issue();
    }

    {
        StrPool::Stats stats = sp.stats();
        TESTA_ASSERT(stats.mBorrowed == 0)
            (stats.mBorrowed)
            .issue();
        TESTA_ASSERT(stats.mTotal == total)
            (stats.mTotal)
            (total)
            .issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(IncrementalStrPool_1thread);

namespace {

void MemPool_tester(
    boost::atomic<bool>* stopper,
    Random& rng,
    MemPool* mpool)
{
    for(;;) {
        if (stopper->load(boost::memory_order_acquire)) {
            break;
        }
        MemPool::BlockHolder blk(mpool->borrow());
        sleepFor(Duration::fromUsec(random::nextInt(rng, 10000)));
        blk.giveBack();
    }
}

} // namespace

void IncrementalMemPool_Nthreads(const string&)
{
    IncrementalMemPool mp;
    auto_ptr<Random> rng(random::newDefault(0));
    boost::atomic<bool> stopper;
    boost::thread_group testers;
    for(int i = 0; i < 32; ++i) {
        testers.create_thread(bind(MemPool_tester, &stopper, boost::ref(*rng), &mp));
    }
    sleepFor(Duration::fromMsec(500));
    stopper.store(true, boost::memory_order_release);
    testers.join_all();
    {
        MemPool::Stats stats = mp.stats();
        TESTA_ASSERT(stats.mBorrowedBlocks == 0)
            (stats).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(IncrementalMemPool_Nthreads);

namespace {

void StrPool_tester(
    boost::atomic<bool>& stopper,
    Random& rng,
    StrPool& spool)
{
    for(;;) {
        if (stopper.load(boost::memory_order_acquire)) {
            break;
        }
        string* s = spool.borrow();
        sleepFor(Duration::fromUsec(random::nextInt(rng, 10000)));
        spool.giveBack(s);
    }
}

} // namespace

void StrPool_Nthreads(const string&)
{
    StrPool sp;
    auto_ptr<Random> rng(random::newDefault());
    boost::atomic<bool> stopper;
    boost::thread_group testers;
    for(int i = 0; i < 32; ++i) {
        testers.create_thread(bind(
                StrPool_tester,
                boost::ref(stopper),
                boost::ref(*rng),
                boost::ref(sp)));
    }
    sleepFor(Duration::fromMsec(500));
    stopper.store(true, boost::memory_order_release);
    testers.join_all();
    {
        StrPool::Stats stats = sp.stats();
        TESTA_ASSERT(stats.mBorrowed == 0)
            (stats).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(StrPool_Nthreads);

} // namespace tablestore
} // namespace aliyun
