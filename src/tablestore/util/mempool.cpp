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
#include "mempool.hpp"
#include "tablestore/util/timestamp.hpp"
#include "tablestore/util/assert.hpp"
#include <boost/lockfree/queue.hpp>
#include <algorithm>
#include <cstdio>

using namespace std;

namespace aliyun {
namespace tablestore {
namespace util {

void MemPool::Stats::prettyPrint(string& out) const
{
    out.append("{\"TotalBlocks\":");
    pp::prettyPrint(out, mTotalBlocks);
    out.append(",\"AvailableBlocks\":");
    pp::prettyPrint(out, mAvailableBlocks);
    out.append(",\"BorrowedBlocks\":");
    pp::prettyPrint(out, mBorrowedBlocks);
    out.append("}");
}

MemPool::Block::Block(MemPool& mpool)
  : mMemPool(mpool)
{
}

MemPool::BlockHolder::~BlockHolder()
{
    OTS_ASSERT(mInner == NULL)
        .what("A block of MemPool must be either given back to the pool or transfered to another block.");
}

MemPool::BlockHolder& MemPool::BlockHolder::operator=(
    const MoveHolder<BlockHolder>& a)
{
    if (mInner != NULL) {
        giveBack();
    }
    mInner = a->mInner;
    a->mInner = NULL;
    return *this;
}

void MemPool::BlockHolder::giveBack()
{
    OTS_ASSERT(mInner != NULL);
    mInner->mutableMemPool().giveBack(mInner);
    mInner = NULL;
}

MemPiece MemPool::BlockHolder::piece() const
{
    OTS_ASSERT(mInner != NULL);
    return mInner->piece();
}

MutableMemPiece MemPool::BlockHolder::mutablePiece()
{
    OTS_ASSERT(mInner != NULL);
    return mInner->mutablePiece();
}


class BlockFreeQueueImpl: public IncrementalMemPool::BlockFreeQueue
{
public:
    explicit BlockFreeQueueImpl()
      : mBlocks(0)
    {}

    bool push(IncrementalMemPool::MyInnerBlock* blk)
    {
        return mBlocks.push(blk);
    }

    bool pop(IncrementalMemPool::MyInnerBlock** pblk)
    {
        return mBlocks.pop(*pblk);
    }

private:
    boost::lockfree::queue<
        IncrementalMemPool::MyInnerBlock*,
        boost::lockfree::fixed_sized<false> > mBlocks;
};

IncrementalMemPool::IncrementalMemPool()
  : mTotalBlocks(0),
    mBorrowedBlocks(0),
    mAvailableBlocks(new BlockFreeQueueImpl())
{
    for(int64_t i = 0; i < kInitBlocks; ++i) {
        auto_ptr<MyInnerBlock> blk(new MyInnerBlock(*this));
        if (mAvailableBlocks->push(blk.get())) {
            blk.release();
            mTotalBlocks.fetch_add(1, boost::memory_order_acq_rel);
        }
    }
}

IncrementalMemPool::~IncrementalMemPool()
{
    int64_t borrowed = mBorrowedBlocks.load(boost::memory_order_acquire);
    OTS_ASSERT(borrowed == 0)
        (borrowed)
        .what("Some of blocks are not returned.");
    for(;;) {
        MyInnerBlock* blk = NULL;
        if (!mAvailableBlocks->pop(&blk)) {
            break;
        }
        delete blk;
    }
}

MemPool::Block* IncrementalMemPool::borrow()
{
    MyInnerBlock* ret = NULL;
    if (!mAvailableBlocks->pop(&ret)) {
        ret = new MyInnerBlock(*this);
        mTotalBlocks.fetch_add(1, boost::memory_order_acq_rel);
    }
    mBorrowedBlocks.fetch_add(1, boost::memory_order_acq_rel);
    return ret;
}

void IncrementalMemPool::giveBack(Block* blk)
{
    MyInnerBlock* myblk = dynamic_cast<MyInnerBlock*>(blk);
    OTS_ASSERT(myblk != NULL);
    bool ret = mAvailableBlocks->push(myblk);
    OTS_ASSERT(ret);
    mBorrowedBlocks.fetch_add(-1, boost::memory_order_acq_rel);
}

MemPool::Stats IncrementalMemPool::stats() const
{
    Stats res;
    res.mTotalBlocks = mTotalBlocks.load(boost::memory_order_acquire);
    res.mBorrowedBlocks = mBorrowedBlocks.load(boost::memory_order_acquire);
    res.mAvailableBlocks = res.mTotalBlocks - res.mBorrowedBlocks;
    return res;
}

StrPool::~StrPool()
{
    int64_t borrowed = mBorrowed.load(boost::memory_order_acquire);
    OTS_ASSERT(borrowed == 0)
        (borrowed)
        .what("Some of strings are not returned.");
    for(; mAvailable->size() > 0;) {
        string* str = mAvailable->pop();;
        delete str;
    }
}

void StrPool::Stats::prettyPrint(string& out) const
{
    out.append("{\"Total\":");
    pp::prettyPrint(out, mTotal);
    out.append(",\"Available\":");
    pp::prettyPrint(out, mAvailable);
    out.append(",\"Borrowed\":");
    pp::prettyPrint(out, mBorrowed);
    out.append("}");
}

StrPool::Stats StrPool::stats() const
{
    Stats res;
    res.mBorrowed = mBorrowed.load(boost::memory_order_acquire);
    res.mAvailable = mAvailable->size();
    res.mTotal = res.mAvailable + res.mBorrowed;
    return res;
}

string* StrPool::borrow()
{
    string* res = mAvailable->pop();
    mBorrowed.fetch_add(1, boost::memory_order_acq_rel);
    return res;
}

void StrPool::giveBack(string* s)
{
    s->clear();
    mAvailable->push(s);
    mBorrowed.fetch_sub(1, boost::memory_order_acq_rel);
}

namespace {

class StrQueue: public StrPool::IQueue, private boost::noncopyable
{
public:
    explicit StrQueue()
      : mStrs(0),
        mSize(0)
    {}

    void push(string* s)
    {
        bool ret = mStrs.push(s);
        OTS_ASSERT(ret);
        mSize.fetch_add(1, boost::memory_order_acq_rel);
    }

    string* pop()
    {
        string* res = NULL;
        bool ret = mStrs.pop(res);
        if (ret) {
            mSize.fetch_sub(1, boost::memory_order_acq_rel);
            return res;
        } else {
            return new string();
        }
    }

    int64_t size() const
    {
        return mSize.load(boost::memory_order_acquire);
    }

private:
    boost::lockfree::queue<
        string*,
        boost::lockfree::fixed_sized<false> > mStrs;
    boost::atomic<int64_t> mSize;

};

} // namespace

StrPool::StrPool()
  : mBorrowed(0),
    mAvailable(new StrQueue())
{}

} // namespace util
} // namespace tablestore
} // namespace aliyun
