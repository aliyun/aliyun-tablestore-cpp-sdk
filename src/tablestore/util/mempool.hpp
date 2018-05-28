#pragma once
#ifndef TABLESTORE_UTIL_MEMPOOL_HPP
#define TABLESTORE_UTIL_MEMPOOL_HPP
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

#include "tablestore/util/random.hpp"
#include "tablestore/util/mempiece.hpp"
#include "tablestore/util/optional.hpp"
#include "tablestore/util/move.hpp"
#include <boost/atomic.hpp>
#include <memory>
#include <vector>
#include <string>
#include <stdint.h>

namespace aliyun {
namespace tablestore {
namespace util {

class MemPool
{
public:
    static const int64_t kBlockSize = 1024 * 1024;

public:
    class Block
    {
    public:
        explicit Block(MemPool&);
        virtual ~Block() {}
        virtual MemPiece piece() const =0;
        virtual MutableMemPiece mutablePiece() =0;
        MemPool& mutableMemPool()
        {
            return mMemPool;
        }

    protected:
        MemPool& mMemPool;
    };

    class BlockHolder
    {
    private:
        BlockHolder(const BlockHolder&); // =delete
        BlockHolder& operator=(const BlockHolder&); // =delete

    public:
        explicit BlockHolder()
          : mInner(NULL)
        {}
        ~BlockHolder();

        explicit BlockHolder(Block* blk)
          : mInner(blk)
        {
            OTS_ASSERT(blk != NULL);
        }

        explicit BlockHolder(const MoveHolder<BlockHolder>& a)
        {
            *this = a;
        }

        BlockHolder& operator=(const MoveHolder<BlockHolder>& a);

        MemPiece piece() const;
        MutableMemPiece mutablePiece();
        void giveBack();

        Block* transfer()
        {
            Block* res = mInner;
            mInner = NULL;
            return res;
        }

    private:
        Block* mInner;
    };

    virtual ~MemPool() {}

    /**
     * Borrows a block from the pool, thread-safely.
     */
    virtual Block* borrow() =0;

    struct Stats
    {
        int64_t mTotalBlocks;
        int64_t mAvailableBlocks;
        int64_t mBorrowedBlocks;

        explicit Stats()
          : mTotalBlocks(0),
            mAvailableBlocks(0),
            mBorrowedBlocks(0)
        {}

        void prettyPrint(std::string&) const;
    };

    /**
     * Returns some infomation about the pool, thread-safely.
     */
    virtual Stats stats() const =0;

protected:
    virtual void giveBack(Block*) =0;
};

/**
 * A memory pool. When its pool is exhausted, new block will be allocated.
 */
class IncrementalMemPool: public MemPool
{
private:
    static const int64_t kInitBlocks = 32;
    IncrementalMemPool(const IncrementalMemPool&); // =delete
    IncrementalMemPool& operator=(const IncrementalMemPool&); // =delete

public:
    explicit IncrementalMemPool();
    ~IncrementalMemPool();

    Block* borrow();
    Stats stats() const;

protected:
    void giveBack(Block*);

public:
    /// for internal use only
    template<int Size>
    struct ThisInnerBlock: public Block
    {
        uint8_t mContent[Size];

        explicit ThisInnerBlock(IncrementalMemPool& mpool)
          : Block(mpool)
        {}

        MemPiece piece() const
        {
            return MemPiece(mContent, Size);
        }

        MutableMemPiece mutablePiece()
        {
            return MutableMemPiece(mContent, Size);
        }
    };

    typedef ThisInnerBlock<kBlockSize - sizeof(ThisInnerBlock<0>)> MyInnerBlock;

    class BlockFreeQueue
    {
    public:
        virtual ~BlockFreeQueue() {}

        virtual bool push(MyInnerBlock*) =0;
        virtual bool pop(MyInnerBlock**) =0;
    };

private:
    boost::atomic<int64_t> mTotalBlocks;
    boost::atomic<int64_t> mBorrowedBlocks;
    std::auto_ptr<BlockFreeQueue> mAvailableBlocks;
};

class StrPool
{
public:
    explicit StrPool();
    ~StrPool();

    struct Stats
    {
        int64_t mTotal;
        int64_t mAvailable;
        int64_t mBorrowed;

        explicit Stats()
          : mTotal(0),
            mAvailable(0),
            mBorrowed(0)
        {}

        void prettyPrint(std::string&) const;
    };

    Stats stats() const;

    std::string* borrow();
    void giveBack(std::string*);

public:
    // for private use
    /**
     * An interface to thread-safe queues on strings.
     */
    class IQueue
    {
    public:
        virtual ~IQueue() {}

        virtual void push(std::string*) =0;
        virtual std::string* pop() =0;
        virtual int64_t size() const =0;
    };

private:
    boost::atomic<int64_t> mBorrowed;
    std::auto_ptr<IQueue> mAvailable;
};

} // namespace util
} // namespace tablestore
} // namespace aliyun

#endif
