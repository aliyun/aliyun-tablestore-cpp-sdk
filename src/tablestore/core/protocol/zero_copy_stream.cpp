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
#include "zero_copy_stream.hpp"
#include "tablestore/util/move.hpp"
#include "tablestore/util/assert.hpp"

using namespace std;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {

MemPoolZeroCopyInputStream::MemPoolZeroCopyInputStream(deque<MemPiece>& a)
  : mReadBytes(0)
{
    moveAssign(mPieces, util::move(a));
}

bool MemPoolZeroCopyInputStream::Next(const void** data, int* size)
{
    if (!mBackupPiece.present() && mPieces.empty()) {
        return false;
    }
    if (mBackupPiece.present()) {
        MemPiece x = *mBackupPiece;
        mBackupPiece = Optional<MemPiece>();
        *data = x.data();
        *size = x.length();
        mReadBytes += x.length();
    } else {
        mLastPiece = mPieces.front();
        mPieces.pop_front();
        *data = mLastPiece.data();
        *size = mLastPiece.length();
        mReadBytes += mLastPiece.length();
    }
    return true;
}

void MemPoolZeroCopyInputStream::BackUp(int count)
{
    OTS_ASSERT(count <= mLastPiece.length())
        (count)
        (mLastPiece.length());
    MemPiece p = mLastPiece.subpiece(mLastPiece.length() - count, count);
    mBackupPiece.reset(util::move(p));
    mReadBytes -= count;
}

bool MemPoolZeroCopyInputStream::Skip(int count)
{
    for(; count > 0;) {
        const void* data = NULL;
        int size = 0;
        bool ret = Next(&data, &size);
        if (!ret) {
            return false;
        }
        if (count >= size) {
            count -= size;
        } else {
            BackUp(size - count);
            break;
        }
    }
    return true;
}

int64_t MemPoolZeroCopyInputStream::ByteCount() const
{
    return mReadBytes;
}


MemPoolZeroCopyOutputStream::MemPoolZeroCopyOutputStream(
    MemPool* mpool)
  : mMemPool(mpool),
    mByteCount(0)
{}

MemPoolZeroCopyOutputStream::~MemPoolZeroCopyOutputStream()
{
    for(; !mBlocks.empty();) {
        MemPool::BlockHolder hold(mBlocks.back());
        mBlocks.pop_back();
        hold.giveBack();
    }
}

bool MemPoolZeroCopyOutputStream::Next(void** data, int* size)
{
    if (!mBlocks.empty()) {
        mPieces.push_back(mBlocks.back()->piece());
        mByteCount += mPieces.back().length();
    }

    mBlocks.push_back(mMemPool->borrow());
    MutableMemPiece mmp = mBlocks.back()->mutablePiece();
    *data = mmp.begin();
    *size = mmp.length();
    return true;
}

void MemPoolZeroCopyOutputStream::BackUp(int count)
{
    OTS_ASSERT(!mBlocks.empty());
    MemPiece mp = mBlocks.back()->piece();
    MemPiece realpiece = mp.subpiece(0, mp.length() - count);
    mPieces.push_back(realpiece);
    mByteCount += realpiece.length();
}

int64_t MemPoolZeroCopyOutputStream::ByteCount() const
{
    return mByteCount;
}

MoveHolder<deque<MemPiece> > MemPoolZeroCopyOutputStream::pieces()
{
    return util::move(mPieces);
}


} // namespace core
} // namespace tablestore
} // namespace aliyun
