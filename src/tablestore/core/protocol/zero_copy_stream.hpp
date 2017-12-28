#pragma once
#ifndef TABLESTORE_CORE_PROTOCOL_ZERO_COPY_STREAM_HPP
#define TABLESTORE_CORE_PROTOCOL_ZERO_COPY_STREAM_HPP
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
#include "tablestore/util/mempiece.hpp"
#include "tablestore/util/optional.hpp"
#include <google/protobuf/io/zero_copy_stream.h>
#include <stdint.h>

namespace aliyun {
namespace tablestore {
namespace core {

class MemPoolZeroCopyInputStream: public google::protobuf::io::ZeroCopyInputStream
{
public:
    explicit MemPoolZeroCopyInputStream(std::deque<util::MemPiece>&);

    bool Next(const void** data, int* size);
    void BackUp(int count);
    bool Skip(int count);
    int64_t ByteCount() const;

private:
    std::deque<util::MemPiece> mPieces;
    util::MemPiece mLastPiece;
    util::Optional<util::MemPiece> mBackupPiece;
    int64_t mReadBytes;
};

class MemPoolZeroCopyOutputStream: public google::protobuf::io::ZeroCopyOutputStream
{
public:
    explicit MemPoolZeroCopyOutputStream(util::MemPool* mpool);
    ~MemPoolZeroCopyOutputStream();

    bool Next(void** data, int* size);
    void BackUp(int count);
    int64_t ByteCount() const;
    util::MoveHolder<std::deque<util::MemPiece> > pieces();

private:
    util::MemPool* mMemPool;
    std::deque<util::MemPool::Block*> mBlocks;
    std::deque<util::MemPiece> mPieces;
    int64_t mByteCount;
};

} // namespace core
} // namespace tablestore
} // namespace aliyun
#endif
