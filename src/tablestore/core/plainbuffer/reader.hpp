#pragma once
#ifndef TABLESTORE_CORE_PLAINBUFFER_READER_HPP
#define TABLESTORE_CORE_PLAINBUFFER_READER_HPP
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
#include "tablestore/util/mempiece.hpp"
#include <stdint.h>

namespace aliyun {
namespace tablestore {

namespace util {
class MemPiece;
} // namespace util

namespace core {
namespace plainbuffer {

namespace impl {
util::Optional<OTSError> readUint8(uint8_t&, uint8_t const *&, uint8_t const *);
util::Optional<OTSError> readUint32(uint32_t&, uint8_t const *&, uint8_t const *);
util::Optional<OTSError> readUint64(uint64_t&, uint8_t const *&, uint8_t const *);
util::Optional<OTSError> readHeader(uint8_t const *&, uint8_t const *);
} // namespace impl

util::Optional<OTSError> readRow(Row&, const util::MemPiece&);
util::Optional<OTSError> readRows(IVector<Row>&, const util::MemPiece&);

} // namespace plainbuffer
} // namespace core
} // namespace tablestore
} // namespace aliyun

#endif
