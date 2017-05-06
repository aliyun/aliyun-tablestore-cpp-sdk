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
#include "plain_buffer_crc8.hpp"

using namespace std;

namespace aliyun {
namespace tablestore {
namespace core {

// PlainBufferCrc8

Crc8Table PlainBufferCrc8::mCrc8Table;

uint8_t PlainBufferCrc8::CrcInt8(uint8_t crc, uint8_t in)
{
    crc = mCrc8Table[(crc ^ in) & 0xff];
    return crc;
}

uint8_t PlainBufferCrc8::CrcInt32(uint8_t crc, uint32_t in) {
    for (int64_t i = 0; i < 4; ++i) {
        crc = CrcInt8(crc, static_cast<uint8_t>(in & 0xff));
        in >>= 8;
    }
    return crc;
}

uint8_t PlainBufferCrc8::CrcInt64(uint8_t crc, uint64_t in) {
    for (int64_t i = 0; i < 8; ++i) {
        crc = CrcInt8(crc, static_cast<uint8_t>(in & 0xff));
        in >>= 8;
    }
    return crc;
}

uint8_t PlainBufferCrc8::CrcString(uint8_t crc, const util::MemPiece& in) {
    if (in.length() == 0) {
        return crc;
    }
    const uint8_t* b = in.data();
    const uint8_t* e = b + in.length();
    for (; b < e; ++b) {
        crc = CrcInt8(crc, *b);
    }
    return crc;
}

} // namespace core
} // namespace tablestore
} // namespace aliyun
