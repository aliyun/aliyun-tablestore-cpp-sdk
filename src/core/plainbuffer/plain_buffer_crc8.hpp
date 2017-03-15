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
#pragma once

#include "util/mempiece.hpp"
#include <string>
#include <stdint.h>

namespace aliyun {
namespace tablestore {
namespace core {

const int32_t kCrc8TableSize = 256;

/**
 * @brief Crc8Table
 *
 * CRC8码表，用于静态初始化变量。
 */
class Crc8Table
{
public:

    Crc8Table()
    {
        for (int32_t i = 0; i < kCrc8TableSize; ++i) {
            uint8_t x = (uint8_t) i;
            for (int32_t j = 8; j > 0; --j) {
                x = ((x << 1) ^ (((x & 0x80) != 0) ? 0x07 : 0));
            }
            mDataTable[i] = x;
        }
    }

    inline uint8_t operator[](int32_t index)
    {
        return mDataTable[index];
    }

private:

    uint8_t mDataTable[kCrc8TableSize];
};

/**
 * @brief PlainBufferCrc8
 *
 * 采用crc-8-ATM规范
 * 多项式: x^8 + x^2 + x + 1
 */
class PlainBufferCrc8
{
public:

    static uint8_t CrcInt8(uint8_t crc, uint8_t in);
    
    static uint8_t CrcInt32(uint8_t crc, uint32_t in);
    
    static uint8_t CrcInt64(uint8_t crc, uint64_t in);
    
    static uint8_t CrcString(uint8_t crc, const util::MemPiece& in);
    
private:

    static Crc8Table mCrc8Table;
};

} // namespace core
} // namespace tablestore
} // namespace aliyun

