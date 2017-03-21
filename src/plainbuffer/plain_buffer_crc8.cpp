/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#include "plain_buffer_crc8.h"

using namespace std;

namespace aliyun {
namespace tablestore {

// PlainBufferCrc8

Crc8Table PlainBufferCrc8::mCrc8Table;

int8_t PlainBufferCrc8::CrcInt8(int8_t crc, int8_t in)
{
    crc = mCrc8Table[(crc ^ in) & 0xff];
    return crc;
}

int8_t PlainBufferCrc8::CrcInt32(int8_t crc, int32_t in) {
    for (int32_t i = 0; i < 4; ++i) {
        crc = CrcInt8(crc, static_cast<int8_t>(in & 0xff));
        in >>= 8;
    }
    return crc;
}

int8_t PlainBufferCrc8::CrcInt64(int8_t crc, int64_t in) {
    for (int32_t i = 0; i < 8; ++i) {
        crc = CrcInt8(crc, static_cast<int8_t>(in & 0xff));
        in >>= 8;
    }
    return crc;
}

int8_t PlainBufferCrc8::CrcString(int8_t crc, const string& in) {
    for (int32_t i = 0; i < static_cast<int32_t>(in.size()); ++i) {
        crc = CrcInt8(crc, in[i]);
    }
    return crc;
}

} // end of tablestore
} // end of aliyun
