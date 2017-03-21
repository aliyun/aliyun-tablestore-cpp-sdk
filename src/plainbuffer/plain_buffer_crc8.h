/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#ifndef PLAIN_BUFFER_CRC8_H
#define PLAIN_BUFFER_CRC8_H

#include <string>
#include <stdint.h>

namespace aliyun {
namespace tablestore {

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
            char x = (int8_t) i;
            for (int32_t j = 8; j > 0; --j) {
                x = (int8_t) ((x << 1) ^ (((x & 0x80) != 0) ? 0x07 : 0));
            }
            mDataTable[i] = x;
        }
    }

    inline int8_t operator[](int32_t index)
    {
        return mDataTable[index];
    }

private:

    int8_t mDataTable[kCrc8TableSize];
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

    static int8_t CrcInt8(int8_t crc, int8_t in);
    
    static int8_t CrcInt32(int8_t crc, int32_t in);
    
    static int8_t CrcInt64(int8_t crc, int64_t in);
    
    static int8_t CrcString(int8_t crc, const std::string& in);
    
private:

    static Crc8Table mCrc8Table;
};

} // end of tablestore
} // end of aliyun

#endif
