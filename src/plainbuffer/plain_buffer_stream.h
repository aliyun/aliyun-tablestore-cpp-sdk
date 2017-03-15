/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#ifndef PLAIN_BUFFER_STREAM_H
#define PLAIN_BUFFER_STREAM_H

#include "ots/ots_exception.h"
#include <string>
#include <stdint.h>

namespace aliyun {
namespace tablestore {

const int LITTLE_ENDIAN_32_SIZE = 4;
const int LITTLE_ENDIAN_64_SIZE = 8;
const int MAX_BUFFER_SIZE = 64 * 1024 * 1024;;

/**
 * @brief PlainBufferInputStream
 *
 * 解析PlainBuffer字符流。
 */
class PlainBufferInputStream
{
public:

    PlainBufferInputStream(const std::string& buffer);

    bool IsAtEnd() const
    {
        return mBuffer.size() == static_cast<size_t>(mCurPos);
    }

    int32_t ReadTag()
    {
        if (IsAtEnd()) {
            mLastTag = 0;
            return 0;
        }
        mLastTag = ReadRawByte();
        return mLastTag;
    }

    bool CheckLastTagWas(int tag)
    {
        return mLastTag == tag;
    }

    int32_t GetLastTag()
    {
        return mLastTag;
    }

    int8_t ReadRawByte()
    {
        if (IsAtEnd()) {
            throw OTSClientException("Read raw byte encountered EOF.");
        }
        return mBuffer[mCurPos++];
    }

    int64_t ReadRawLittleEndian64()
    {
        int8_t b1 = ReadRawByte();
        int8_t b2 = ReadRawByte();
        int8_t b3 = ReadRawByte();
        int8_t b4 = ReadRawByte();
        int8_t b5 = ReadRawByte();
        int8_t b6 = ReadRawByte();
        int8_t b7 = ReadRawByte();
        int8_t b8 = ReadRawByte();
        return (((int64_t) b1 & 0xff)) |
            (((int64_t) b2 & 0xff) << 8) |
            (((int64_t) b3 & 0xff) << 16) |
            (((int64_t) b4 & 0xff) << 24) |
            (((int64_t) b5 & 0xff) << 32) |
            (((int64_t) b6 & 0xff) << 40) |
            (((int64_t) b7 & 0xff) << 48) |
            (((int64_t) b8 & 0xff) << 56);
    }

    int32_t ReadRawLittleEndian32()
    {
        int8_t b1 = ReadRawByte();
        int8_t b2 = ReadRawByte();
        int8_t b3 = ReadRawByte();
        int8_t b4 = ReadRawByte();
        return (((int32_t) b1 & 0xff)) |
            (((int32_t) b2 & 0xff) << 8) |
            (((int32_t) b3 & 0xff) << 16) |
            (((int32_t) b4 & 0xff) << 24);
    }

    bool ReadBoolean()
    {
        return ReadRawByte() != 0;
    }

    union DoubleDecoder
    {
        int64_t int64Value;
        double doubleValue;
    };
    
    double ReadDouble()
    {
        DoubleDecoder dd;
        dd.int64Value = ReadRawLittleEndian64();
        return dd.doubleValue;
    }

    int32_t ReadInt32()
    {
        return ReadRawLittleEndian32();
    }

    int64_t ReadInt64()
    {
        return ReadRawLittleEndian64();
    }

    std::string ReadBytes(int32_t size)
    {
        if (mBuffer.size() - mCurPos < static_cast<size_t>(size)) {
            throw OTSClientException("Read bytes encountered EOF.");
        }
        size_t tmpPos = mCurPos;
        mCurPos += size;
        return mBuffer.substr(tmpPos, size);
    }

    std::string ReadUTFString(int32_t size)
    {
        if (mBuffer.size() - mCurPos < static_cast<size_t>(size)) {
            throw OTSClientException("Read UTF string encountered EOF.");
        }
        std::string str = mBuffer.substr(mCurPos, size);
        mCurPos += size;
        return str;
    }

private:

    std::string mBuffer;

    size_t mCurPos;

    int32_t mLastTag;
};

/**
 * @brief PlainBufferOutputStream
 *
 * 构造PlainBuffer字符流。
 */
class PlainBufferOutputStream
{
public:

    PlainBufferOutputStream(int32_t capacity);

    const std::string& GetBuffer()
    {
        return mBuffer;
    }

    bool IsFull()
    {
        return mBuffer.size() == mBuffer.capacity();
    }

    int32_t Count()
    {
        return static_cast<int32_t>(mBuffer.size());
    }

    int32_t Remain()
    {
        return static_cast<int32_t>(mBuffer.capacity() - mBuffer.size());
    }

    void Clear()
    {
        mBuffer.clear();
    }

    void WriteRawByte(int8_t value)
    {
        if (IsFull()) {
            throw OTSClientException("The buffer is full.");
        }
        mBuffer.append(1, value);
    }

    // Write a little-endian 32-bit integer.
    void WriteRawLittleEndian32(int32_t value)
    {
        WriteRawByte((value) & 0xFF);
        WriteRawByte((value >> 8) & 0xFF);
        WriteRawByte((value >> 16) & 0xFF);
        WriteRawByte((value >> 24) & 0xFF);
    }

    // Write a little-endian 64-bit integer.
    void WriteRawLittleEndian64(int64_t value)
    {
        WriteRawByte((int) (value) & 0xFF);
        WriteRawByte((int) (value >> 8) & 0xFF);
        WriteRawByte((int) (value >> 16) & 0xFF);
        WriteRawByte((int) (value >> 24) & 0xFF);
        WriteRawByte((int) (value >> 32) & 0xFF);
        WriteRawByte((int) (value >> 40) & 0xFF);
        WriteRawByte((int) (value >> 48) & 0xFF);
        WriteRawByte((int) (value >> 56) & 0xFF);
    }

    void WriteDouble(double value)
    {
        int64_t* int64Value = reinterpret_cast<int64_t*>(&value);
        WriteRawLittleEndian64(*int64Value);
    }

    void WriteBoolean(bool value)
    {
        WriteRawByte(value ? 1 : 0);
    }

    void WriteBytes(const std::string& value)
    {
        if (mBuffer.size() + value.size() > mBuffer.capacity()) {
            throw OTSClientException("The buffer is full.");
        }
        mBuffer.append(value);
    }

private:

    std::string mBuffer;
};

} // end of tablestore
} // end of aliyun

#endif
