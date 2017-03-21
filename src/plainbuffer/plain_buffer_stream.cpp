/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#include "plain_buffer_stream.h"

using namespace std;

namespace aliyun {
namespace tablestore {

// PlainBufferInputStream

PlainBufferInputStream::PlainBufferInputStream(const std::string& buffer)
    : mBuffer(buffer)
    , mCurPos(0)
    , mLastTag(0)
{
}

// PlainBufferOutputStream

PlainBufferOutputStream::PlainBufferOutputStream(int capacity)
{
    if (capacity <= 0 || capacity > MAX_BUFFER_SIZE) {
        throw OTSClientException("The capacity of output stream is out of range.");
    }
    mBuffer.reserve(capacity);
}

} // end of tablestore
} // end of aliyun
