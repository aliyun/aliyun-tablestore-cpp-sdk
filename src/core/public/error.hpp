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

#include "util/move.hpp"
#include <string>
#include <stdint.h>

namespace aliyun {
namespace tablestore {
namespace core {

class Error
{
public:
    explicit Error()
      : mHttpStatus(0)
    {}
    explicit Error(
        int64_t httpStatus,
        const std::string& errorCode,
        const std::string& message,
        const std::string& traceId = std::string(),
        const std::string& requestId = std::string())
      : mHttpStatus(httpStatus),
        mErrorCode(errorCode),
        mMessage(message),
        mRequestId(requestId),
        mTraceId(traceId)
    {}
    ~Error() {}

    explicit Error(const util::MoveHolder<Error>& ano)
      : mHttpStatus(ano->httpStatus()),
        mErrorCode(ano->errorCode()),
        mMessage(ano->message()),
        mRequestId(ano->requestId()),
        mTraceId(ano->traceId())
    {}

    Error& operator=(const util::MoveHolder<Error>& ano)
    {
        util::moveAssign(&mHttpStatus, util::move(ano->mHttpStatus));
        util::moveAssign(&mErrorCode, util::move(ano->mErrorCode));
        util::moveAssign(&mMessage, util::move(ano->mMessage));
        util::moveAssign(&mRequestId, util::move(ano->mRequestId));
        util::moveAssign(&mTraceId, util::move(ano->mTraceId));
        return *this;
    }

    const std::string& errorCode() const throw()
    {
        return mErrorCode;
    }

    std::string* mutableErrorCode() throw()
    {
        return &mErrorCode;
    }

    const std::string& message() const
    {
        return mMessage;
    }

    std::string* mutableMessage() throw()
    {
        return &mMessage;
    }
    

    const std::string& requestId() const throw()
    {
        return mRequestId;
    }

    std::string* mutableRequestId() throw()
    {
        return &mRequestId;
    }
    

    const std::string& traceId() const throw()
    {
        return mTraceId;
    }

    std::string* mutableTraceId() throw()
    {
        return &mTraceId;
    }
    
    int64_t httpStatus() const throw()
    {
        return mHttpStatus;
    }

    int64_t* mutableHttpStatus() throw()
    {
        return &mHttpStatus;
    }
    
    void prettyPrint(std::string*) const;

private:
    int64_t mHttpStatus;
    std::string mErrorCode;
    std::string mMessage;
    std::string mRequestId;
    std::string mTraceId;
};

bool isOk(const Error&);
bool isCurlError(const Error&);
bool isSdkError(const Error&);
bool isTemporary(const Error&);

} // namespace core
} // namespace tablestore
} // namespace aliyun

