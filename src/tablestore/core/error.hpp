#pragma once
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
#include "tablestore/util/move.hpp"
#include <string>
#include <stdint.h>

namespace aliyun {
namespace tablestore {
namespace core {

class Error
{
public:
    /*
     * HTTP status that indicate SDK errors.
     */
    static const int64_t kHttpStatus_CouldntResolveHost = 6;
    static const int64_t kHttpStatus_CouldntConnect = 7;
    static const int64_t kHttpStatus_OperationTimeout = 28;
    static const int64_t kHttpStatus_WriteRequestFail = 55;
    static const int64_t kHttpStatus_CorruptedResponse = 56;
    static const int64_t kHttpStatus_NoAvailableConnection = 89;
    static const int64_t kHttpStatus_SslHandshakeFail = 35;
    
    /*
     * Error codes that indicate SDK errors.
     */
    static const std::string kErrorCode_CouldntResolveHost;
    static const std::string kErrorCode_CouldntConnect;
    static const std::string kErrorCode_WriteRequestFail;
    static const std::string kErrorCode_CorruptedResponse;
    static const std::string kErrorCode_NoAvailableConnection;
    static const std::string kErrorCode_OTSRequestTimeout;
    static const std::string kErrorCode_SslHandshakeFail;

    /*
     * Error codes reported by TableStore servers
     */
    static const std::string kErrorCode_OTSOutOfColumnCountLimit;
    static const std::string kErrorCode_OTSObjectNotExist;
    static const std::string kErrorCode_OTSServerBusy;
    static const std::string kErrorCode_OTSCapacityUnitExhausted;
    static const std::string kErrorCode_OTSTooFrequentReservedThroughputAdjustment;
    static const std::string kErrorCode_OTSInternalServerError;
    static const std::string kErrorCode_OTSQuotaExhausted;
    static const std::string kErrorCode_OTSRequestBodyTooLarge;
    static const std::string kErrorCode_OTSTimeout;
    static const std::string kErrorCode_OTSObjectAlreadyExist;
    static const std::string kErrorCode_OTSTableNotReady;
    static const std::string kErrorCode_OTSConditionCheckFail;
    static const std::string kErrorCode_OTSOutOfRowSizeLimit;
    static const std::string kErrorCode_OTSInvalidPK;
    static const std::string kErrorCode_OTSMethodNotAllowed;
    static const std::string kErrorCode_OTSAuthFailed;
    static const std::string kErrorCode_OTSServerUnavailable;
    static const std::string kErrorCode_OTSParameterInvalid;
    static const std::string kErrorCode_OTSRowOperationConflict;
    static const std::string kErrorCode_OTSPartitionUnavailable;

    enum Predefined
    {
        kPredefined_CouldntResoveHost,
        kPredefined_CouldntConnect,
        kPredefined_OperationTimeout,
        kPredefined_WriteRequestFail,
        kPredefined_CorruptedResponse,
        kPredefined_NoConnectionAvailable,
        kPredefined_SslHandshakeFail,

        kPredefined_OTSOutOfColumnCountLimit,
        kPredefined_OTSObjectNotExist,
        kPredefined_OTSServerBusy,
        kPredefined_OTSCapacityUnitExhausted,
        kPredefined_OTSTooFrequentReservedThroughputAdjustment,
        kPredefined_OTSInternalServerError,
        kPredefined_OTSQuotaExhausted,
        kPredefined_OTSRequestBodyTooLarge,
        kPredefined_OTSTimeout,
        kPredefined_OTSObjectAlreadyExist,
        kPredefined_OTSTableNotReady,
        kPredefined_OTSConditionCheckFail,
        kPredefined_OTSOutOfRowSizeLimit,
        kPredefined_OTSInvalidPK,
        kPredefined_OTSMethodNotAllowed,
        kPredefined_OTSAuthFailed,
        kPredefined_OTSServerUnavailable,
        kPredefined_OTSParameterInvalid,
        kPredefined_OTSRowOperationConflict,
        kPredefined_OTSPartitionUnavailable,
    };
    
    explicit Error()
      : mHttpStatus(200)
    {}
    explicit Error(Predefined);
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
        util::moveAssign(mHttpStatus, util::move(ano->mHttpStatus));
        util::moveAssign(mErrorCode, util::move(ano->mErrorCode));
        util::moveAssign(mMessage, util::move(ano->mMessage));
        util::moveAssign(mRequestId, util::move(ano->mRequestId));
        util::moveAssign(mTraceId, util::move(ano->mTraceId));
        return *this;
    }

    const std::string& errorCode() const throw()
    {
        return mErrorCode;
    }

    std::string& mutableErrorCode() throw()
    {
        return mErrorCode;
    }

    const std::string& message() const
    {
        return mMessage;
    }

    std::string& mutableMessage() throw()
    {
        return mMessage;
    }
    

    const std::string& requestId() const throw()
    {
        return mRequestId;
    }

    std::string& mutableRequestId() throw()
    {
        return mRequestId;
    }
    

    const std::string& traceId() const throw()
    {
        return mTraceId;
    }

    std::string& mutableTraceId() throw()
    {
        return mTraceId;
    }
    
    int64_t httpStatus() const throw()
    {
        return mHttpStatus;
    }

    int64_t& mutableHttpStatus() throw()
    {
        return mHttpStatus;
    }
    
    void prettyPrint(std::string&) const;

private:
    void init(int64_t, const std::string&);

private:
    int64_t mHttpStatus;
    std::string mErrorCode;
    std::string mMessage;
    std::string mRequestId;
    std::string mTraceId;
};

bool isCurlError(const Error&);
bool isTemporary(const Error&);


} // namespace core
} // namespace tablestore
} // namespace aliyun

