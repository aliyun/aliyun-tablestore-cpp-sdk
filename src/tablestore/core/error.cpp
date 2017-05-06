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

#include "tablestore/core/error.hpp"
#include "tablestore/util/mempiece.hpp"
#include "tablestore/util/prettyprint.hpp"

using namespace std;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {

const string Error::kErrorCode_CouldntResolveHost("OTSCouldntResolveHost");
const string Error::kErrorCode_CouldntConnect("OTSCouldntConnect");
const string Error::kErrorCode_WriteRequestFail("OTSWriteRequestFail");
const string Error::kErrorCode_CorruptedResponse("OTSCorruptedResponse");
const string Error::kErrorCode_NoAvailableConnection("OTSNoAvailableConnection");
const string Error::kErrorCode_SslHandshakeFail("OTSSslHandshakeFail");

const string Error::kErrorCode_OTSOutOfColumnCountLimit("OTSOutOfColumnCountLimit");
const string Error::kErrorCode_OTSObjectNotExist("OTSObjectNotExist");
const string Error::kErrorCode_OTSServerBusy("OTSServerBusy");
const string Error::kErrorCode_OTSCapacityUnitExhausted("OTSCapacityUnitExhausted");
const string Error::kErrorCode_OTSTooFrequentReservedThroughputAdjustment(
    "OTSTooFrequentReservedThroughputAdjustment");
const string Error::kErrorCode_OTSInternalServerError("OTSInternalServerError");
const string Error::kErrorCode_OTSQuotaExhausted("OTSQuotaExhausted");
const string Error::kErrorCode_OTSRequestBodyTooLarge("OTSRequestBodyTooLarge");
const string Error::kErrorCode_OTSTimeout("OTSTimeout");
const string Error::kErrorCode_OTSObjectAlreadyExist("OTSObjectAlreadyExist");
const string Error::kErrorCode_OTSTableNotReady("OTSTableNotReady");
const string Error::kErrorCode_OTSConditionCheckFail("OTSConditionCheckFail");
const string Error::kErrorCode_OTSOutOfRowSizeLimit("OTSOutOfRowSizeLimit");
const string Error::kErrorCode_OTSInvalidPK("OTSInvalidPK");
const string Error::kErrorCode_OTSRequestTimeout("OTSRequestTimeout");
const string Error::kErrorCode_OTSMethodNotAllowed("OTSMethodNotAllowed");
const string Error::kErrorCode_OTSAuthFailed("OTSAuthFailed");
const string Error::kErrorCode_OTSServerUnavailable("OTSServerUnavailable");
const string Error::kErrorCode_OTSParameterInvalid("OTSParameterInvalid");
const string Error::kErrorCode_OTSRowOperationConflict("OTSRowOperationConflict");
const string Error::kErrorCode_OTSPartitionUnavailable("OTSPartitionUnavailable");

Error::Error(Predefined def)
{
    switch(def) {
    case kPredefined_CouldntResoveHost:
        init(kHttpStatus_CouldntResolveHost, kErrorCode_CouldntResolveHost);
        break;
    case kPredefined_CouldntConnect:
        init(kHttpStatus_CouldntConnect, kErrorCode_CouldntConnect);
        break;
    case kPredefined_OperationTimeout:
        init(kHttpStatus_OperationTimeout, kErrorCode_OTSRequestTimeout);
        break;
    case kPredefined_WriteRequestFail:
        init(kHttpStatus_WriteRequestFail, kErrorCode_WriteRequestFail);
        break;
    case kPredefined_CorruptedResponse:
        init(kHttpStatus_CorruptedResponse, kErrorCode_CorruptedResponse);
        break;
    case kPredefined_NoConnectionAvailable:
        init(kHttpStatus_NoAvailableConnection, kErrorCode_NoAvailableConnection);
        break;
    case kPredefined_OTSOutOfColumnCountLimit:
        init(400, kErrorCode_OTSOutOfColumnCountLimit);
        break;
    case kPredefined_OTSObjectNotExist:
        init(404, kErrorCode_OTSObjectNotExist);
        break;
    case kPredefined_OTSServerBusy:
        init(503, kErrorCode_OTSServerBusy);
        break;
    case kPredefined_OTSCapacityUnitExhausted:
        init(403, kErrorCode_OTSCapacityUnitExhausted);
        break;
    case kPredefined_OTSTooFrequentReservedThroughputAdjustment:
        init(403, kErrorCode_OTSTooFrequentReservedThroughputAdjustment);
        break;
    case kPredefined_OTSInternalServerError:
        init(500, kErrorCode_OTSInternalServerError);
        break;
    case kPredefined_OTSQuotaExhausted:
        init(403, kErrorCode_OTSQuotaExhausted);
        break;
    case kPredefined_OTSRequestBodyTooLarge:
        init(413, kErrorCode_OTSRequestBodyTooLarge);
        break;
    case kPredefined_OTSTimeout:
        init(503, kErrorCode_OTSTimeout);
        break;
    case kPredefined_OTSObjectAlreadyExist:
        init(409, kErrorCode_OTSObjectAlreadyExist);
        break;
    case kPredefined_OTSTableNotReady:
        init(404, kErrorCode_OTSTableNotReady);
        break;
    case kPredefined_OTSConditionCheckFail:
        init(403, kErrorCode_OTSConditionCheckFail);
        break;
    case kPredefined_OTSOutOfRowSizeLimit:
        init(400, kErrorCode_OTSOutOfRowSizeLimit);
        break;
    case kPredefined_OTSInvalidPK:
        init(400, kErrorCode_OTSInvalidPK);
        break;
    case kPredefined_OTSMethodNotAllowed:
        init(405, kErrorCode_OTSMethodNotAllowed);
        break;
    case kPredefined_OTSAuthFailed:
        init(403, kErrorCode_OTSAuthFailed);
        break;
    case kPredefined_OTSServerUnavailable:
        init(503, kErrorCode_OTSServerUnavailable);
        break;
    case kPredefined_OTSParameterInvalid:
        init(400, kErrorCode_OTSParameterInvalid);
        break;
    case kPredefined_OTSRowOperationConflict:
        init(409, kErrorCode_OTSRowOperationConflict);
        break;
    case kPredefined_OTSPartitionUnavailable:
        init(503, kErrorCode_OTSPartitionUnavailable);
        break;
    case kPredefined_SslHandshakeFail:
        init(kHttpStatus_SslHandshakeFail, kErrorCode_SslHandshakeFail);
        break;
    }
}

void Error::init(int64_t hs, const string& ec)
{
    mHttpStatus = hs;
    mErrorCode = ec;
}

void Error::prettyPrint(string& out) const
{
    out.append("{\"HttpStatus\": ");
    pp::prettyPrint(out, mHttpStatus);
    out.append(", \"ErrorCode\": \"");
    out.append(mErrorCode);
    out.append("\", \"Message\": \"");
    out.append(mMessage);
    if (!mRequestId.empty()) {
        out.append("\", \"RequestId\": \"");
        out.append(mRequestId);
    }
    if (!mTraceId.empty()) {
        out.append("\", \"TraceId\": \"");
        out.append(mTraceId);
    }
    out.append("\"}");
}

bool isCurlError(const Error& err)
{
    return err.httpStatus() > 0 && err.httpStatus() <= 99;
}

bool isTemporary(const Error& err)
{
    if (err.httpStatus() >= 500 && err.httpStatus() <= 599) {
        return true;
    }
    if (err.httpStatus() >= 400 && err.httpStatus() <= 499) {
        MemPiece code = MemPiece::from(err.errorCode());
        MemPiece msg = MemPiece::from(err.message());
        if (code == MemPiece::from("OTSQuotaExhausted") && msg == MemPiece::from("Too frequent table operations.")) {
            return true;
        } else if (code == MemPiece::from("OTSRowOperationConflict")) {
            return true;
        } else if (code == MemPiece::from("OTSTableNotReady")) {
            return true;
        } else if (code == MemPiece::from("OTSTooFrequentReservedThroughputAdjustment")) {
            return true;
        } else if (code == MemPiece::from("OTSCapacityUnitExhausted")) {
            return true;
        } else if (code == MemPiece::from("OTSRequestTimeout")) {
            return true;
        }
    }
    if (isCurlError(err)) {
        switch(err.httpStatus()) {
        case Error::kHttpStatus_CouldntResolveHost:
        case Error::kHttpStatus_CouldntConnect:
        case Error::kHttpStatus_OperationTimeout:
        case Error::kHttpStatus_WriteRequestFail:
        case Error::kHttpStatus_CorruptedResponse:
        case Error::kHttpStatus_NoAvailableConnection:
            return true;
        default:
            break;
        }
    }
    return false;
}

} // namespace core
} // namespace tablestore
} // namespace aliyun

