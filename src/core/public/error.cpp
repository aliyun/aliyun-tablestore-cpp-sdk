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

#include "core/error.hpp"
#include "util/mempiece.hpp"
#include "util/prettyprint.hpp"

using namespace std;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {

void Error::prettyPrint(string* out) const
{
    out->append("{\"HttpStatus\": ");
    pp::prettyPrint(out, mHttpStatus);
    out->append(", \"ErrorCode\": \"");
    out->append(mErrorCode);
    out->append("\", \"Message\": \"");
    out->append(mMessage);
    if (!mRequestId.empty()) {
        out->append("\", \"RequestId\": \"");
        out->append(mRequestId);
    }
    if (!mTraceId.empty()) {
        out->append("\", \"TraceId\": \"");
        out->append(mTraceId);
    }
    out->append("\"}");
}

bool isOk(const Error& err)
{
    return err.httpStatus() >= 200 && err.httpStatus() <= 299;
}

bool isCurlError(const Error& err)
{
    return err.httpStatus() > 0 && err.httpStatus() <= 99;
}

bool isSdkError(const Error& err)
{
    return err.httpStatus() < 0;
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
        case 5: // CURLE_COULDNT_RESOLVE_PROXY
        case 6: // CURLE_COULDNT_RESOLVE_HOST
        case 7: // CURLE_COULDNT_CONNECT
        case 28: // CURLE_OPERATION_TIMEDOUT
        case 35: // CURLE_SSL_CONNECT_ERROR
        case 51: // CURLE_PEER_FAILED_VERIFICATION
        case 52: // CURLE_GOT_NOTHING
        case 55: // CURLE_SEND_ERROR
        case 56: // CURLE_RECV_ERROR
        case 70: // CURLE_REMOTE_DISK_FULL
        case 81: // CURLE_AGAIN
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

