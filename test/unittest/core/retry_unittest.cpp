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
#include "core/retry.hpp"
#include "core/types.hpp"
#include "core/error.hpp"
#include "util/prettyprint.hpp"
#include "util/foreach.hpp"
#include "testa/testa.hpp"
#include <tr1/tuple>
#include <deque>
#include <map>
#include <string>

using namespace std;
using namespace std::tr1;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {

namespace {

bool isClientError(int httpStatus)
{
    return (httpStatus < 200);
}

bool idempotent(Action act)
{
    if (act == API_LIST_TABLE ||
        act == API_DESCRIBE_TABLE ||
        act == API_GET_ROW ||
        act == API_BATCH_GET_ROW ||
        act == API_GET_RANGE ||
        act == API_COMPUTE_SPLIT_POINTS_BY_SIZE) {
        return true;
    }
    return false;
}

bool retriable(
    const Error& error,
    bool idempotent)
{
    int httpStatus = error.httpStatus();
    if (httpStatus >= 200 && httpStatus <= 299) {
        return false;
    }
    const string errorCode = error.errorCode();
    const string errorMessage = error.message();
    if (errorCode == "OTSRowOperationConflict" ||
        errorCode == "OTSNotEnoughCapacityUnit" || 
        errorCode == "OTSTableNotReady" ||
        errorCode == "OTSPartitionUnavailable" ||
        errorCode == "OTSServerBusy" ||
        errorCode == "OTSCapacityUnitExhausted" ||
        errorCode == "OTSTooFrequentReservedThroughputAdjustment" ||
        (errorCode == "OTSQuotaExhausted" && errorMessage == "Too frequent table operations.")) {
        return true;
    }
    bool isServerError = (httpStatus >= 500 && httpStatus <= 599);
    if (idempotent && 
        (errorCode == "OTSTimeout" ||
         errorCode == "OTSInternalServerError" ||
         errorCode == "OTSServerUnavailable" ||
         errorCode == "OTSRequestTimeout" ||
         isServerError)) {
        return true;
    }
    return false;
}

} // namespace

bool DefaultRetryStrategy_retriable_oracle(const tuple<Action, Error>& in)
{
    Action act = get<0>(in);
    const Error& err = get<1>(in);
    bool isIdempotent = idempotent(act);
    if (isClientError(err.httpStatus())) {
        return isIdempotent;
    } else {
        return retriable(err, isIdempotent);
    }
}

bool DefaultRetryStrategy_retriable_trial(const tuple<Action, Error>& in)
{
    return DefaultRetryStrategy::retriable(get<0>(in), get<1>(in));
}

void DefaultRetryStrategy_retriable_tb(function<void(const tuple<Action, Error>&)> cs)
{
    map<string, int> codeStatuses;
    codeStatuses["OTSAuthFailed"] = 403;
    codeStatuses["OTSRequestBodyTooLarge"] = 413;
    codeStatuses["OTSRequestTimeout"] = 408;
    codeStatuses["OTSMethodNotAllowed"] = 405;
    codeStatuses["OTSParameterInvalid"] = 400;
    codeStatuses["OTSInternalServerError"] = 500;
    codeStatuses["OTSQuotaExhausted"] = 403;
    codeStatuses["OTSServerBusy"] = 503;
    codeStatuses["OTSPartitionUnavailable"] = 503;
    codeStatuses["OTSTimeout"] = 503;
    codeStatuses["OTSServerUnavailable"] = 503;
    codeStatuses["OTSRowOperationConflict"] = 409;
    codeStatuses["OTSObjectAlreadyExist"] = 409;
    codeStatuses["OTSObjectNotExist"] = 404;
    codeStatuses["OTSTableNotReady"] = 404;
    codeStatuses["OTSTooFrequentReservedThroughputAdjustment"] = 403;
    codeStatuses["OTSCapacityUnitExhausted"] = 403;
    codeStatuses["OTSConditionCheckFail"] = 403;
    codeStatuses["OTSOutOfRowSizeLimit"] = 400;
    codeStatuses["OTSOutOfColumnCountLimit"] = 400;
    codeStatuses["OTSInvalidPK"] = 400;
    map<int, deque<string> > statusCodes;
    FOREACH_ITER(i, codeStatuses) {
        statusCodes[i->second].push_back(i->first);
    }
    statusCodes[28].push_back(""); // curl error
    statusCodes[200].push_back(""); // ok
    deque<Action> actions;
    collectActions(&actions);
    deque<string> errMsgs;
    errMsgs.push_back("Too frequent table operations.");
    errMsgs.push_back("whatever");
    FOREACH_ITER(i, statusCodes) {
        int httpStatus = i->first;
        FOREACH_ITER(j, i->second) {
            const string& errCode = *j;
            FOREACH_ITER(k, errMsgs) {
                const string& errMsg = *k;
                FOREACH_ITER(m, actions) {
                    Action act = *m;
                    Error err(httpStatus, errCode, errMsg);
                    cs(make_tuple(act, err));
                }
            }
        }
    }
}
TESTA_DEF_EQ_WITH_TB(DefaultRetryStrategy_retriable,
    DefaultRetryStrategy_retriable_tb,
    DefaultRetryStrategy_retriable_trial, DefaultRetryStrategy_retriable_oracle);

} // namespace core
} // namespace tablestore
} // namespace aliyun

