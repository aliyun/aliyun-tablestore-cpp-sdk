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
#include "tablestore/core/retry.hpp"
#include "tablestore/core/types.hpp"
#include "tablestore/core/error.hpp"
#include "tablestore/util/prettyprint.hpp"
#include "tablestore/util/foreach.hpp"
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

void collectPredefinedErrors(deque<Error::Predefined>& def)
{
    def.push_back(Error::kPredefined_CouldntResoveHost);
    def.push_back(Error::kPredefined_CouldntConnect);
    def.push_back(Error::kPredefined_OperationTimeout);
    def.push_back(Error::kPredefined_WriteRequestFail);
    def.push_back(Error::kPredefined_CorruptedResponse);
    def.push_back(Error::kPredefined_NoConnectionAvailable);
    def.push_back(Error::kPredefined_OTSOutOfColumnCountLimit);
    def.push_back(Error::kPredefined_OTSObjectNotExist);
    def.push_back(Error::kPredefined_OTSServerBusy);
    def.push_back(Error::kPredefined_OTSCapacityUnitExhausted);
    def.push_back(Error::kPredefined_OTSTooFrequentReservedThroughputAdjustment);
    def.push_back(Error::kPredefined_OTSInternalServerError);
    def.push_back(Error::kPredefined_OTSQuotaExhausted);
    def.push_back(Error::kPredefined_OTSRequestBodyTooLarge);
    def.push_back(Error::kPredefined_OTSTimeout);
    def.push_back(Error::kPredefined_OTSObjectAlreadyExist);
    def.push_back(Error::kPredefined_OTSTableNotReady);
    def.push_back(Error::kPredefined_OTSConditionCheckFail);
    def.push_back(Error::kPredefined_OTSOutOfRowSizeLimit);
    def.push_back(Error::kPredefined_OTSInvalidPK);
    def.push_back(Error::kPredefined_OTSMethodNotAllowed);
    def.push_back(Error::kPredefined_OTSAuthFailed);
    def.push_back(Error::kPredefined_OTSServerUnavailable);
    def.push_back(Error::kPredefined_OTSParameterInvalid);
    def.push_back(Error::kPredefined_OTSRowOperationConflict);
    def.push_back(Error::kPredefined_OTSPartitionUnavailable);
}

bool idempotent(Action act)
{
    switch(act) {
    case kApi_ListTable: case kApi_DescribeTable: case kApi_DeleteTable:
    case kApi_CreateTable: case kApi_ComputeSplitsBySize:
    case kApi_GetRow: case kApi_BatchGetRow: case kApi_GetRange:
    case kApi_DeleteRow:
        return true;
    case kApi_UpdateTable: case kApi_PutRow: case kApi_UpdateRow: case kApi_BatchWriteRow:
        return false;
    }
    OTS_ASSERT(false)((int) act);
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
    if (httpStatus == Error::kHttpStatus_CouldntConnect) {
        return true;
    }
    if (httpStatus == Error::kHttpStatus_CouldntResolveHost) {
        return true;
    }
    if (httpStatus == Error::kHttpStatus_NoAvailableConnection) {
        return true;
    }
    if (httpStatus == Error::kHttpStatus_WriteRequestFail) {
        return idempotent;
    }
    if (httpStatus == Error::kHttpStatus_CorruptedResponse) {
        return idempotent;
    }
    if (httpStatus == Error::kHttpStatus_OperationTimeout) {
        return idempotent;
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
    return retriable(err, isIdempotent);
}

bool DefaultRetryStrategy_retriable_trial(const tuple<Action, Error>& in)
{
    return RetryStrategy::retriable(get<0>(in), get<1>(in));
}

void DefaultRetryStrategy_retriable_tb(const string&, function<void(const tuple<Action, Error>&)> cs)
{
    deque<Error::Predefined> defs;
    collectPredefinedErrors(defs);
    deque<Action> actions;
    collectEnum(actions);
    deque<string> errMsgs;
    errMsgs.push_back("Too frequent table operations.");
    errMsgs.push_back("whatever");
    FOREACH_ITER(i, defs) {
        Error::Predefined def = *i;
        FOREACH_ITER(k, errMsgs) {
            const string& errMsg = *k;
            FOREACH_ITER(m, actions) {
                Action act = *m;
                Error err(def);
                err.mutableMessage() = errMsg;
                cs(make_tuple(act, err));
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

