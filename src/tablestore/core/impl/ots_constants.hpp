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

#include <string>

namespace aliyun {
namespace tablestore {
namespace core {

// request
const std::string kAPIVersion = "2015-12-31";
const std::string kOTSContentMD5 = "x-ots-contentmd5";
const std::string kOTSDate = "x-ots-date";
const std::string kOTSAPIVersion = "x-ots-apiversion";
const std::string kOTSAccessKeyId = "x-ots-accesskeyid";
const std::string kOTSStsToken = "x-ots-ststoken";
const std::string kOTSInstanceName = "x-ots-instancename";
const std::string kOTSSignature = "x-ots-signature";
const std::string kOTSTraceId = "x-ots-sdk-traceid";

const std::string kOTSHeaderPrefix = "x-ots-";

const std::string kUserAgent = "User-Agent";
const std::string kSDKUserAgent = "aliyun-tablestore-sdk-cpp98/4.3.0(amd64;)";

// response
const std::string kOTSRequestId = "x-ots-requestid";
const std::string kOTSAuthorization = "Authorization";
const std::string kOTSTraceInfo = "x-ots-traceinfo";
const std::string kOTSContentType = "x-ots-contenttype";

// APIs
const std::string kAPICreateTable = "CreateTable";
const std::string kAPIListTable = "ListTable";
const std::string kAPIDescribeTable = "DescribeTable";
const std::string kAPIDeleteTable = "DeleteTable";
const std::string kAPIUpdateTable = "UpdateTable";
const std::string kAPIGetRow = "GetRow";
const std::string kAPIPutRow = "PutRow";
const std::string kAPIUpdateRow = "UpdateRow";
const std::string kAPIDeleteRow = "DeleteRow";
const std::string kAPIBatchGetRow = "BatchGetRow";
const std::string kAPIBatchWriteRow = "BatchWriteRow";
const std::string kAPIGetRange = "GetRange";
const std::string kAPIComputeSplitPointsBySize = "ComputeSplitPointsBySize";

// URIs
const std::string kURICreateTable = "/CreateTable";
const std::string kURIListTable = "/ListTable";
const std::string kURIDescribeTable = "/DescribeTable";
const std::string kURIDeleteTable = "/DeleteTable";
const std::string kURIUpdateTable = "/UpdateTable";
const std::string kURIGetRow = "/GetRow";
const std::string kURIPutRow = "/PutRow";
const std::string kURIUpdateRow = "/UpdateRow";
const std::string kURIDeleteRow = "/DeleteRow";
const std::string kURIBatchGetRow = "/BatchGetRow";
const std::string kURIBatchWriteRow = "/BatchWriteRow";
const std::string kURIGetRange = "/GetRange";
const std::string kURIComputeSplitPointsBySize = "/ComputeSplitPointsBySize";

// Profiling states
const std::string kProfPreProcess = "PRE_PROCESS";
const std::string kProfSendRequest = "SEND_REQUEST";
const std::string kProfPostProcess = "POST_PROCESS";
const std::string kProfFinishProcess = "FINISH_PROCESS";

} // namespace core
} // namespace tablestore
} // namespace aliyun

