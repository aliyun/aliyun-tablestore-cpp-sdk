/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#ifndef OTS_CONSTANTS_H
#define OTS_CONSTANTS_H

#include <string>

namespace aliyun {
namespace tablestore {

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

const std::string kUserAgent = "UserAgent";
const std::string kSDKUserAgent = "aliyun-tablestore-sdk-cpp98/4.2.1(amd64;)";

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

} // end of tablestore
} // end of aliyun

#endif
