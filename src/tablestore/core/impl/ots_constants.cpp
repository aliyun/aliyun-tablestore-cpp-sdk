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
#include "ots_constants.hpp"

using namespace std;

namespace aliyun {
namespace tablestore {
namespace core {
namespace impl {

const string kOTSHeaderPrefix("x-ots-");
const string kAPIVersion("2015-12-31");
const string kOTSContentMD5("x-ots-contentmd5");
const string kOTSDate("x-ots-date");
const string kOTSAPIVersion("x-ots-apiversion");
const string kOTSAccessKeyId("x-ots-accesskeyid");
const string kOTSStsToken("x-ots-ststoken");
const string kOTSInstanceName("x-ots-instancename");
const string kOTSSignature("x-ots-signature");
const string kOTSTraceId("x-ots-sdk-traceid");
const string kHttpContentType("Content-Type");
const string kHttpContentLength("Content-Length");
const string kHttpAccept("Accept");
const string kMimeType("application/x.pb2");
const string kUserAgent("User-Agent");

const string kOTSRequestId("x-ots-requestid");
const string kOTSAuthorization("Authorization");
const string kOTSTraceInfo("x-ots-traceinfo");
const string kOTSContentType("x-ots-contenttype");

} // namespace impl
} // namespace core
} // namespace tablestore
} // namespace aliyun
