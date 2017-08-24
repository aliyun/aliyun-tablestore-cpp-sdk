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
#include <string>

namespace aliyun {
namespace tablestore {
namespace core {
namespace impl {

// request
extern const std::string kOTSHeaderPrefix;

extern const std::string kAPIVersion;
extern const std::string kOTSContentMD5;
extern const std::string kOTSDate;
extern const std::string kOTSAPIVersion;
extern const std::string kOTSAccessKeyId;
extern const std::string kOTSStsToken;
extern const std::string kOTSInstanceName;
extern const std::string kOTSSignature;
extern const std::string kOTSTraceId;
extern const std::string kHttpContentType;
extern const std::string kHttpContentLength;
extern const std::string kHttpAccept;
extern const std::string kMimeType;
extern const std::string kUserAgent;
extern const std::string kSDKUserAgent;

// response
extern const std::string kOTSRequestId;
extern const std::string kOTSAuthorization;
extern const std::string kOTSTraceInfo;
extern const std::string kOTSContentType;

} // namespace impl
} // namespace core
} // namespace tablestore
} // namespace aliyun

extern "C" {
extern char const * const kTableStoreBuildInfo; // let gdb happy
}

