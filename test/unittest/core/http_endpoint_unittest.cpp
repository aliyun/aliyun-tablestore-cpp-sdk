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
#include "src/tablestore/core/http/types.hpp"
#include "tablestore/util/prettyprint.hpp"
#include "testa/testa.hpp"
#include <string>

using namespace std;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {

namespace {
void HttpEndpoint_ParseUrl(const string&)
{
    {
        http::Endpoint ep;
        Optional<string> err = http::Endpoint::parse(ep, "http://ots.aliyun.com");
        TESTA_ASSERT(!err.present())
            (*err).issue();
        TESTA_ASSERT(ep.mProtocol == http::Endpoint::HTTP)
            (ep).issue();
        TESTA_ASSERT(ep.mHost == "ots.aliyun.com")
            (ep).issue();
        TESTA_ASSERT(ep.mPort == "80")
            (ep).issue();
    }
    {
        http::Endpoint ep;
        Optional<string> err = http::Endpoint::parse(ep, "http://ots.aliyun.com/");
        TESTA_ASSERT(!err.present())
            (*err).issue();
        TESTA_ASSERT(ep.mProtocol == http::Endpoint::HTTP)
            (ep).issue();
        TESTA_ASSERT(ep.mHost == "ots.aliyun.com")
            (ep).issue();
        TESTA_ASSERT(ep.mPort == "80")
            (ep).issue();
    }
    {
        http::Endpoint ep;
        Optional<string> err = http::Endpoint::parse(ep, "http://ots.aliyun.com:8888");
        TESTA_ASSERT(!err.present())
            (*err).issue();
        TESTA_ASSERT(ep.mProtocol == http::Endpoint::HTTP)
            (ep).issue();
        TESTA_ASSERT(ep.mHost == "ots.aliyun.com")
            (ep).issue();
        TESTA_ASSERT(ep.mPort == "8888")
            (ep).issue();
    }
    {
        http::Endpoint ep;
        Optional<string> err = http::Endpoint::parse(ep, "http://ots.aliyun.com:8888/");
        TESTA_ASSERT(!err.present())
            (*err).issue();
        TESTA_ASSERT(ep.mProtocol == http::Endpoint::HTTP)
            (ep).issue();
        TESTA_ASSERT(ep.mHost == "ots.aliyun.com")
            (ep).issue();
        TESTA_ASSERT(ep.mPort == "8888")
            (ep).issue();
    }
    {
        http::Endpoint ep;
        Optional<string> err = http::Endpoint::parse(ep, "https://ots.aliyun.com");
        TESTA_ASSERT(!err.present())
            (*err).issue();
        TESTA_ASSERT(ep.mProtocol == http::Endpoint::HTTPS)
            (ep).issue();
        TESTA_ASSERT(ep.mHost == "ots.aliyun.com")
            (ep).issue();
        TESTA_ASSERT(ep.mPort == "443")
            (ep).issue();
    }
    {
        http::Endpoint ep;
        Optional<string> err = http::Endpoint::parse(ep, "http://1.2.3.4");
        TESTA_ASSERT(!err.present())
            (*err).issue();
        TESTA_ASSERT(ep.mProtocol == http::Endpoint::HTTP)
            (ep).issue();
        TESTA_ASSERT(ep.mHost == "1.2.3.4")
            (ep).issue();
        TESTA_ASSERT(ep.mPort == "80")
            (ep).issue();
    }
    {
        http::Endpoint ep;
        Optional<string> err = http::Endpoint::parse(ep, "http://1.2.3.4/");
        TESTA_ASSERT(!err.present())
            (*err).issue();
        TESTA_ASSERT(ep.mProtocol == http::Endpoint::HTTP)
            (ep).issue();
        TESTA_ASSERT(ep.mHost == "1.2.3.4")
            (ep).issue();
        TESTA_ASSERT(ep.mPort == "80")
            (ep).issue();
    }
    {
        http::Endpoint ep;
        Optional<string> err = http::Endpoint::parse(ep, "http://1.2.3.4:8888");
        TESTA_ASSERT(!err.present())
            (*err).issue();
        TESTA_ASSERT(ep.mProtocol == http::Endpoint::HTTP)
            (ep).issue();
        TESTA_ASSERT(ep.mHost == "1.2.3.4")
            (ep).issue();
        TESTA_ASSERT(ep.mPort == "8888")
            (ep).issue();
    }
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(HttpEndpoint_ParseUrl);

} // namespace core
} // namespace tablestore
} // namespace aliyun

