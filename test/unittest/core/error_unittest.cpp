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
#include "util/prettyprint.hpp"
#include "testa/testa.hpp"
#include <string>

using namespace std;

namespace aliyun {
namespace tablestore {

void Error_complete(const string&)
{
    core::Error err(400, "ParameterInvalid", "xxx", "trace", "request");
    TESTA_ASSERT(pp::prettyPrint(err) == "{\"HttpStatus\": 400, \"ErrorCode\": \"ParameterInvalid\", \"Message\": \"xxx\", \"RequestId\": \"request\", \"TraceId\": \"trace\"}")
        (err)
        .issue();
}
TESTA_DEF_JUNIT_LIKE1(Error_complete);

void Error_no_traceid(const string&)
{
    core::Error err(400, "ParameterInvalid", "xxx", "trace");
    TESTA_ASSERT(pp::prettyPrint(err) == "{\"HttpStatus\": 400, \"ErrorCode\": \"ParameterInvalid\", \"Message\": \"xxx\", \"TraceId\": \"trace\"}")
        (err)
        .issue();
}
TESTA_DEF_JUNIT_LIKE1(Error_no_traceid);

void Error_no_requestid_traceid(const string&)
{
    core::Error err(400, "ParameterInvalid", "xxx");
    TESTA_ASSERT(pp::prettyPrint(err) == "{\"HttpStatus\": 400, \"ErrorCode\": \"ParameterInvalid\", \"Message\": \"xxx\"}")
        (err)
        .issue();
}
TESTA_DEF_JUNIT_LIKE1(Error_no_requestid_traceid);

} // namespace tablestore
} // namespace aliyun

