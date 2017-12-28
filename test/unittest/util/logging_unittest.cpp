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
#include "tablestore/util/logging.hpp"
#include "testa/testa.hpp"
#include <memory>

using namespace std;

namespace aliyun {
namespace tablestore {

void Logging(const string&)
{
    auto_ptr<util::Logger> logger(
        util::createLogger("/test-logging", util::Logger::kInfo));
    string hit = "hit";
    string miss = "miss";
    {
        auto_ptr<util::Logger> info(logger->spawn("/info"));
        OTS_LOG_DEBUG(*info)("NotThere", miss);
        OTS_LOG_INFO(*info)("BeThere", hit);
    }
    {
        auto_ptr<util::Logger> debug(logger->spawn("/debug"));
        OTS_LOG_DEBUG(*debug)("NotThere", miss);
        OTS_LOG_INFO(*debug)("BeThere", hit);
    }
    {
        auto_ptr<util::Logger> error(logger->spawn("/error", util::Logger::kError));
        OTS_LOG_INFO(*error)("NotThere", miss);
        OTS_LOG_ERROR(*error)("BeThere", hit);
    }
    util::SinkerCenter::singleton()->flushAll();
    TESTA_ASSERT(true).issue(); // this result should be checked manually.
}
TESTA_DEF_JUNIT_LIKE1(Logging);

} // namespace tablestore
} // namespace aliyun
