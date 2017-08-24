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
#ifndef TABLESTORE_UTIL_LOGGING_HPP
#define TABLESTORE_UTIL_LOGGING_HPP

#include "logging.ipp"

#define OTS_LOG_PINGPONG_A(key, val) OTS_LOG_PINGPONG_OP(key, val, B)
#define OTS_LOG_PINGPONG_B(key, val) OTS_LOG_PINGPONG_OP(key, val, A)
#define OTS_LOG_PINGPONG_OP(key, val, next)                     \
    append(key, pp::prettyPrint(val)). OTS_LOG_PINGPONG_##next

#define OTS_LOG(logger, lvl) \
    if ((logger).level() > (lvl)) {} \
    else aliyun::tablestore::util::impl:: \
             LogHelper((lvl), logger, __FILE__, __LINE__, __func__). OTS_LOG_PINGPONG_A

#define OTS_LOG_DEBUG(logger) \
    OTS_LOG(logger, aliyun::tablestore::util::Logger::kDebug)

#define OTS_LOG_INFO(logger) \
    OTS_LOG(logger, aliyun::tablestore::util::Logger::kInfo)

#define OTS_LOG_ERROR(logger) \
    OTS_LOG(logger, aliyun::tablestore::util::Logger::kError)

#endif /* TABLESTORE_UTIL_LOGGING_HPP */
