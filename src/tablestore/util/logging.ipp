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
#include "tablestore/util/logger.hpp"
#include "tablestore/util/prettyprint.hpp"
#include <deque>
#include <string>
#include <utility>

namespace aliyun {
namespace tablestore {
namespace util {
namespace impl {

class LogHelper
{
public:
    LogHelper(
        Logger::LogLevel lvl,
        Logger& logger,
        const char* fn,
        int line,
        const char* func)
      : mLevel(lvl),
        mLogger(logger),
        mFile(fn),
        mLine(pp::prettyPrint(line)),
        mFunc(func),
        OTS_LOG_PINGPONG_A(*this),
        OTS_LOG_PINGPONG_B(*this)
    {}
    ~LogHelper();

    LogHelper& append(const char* msg, const std::string& x)
    {
        mValues.push_back(std::make_pair(std::string(msg), x));
        return *this;
    }

    void what(const std::string& msg)
    {
        mWhat = msg;
    }

private:
    Logger::LogLevel mLevel;
    Logger& mLogger;
    std::deque< std::pair< std::string, std::string> > mValues;
    std::string mWhat;
    std::string mFile;
    std::string mLine;
    std::string mFunc;

public:
    /**
     * Ender of macro expansion
     */
    LogHelper& OTS_LOG_PINGPONG_A;
    LogHelper& OTS_LOG_PINGPONG_B;
};

} // namespace impl
} // namespace util
} // namespace tablestore
} // namespace aliyun
