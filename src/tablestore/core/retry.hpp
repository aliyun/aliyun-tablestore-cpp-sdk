#pragma once
#ifndef TABLESTORE_CORE_RETRY_HPP
#define TABLESTORE_CORE_RETRY_HPP
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
#include "tablestore/core/types.hpp"
#include "tablestore/util/timestamp.hpp"
#include <tr1/memory>
#include <string>
#include <stdint.h>

namespace aliyun {
namespace tablestore {
namespace util {
namespace random {
class Random;
} // namespace random
} // namespace util

namespace core {

class OTSError;

class RetryStrategy
{
public:
    enum RetryCategory
    {
        UNRETRIABLE,
        RETRIABLE,
        DEPENDS,
    };

    static RetryCategory retriable(const OTSError&);
    static bool retriable(Action, const OTSError&);

    virtual ~RetryStrategy() {}

    virtual RetryStrategy* clone() const =0;
    virtual int64_t retries() const throw() =0;
    virtual bool shouldRetry(Action, const OTSError&) const =0;
    virtual util::Duration nextPause() =0;
};

class DeadlineRetryStrategy: public RetryStrategy
{
public:
    explicit DeadlineRetryStrategy(
        const std::tr1::shared_ptr<util::random::Random>&,
        util::Duration timeout);

    DeadlineRetryStrategy* clone() const;
    int64_t retries() const throw();
    bool shouldRetry(Action, const OTSError&) const;
    util::Duration nextPause();

private:
    static const util::Duration kMaxPauseBase;

    std::tr1::shared_ptr<util::random::Random> mRandom;
    util::Duration mTimeout;

    // reset for cloned ones
    util::Duration mPauseBase;
    int64_t mRetries;
    util::MonotonicTime mDeadline;
};

class CountingRetryStrategy: public RetryStrategy
{
public:
    explicit CountingRetryStrategy(
        const std::tr1::shared_ptr<util::random::Random>&,
        int64_t n,
        util::Duration interval);

    CountingRetryStrategy* clone() const;
    int64_t retries() const throw();
    bool shouldRetry(Action, const OTSError&) const;
    util::Duration nextPause();

private:
    std::tr1::shared_ptr<util::random::Random> mRandom;
    util::Duration mInterval;
    const int64_t mMaxRetries;

    // reset for cloned ones
    int64_t mRetries;
};

class NoRetry: public RetryStrategy
{
public:
    explicit NoRetry();

    NoRetry* clone() const;
    int64_t retries() const throw();
    bool shouldRetry(Action, const OTSError&) const;
    util::Duration nextPause();
};

} // namespace core
} // namespace tablestore
} // namespace aliyun

#endif
