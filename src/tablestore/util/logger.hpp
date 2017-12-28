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
#include <tr1/memory>
#include <memory>
#include <string>

namespace aliyun {
namespace tablestore {
namespace util {

class Record
{
public:
    virtual ~Record() {}
};

class Sinker
{
public:
    virtual ~Sinker() {}
    /**
     * Sinks a record, and transfers the ownership of this record as well.
     * It is thread-safe.
     */
    virtual void sink(Record*) =0;

    /**
     * Flushs a sinker.
     * When this function returns, the flushing must be finished.
     * It is thread-safe.
     */
    virtual void flush() =0;
};

class Logger
{
public:
    enum LogLevel
    {
        kDebug,
        kInfo,
        kError,
    };
    
    virtual ~Logger() {}

    virtual LogLevel level() const =0;

    /**
     * Record a piece of log
     */
    virtual void record(LogLevel, const std::string&) =0;

    /**
     * Spawn a Logger, which is owned by its root.
     */
    virtual Logger* spawn(const std::string& key) =0;

    /**
     * Spawn a Logger with specified logging level, which is owned by the user.
     * This operation is generally thread-unsafe.
     */
    virtual Logger* spawn(const std::string& key, LogLevel) =0;
};

class SinkerCenter
{
public:
    virtual ~SinkerCenter() {}

    /**
     * Fetchs the singleton of SinkerCenter.
     */
    static std::tr1::shared_ptr<SinkerCenter> singleton();

    /**
     * Registers a sinker into this sinker center, thread-safely.
     *
     * If @p key does not associate with any sinker, it returns NULL.
     * And the sinker will be assoicated with it, with ownership transfered.
     * 
     * If there is already a sinker already associated with @p key, 
     * it will be returned without transferring the ownership.
     * Meanwhile, the ownership of the sinker in paramater is not transfered.
     */
    virtual Sinker* registerSinker(const std::string& key, Sinker*) =0;

    /**
     * Flushs all registered sinkers, thread-safely.
     */
    virtual void flushAll() =0;
};

/**
 * Creates a logger backboned by a thread which periodically dumps records to 
 * stderr.
 * It is discouraged to use this logger in production.
 */
Logger* createLogger(const std::string& loggerKey, Logger::LogLevel);

} // namespace util
} // namespace tablestore
} // namespace aliyun
