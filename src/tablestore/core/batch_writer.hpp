#pragma once
#ifndef TABLESTORE_CORE_BATCH_WRITER_HPP
#define TABLESTORE_CORE_BATCH_WRITER_HPP
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
#include "tablestore/core/client.hpp"
#include "tablestore/core/types.hpp"
#include "tablestore/core/error.hpp"
#include "tablestore/util/threading.hpp"
#include "tablestore/util/optional.hpp"
#include "tablestore/util/timestamp.hpp"
#include <tr1/functional>

namespace aliyun {
namespace tablestore {
namespace core {

/**
 * BatchWriter is a useful tool to collects writes, periodically sends them in batches.
 * It aims at throughput rather than latency.
 * It will also deal with retrying in a heuristic way:
 * 1. In regular time, a BatchWriter sends at most maxConcurrency() 
 *    (called "concurrency") BatchWriteRowRequests with at most maxBatchSize() 
 *    rows every regularNap() period (called "nap").
 * 2. Once it detects backend busy from TableStore service, it will back off.
 *    While backing off, 
 *   2.1 in each round it shrinks half of concurrency until concurrency is down to 1;
 *   2.2 then in each round it doubles nap until nap is up to maxNap().
 * 3. If no busy is detected during a nap, it will recover from backing off.
 *    While recovering,
 *   3.1 it first shrink nap constantly by napShrinkStep() until it is down 
 *     to regularNap();
 *   3.2 then it constantly increases concurrency by 1 util it is up to maxConcurrency().
 */
class BatchWriterConfig
{
public:
    explicit BatchWriterConfig();

    void prettyPrint(std::string&) const;
    util::Optional<OTSError> validate() const;

    int64_t maxConcurrency() const
    {
        return mMaxConcurrency;
    }
    int64_t& mutableMaxConcurrency()
    {
        return mMaxConcurrency;
    }

    int64_t maxBatchSize() const
    {
        return mMaxBatchSize;
    }
    int64_t& mutableMaxBatchSize()
    {
        return mMaxBatchSize;
    }

    util::Duration regularNap() const
    {
        return mRegularNap;
    }
    util::Duration& mutableRegularNap()
    {
        return mRegularNap;
    }

    util::Duration maxNap() const
    {
        return mMaxNap;
    }
    util::Duration& mutableMaxNap()
    {
        return mMaxNap;
    }

    util::Duration napShrinkStep() const
    {
        return mNapShrinkStep;
    }
    util::Duration& mutableNapShrinkStep()
    {
        return mNapShrinkStep;
    }

    typedef std::tr1::shared_ptr<util::Actor> ActorPtr;
    /**
     * Actors where user callbacks run.
     * 32 newly-created threads by default.
     */
    const util::Optional<std::deque<ActorPtr> >& actors() const
    {
        return mActors;
    }
    util::Optional<std::deque<ActorPtr> >& mutableActors()
    {
        return mActors;
    }

private:
    int64_t mMaxConcurrency;
    int64_t mMaxBatchSize;
    util::Duration mRegularNap;
    util::Duration mMaxNap;
    util::Duration mNapShrinkStep;
    util::Optional<std::deque<ActorPtr> > mActors;
};

/**
 * 1. Any instance will launch an additional thread.
 * 2. Requests will be sent in any order.
 * 3. It assumes errors of both RETRIABLE and DEPENDS in RetryStrategy::RetryCategory 
 *    are retriable.
 * 4. The underlying TableStore client should do no retries at all.
 */
class SyncBatchWriter
{
public:
    virtual ~SyncBatchWriter() {}

    static util::Optional<OTSError> create(
        SyncBatchWriter*&,
        AsyncClient&,
        const BatchWriterConfig&);

    /**
     * Puts a row.
     * When the row already exists, it will be overwritten if the row condition
     * in the request is ignore or expect-exist.
     */
    virtual util::Optional<OTSError> putRow(
        PutRowResponse&, const PutRowRequest&) =0;

    /**
     * Updates a row.
     * It can be used either to modify an existent row or to insert a new row.
     */
    virtual util::Optional<OTSError> updateRow(
        UpdateRowResponse&, const UpdateRowRequest&) =0;

    /**
     * Deletes a row.
     */
    virtual util::Optional<OTSError> deleteRow(
        DeleteRowResponse&, const DeleteRowRequest&) =0;
};

/**
 * 1. Any instance will launch an additional thread.
 * 2. Requests will be sent in any order.
 * 3. It assumes errors of both RETRIABLE and DEPENDS in RetryStrategy::RetryCategory 
 *    are retriable.
 * 4. The underlying TableStore client should do no retries at all.
 */
class AsyncBatchWriter
{
public:
    virtual ~AsyncBatchWriter() {}

    static util::Optional<OTSError> create(
        AsyncBatchWriter*&,
        AsyncClient&,
        const BatchWriterConfig&);

    /**
     * Puts a row.
     * When the row already exists, it will be overwritten if the row condition
     * in the request is ignore or expect-exist.
     * Caveats:
     * - Content of the request will probably be changed.
     * - It is generally unwise to do blocking things in callback.
     */
    virtual void putRow(
        PutRowRequest&,
        const std::tr1::function<void(
            PutRowRequest&, util::Optional<OTSError>&, PutRowResponse&)>&) =0;

    /**
     * Updates a row.
     * It can be used either to modify an existent row or to insert a new row.
     *
     * Caveats:
     * - Content of the request will probably be changed.
     * - It is generally unwise to do blocking things in callback.
     */
    virtual void updateRow(
        UpdateRowRequest&,
        const std::tr1::function<void(
            UpdateRowRequest&, util::Optional<OTSError>&, UpdateRowResponse&)>&) =0;

    /**
     * Deletes a row.
     *
     * Caveats:
     * - Content of the request will probably be changed.
     * - It is generally unwise to do blocking things in callback.
     */
    virtual void deleteRow(
        DeleteRowRequest&,
        const std::tr1::function<void(
            DeleteRowRequest&, util::Optional<OTSError>&, DeleteRowResponse&)>&) =0;
};

} // namespace core
} // namespace tablestore
} // namespace aliyun

#endif
