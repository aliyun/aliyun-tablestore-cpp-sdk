#pragma once
#ifndef UNITTEST_CORE_COMMON_HPP
#define UNITTEST_CORE_COMMON_HPP
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
#include "tablestore/util/threading.hpp"
#include <tr1/functional>
#include <tr1/memory>
#include <string>
#include <stdint.h>

namespace aliyun {
namespace tablestore {
namespace core {

class MockAsyncClient: public AsyncClient
{
private:
    util::Logger& mLogger;
    std::deque<std::tr1::shared_ptr<util::Actor> > mActor;
    std::tr1::shared_ptr<RetryStrategy> mRetryStrategy;

public:
    explicit MockAsyncClient(util::Logger&);
    ~MockAsyncClient();

    util::Logger& mutableLogger();
    const std::deque<std::tr1::shared_ptr<util::Actor> >& actors() const;
    const RetryStrategy& retryStrategy() const;
    std::tr1::shared_ptr<RetryStrategy>& mutableRetryStrategy();

#define DEF_ACTION(api, upcase) \
    private:\
    typedef std::tr1::function<void(\
        upcase##Request&, util::Optional<OTSError>&, upcase##Response&)> upcase##Callback;\
    typedef std::tr1::function<void(upcase##Request&, const upcase##Callback&)> upcase##Action;\
    upcase##Action m##upcase##Action;\
    public:\
    upcase##Action& mutable##upcase() {return m##upcase##Action;}\
    void api(upcase##Request& req, const upcase##Callback& cb)\
    {\
        OTS_ASSERT(m##upcase##Action);\
        mActor[0]->pushBack(bind(m##upcase##Action, req, cb));\
    }

    DEF_ACTION(createTable, CreateTable);
    DEF_ACTION(deleteTable, DeleteTable);
    DEF_ACTION(listTable, ListTable);
    DEF_ACTION(describeTable, DescribeTable);
    DEF_ACTION(updateTable, UpdateTable);
    DEF_ACTION(putRow, PutRow);
    DEF_ACTION(updateRow, UpdateRow);
    DEF_ACTION(deleteRow, DeleteRow);
    DEF_ACTION(batchWriteRow, BatchWriteRow);
    DEF_ACTION(getRow, GetRow);
    DEF_ACTION(batchGetRow, BatchGetRow);
    DEF_ACTION(getRange, GetRange);
    DEF_ACTION(computeSplitsBySize, ComputeSplitsBySize);

#undef DEF_ACTION
};

class Channel
{
public:
    explicit Channel();

    std::string consume();
    void produce(const std::string&);

private:
    util::Semaphore mConsumeSem;
    util::Semaphore mProduceSem;
    std::string mMsg;
};

class Slave
{
public:
    explicit Slave(Channel& toMaster, Channel& toSlave);

    void ask(const std::string&);
    void say(const std::string&);

private:
    Channel& mToMaster;
    Channel& mToSlave;
};

class Master
{
public:
    explicit Master(Channel& toMaster, Channel& toSlave);

    std::string listen();
    void answer(const std::string&);

private:
    Channel& mToMaster;
    Channel& mToSlave;
};

class MasterSlave
{
public:
    explicit MasterSlave();

    Master& master()
    {
        return mMaster;
    }

    Slave& slave()
    {
        return mSlave;
    }

private:
    Channel mToMaster;
    Channel mToSlave;
    Master mMaster;
    Slave mSlave;
};

} // namespace core
} // namespace tablestore
} // namespace aliyun
#endif
