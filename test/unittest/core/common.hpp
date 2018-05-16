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
#include "gmock/gmock.h"
#include "tablestore/core/client.hpp"
#include "tablestore/util/threading.hpp"
#include <tr1/functional>
#include <string>
#include <stdint.h>

namespace aliyun {
namespace tablestore {
namespace core {

void initGmock();

class MockAsyncClient : public AsyncClient {
public:
    MOCK_METHOD2(createTable, void(
        CreateTableRequest&,
        const std::tr1::function<void(
                CreateTableRequest&, util::Optional<OTSError>&, CreateTableResponse&)>&));
    MOCK_METHOD2(deleteTable, void(
        DeleteTableRequest&,
        const std::tr1::function<void(
                DeleteTableRequest&, util::Optional<OTSError>&, DeleteTableResponse&)>&));
    MOCK_METHOD2(listTable, void(
        ListTableRequest&,
        const std::tr1::function<void(
                ListTableRequest&, util::Optional<OTSError>&, ListTableResponse&)>&));
    MOCK_METHOD2(describeTable, void(
        DescribeTableRequest&,
        const std::tr1::function<void(
                DescribeTableRequest&, util::Optional<OTSError>&, DescribeTableResponse&)>&));
    MOCK_METHOD2(updateTable, void(
        UpdateTableRequest&,
        const std::tr1::function<void(
                UpdateTableRequest&, util::Optional<OTSError>&, UpdateTableResponse&)>&));
    MOCK_METHOD2(putRow, void(
        PutRowRequest&,
        const std::tr1::function<void(
                PutRowRequest&, util::Optional<OTSError>&, PutRowResponse&)>&));
    MOCK_METHOD2(updateRow, void(
        UpdateRowRequest&,
        const std::tr1::function<void(
                UpdateRowRequest&, util::Optional<OTSError>&, UpdateRowResponse&)>&));
    MOCK_METHOD2(deleteRow, void(
        DeleteRowRequest&,
        const std::tr1::function<void(
                DeleteRowRequest&, util::Optional<OTSError>&, DeleteRowResponse&)>&));
    MOCK_METHOD2(batchWriteRow, void(
        BatchWriteRowRequest&,
        const std::tr1::function<void(
                BatchWriteRowRequest&, util::Optional<OTSError>&, BatchWriteRowResponse&)>&));
    MOCK_METHOD2(getRow, void(
        GetRowRequest&,
        const std::tr1::function<void(
                GetRowRequest&, util::Optional<OTSError>&, GetRowResponse&)>&));
    MOCK_METHOD2(batchGetRow, void(
        BatchGetRowRequest&,
        const std::tr1::function<void(
                BatchGetRowRequest&, util::Optional<OTSError>&, BatchGetRowResponse&)>&));
    MOCK_METHOD2(getRange, void(
        GetRangeRequest&,
        const std::tr1::function<void(
                GetRangeRequest&, util::Optional<OTSError>&, GetRangeResponse&)>&));
    MOCK_METHOD2(computeSplitsBySize, void(
        ComputeSplitsBySizeRequest&,
        const std::tr1::function<void(
                ComputeSplitsBySizeRequest&, util::Optional<OTSError>&, ComputeSplitsBySizeResponse&)>&));
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
