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
#include "common.hpp"
#include "tablestore/util/timestamp.hpp"
#include "tablestore/util/assert.hpp"
#include <cstdio>

using namespace std;

namespace aliyun {
namespace tablestore {
namespace core {

Channel::Channel()
  : mConsumeSem(0),
    mProduceSem(1)
{}

string Channel::consume()
{
    mConsumeSem.wait();
    string res = mMsg;
    mProduceSem.post();
    return res;
}

void Channel::produce(const string& msg)
{
    mProduceSem.wait();
    mMsg = msg;
    mConsumeSem.post();
}

MasterSlave::MasterSlave()
  : mToMaster(),
    mToSlave(),
    mMaster(mToMaster, mToSlave),
    mSlave(mToMaster, mToSlave)
{}

Master::Master(Channel& toMaster, Channel& toSlave)
  : mToMaster(toMaster),
    mToSlave(toSlave)
{}

string Master::listen()
{
    return mToMaster.consume();
}

void Master::answer(const string& msg)
{
    mToSlave.produce(msg);
}

Slave::Slave(Channel& toMaster, Channel& toSlave)
  : mToMaster(toMaster),
    mToSlave(toSlave)
{}

void Slave::ask(const string& q)
{
    mToMaster.produce(q);
    string a = mToSlave.consume();
    OTS_ASSERT(a == q)
        (q)
        (a);
}

void Slave::say(const string& q)
{
    mToMaster.produce(q);
}

} // namespace core
} // namespace tablestore
} // namespace aliyun

