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
#include "util/move.hpp"
#include "testa/testa.hpp"
#include <deque>
#include <string>

using namespace std;

namespace aliyun {
namespace tablestore {

namespace {

class A: public util::Moveable
{
    string mId;
    string* mLog;

public:
    explicit A(const string& id, string* log)
      : mId(id),
        mLog(log)
    {
        mLog->append("ctor:");
        mLog->append(mId);
        mLog->push_back(',');
    }

    explicit A(const util::MoveHolder<A>& ano) throw()
      : mId(ano->mId),
        mLog(ano->mLog)
    {
        mLog->append("move ctor:");
        mLog->append(mId);
        mLog->push_back(',');
        ano->mId.clear();
    }
    A& operator=(const util::MoveHolder<A>& ano) throw()
    {
        mLog->append("dtor:");
        mLog->append(mId);
        mLog->push_back(',');

        mId = ano->mId;
        mLog = ano->mLog;
        mLog->append("move assign:");
        mLog->append(mId);
        mLog->push_back(',');

        ano->mId.clear();

        return *this;
    }

    ~A()
    {
        mLog->append("dtor:");
        mLog->append(mId);
        mLog->push_back(',');
    }
};

} // namespace

void Move_Moveable()
{
    string log;
    {
        A a("a", &log);
        A b("b", &log);
        util::moveAssign(&a, util::move(b));
    }
    TESTA_ASSERT(log == "ctor:a,ctor:b,dtor:a,move assign:b,dtor:,dtor:b,")
        (log)
        .issue();
}
TESTA_DEF_JUNIT_LIKE1(Move_Moveable);

void Move_int()
{
    int a = 10;
    int b = 0;
    util::moveAssign(&b, util::move(a));
    TESTA_ASSERT(b == 10)
        (a)
        .issue();
}
TESTA_DEF_JUNIT_LIKE1(Move_int);

void Move_deque()
{
    deque<int> a;
    a.push_back(1);
    deque<int> b;
    util::moveAssign(&b, util::move(a));
    TESTA_ASSERT(pp::prettyPrint(a) == "[]" && pp::prettyPrint(b) == "[1]")
        (a)(b).issue();
}
TESTA_DEF_JUNIT_LIKE1(Move_deque);

} // namespace tablestore
} // namespace aliyun

