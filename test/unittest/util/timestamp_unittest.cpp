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
#include "tablestore/util/timestamp.hpp"
#include "testa/testa.hpp"
#include <deque>
#include <string>

using namespace std;

namespace aliyun {
namespace tablestore {

void Duration_cons(const string&)
{
    TESTA_ASSERT(util::Duration::fromHour(1).toUsec() == 3600 * util::kUsecPerSec)
        (util::Duration::fromHour(1).toUsec())
        .issue();
    TESTA_ASSERT(util::Duration::fromMin(1).toUsec() == 60 * util::kUsecPerSec)
        (util::Duration::fromMin(1).toUsec())
        .issue();
    TESTA_ASSERT(util::Duration::fromSec(1).toUsec() == util::kUsecPerSec)
        (util::Duration::fromSec(1).toUsec())
        .issue();
    TESTA_ASSERT(util::Duration::fromMsec(1).toUsec() == util::kUsecPerMsec)
        (util::Duration::fromMsec(1).toUsec())
        .issue();
    TESTA_ASSERT(util::Duration::fromUsec(1).toUsec() == 1)
        (util::Duration::fromUsec(1).toUsec())
        .issue();
    TESTA_ASSERT(util::Duration::fromHour(1.0).toUsec() == 3600 * util::kUsecPerSec)
        (util::Duration::fromHour(1.0).toUsec())
        .issue();
    TESTA_ASSERT(util::Duration::fromMin(1.0).toUsec() == 60 * util::kUsecPerSec)
        (util::Duration::fromMin(1.0).toUsec())
        .issue();
    TESTA_ASSERT(util::Duration::fromSec(1.0).toUsec() == util::kUsecPerSec)
        (util::Duration::fromSec(1.0).toUsec())
        .issue();
    TESTA_ASSERT(util::Duration::fromMsec(1.0).toUsec() == util::kUsecPerMsec)
        (util::Duration::fromMsec(1.0).toUsec())
        .issue();
    TESTA_ASSERT(util::Duration::fromUsec(1.0).toUsec() == 1)
        (util::Duration::fromUsec(1.0).toUsec())
        .issue();
}
TESTA_DEF_JUNIT_LIKE1(Duration_cons);

void Duration_assignment(const string&)
{
    util::Duration a = util::Duration::fromUsec(1);
    util::Duration b = util::Duration::fromUsec(2);
    util::Duration c(a);
    TESTA_ASSERT(a == c)
        (a)(b).issue();
    c = b;
    TESTA_ASSERT(b == c)
        (b)(c).issue();
}
TESTA_DEF_JUNIT_LIKE1(Duration_assignment);

void Duration_plus(const string&)
{
    util::Duration a = util::Duration::fromUsec(1);
    util::Duration b = util::Duration::fromUsec(2);
    util::Duration c = a + b;
    TESTA_ASSERT(c.toUsec() == 3)
        (c).issue();
    b += a;
    TESTA_ASSERT(b.toUsec() == 3)
        (b).issue();
}
TESTA_DEF_JUNIT_LIKE1(Duration_plus);

void Duration_minus(const string&)
{
    util::Duration a = util::Duration::fromUsec(1);
    util::Duration b = util::Duration::fromUsec(2);
    util::Duration c = a - b;
    TESTA_ASSERT(c.toUsec() == -1)
        (c).issue();
    a -= b;
    TESTA_ASSERT(a.toUsec() == -1)
        (a).issue();
}
TESTA_DEF_JUNIT_LIKE1(Duration_minus);

void Duration_negate(const string&)
{
    util::Duration a = util::Duration::fromUsec(1);
    util::Duration b = -a;
    TESTA_ASSERT(b.toUsec() == -1)
        (b).issue();
}
TESTA_DEF_JUNIT_LIKE1(Duration_negate);

void Duration_toint(const string&)
{
    util::Duration a = util::Duration::fromUsec(1002003);
    TESTA_ASSERT(a.toUsec() == 1002003)
        (a).issue();
    TESTA_ASSERT(a.toMsec() == 1002)
        (a).issue();
    TESTA_ASSERT(a.toSec() == 1)
        (a).issue();
}
TESTA_DEF_JUNIT_LIKE1(Duration_toint);

void Duration_multiply(const string&)
{
    util::Duration a = util::Duration::fromUsec(1);
    util::Duration b = a * 3;
    TESTA_ASSERT(b.toUsec() == 3)
        (b).issue();
    util::Duration c = 3 * a;
    TESTA_ASSERT(c.toUsec() == 3)
        (c).issue();
    a *= 3;
    TESTA_ASSERT(a.toUsec() == 3)
        (a).issue();
}
TESTA_DEF_JUNIT_LIKE1(Duration_multiply);


void MonotonicTime_assignment(const string&)
{
    util::MonotonicTime a(1);
    util::MonotonicTime b(2);
    util::MonotonicTime c(a);
    TESTA_ASSERT(a == c)
        (a)(c).issue();
    c = b;
    TESTA_ASSERT(b == c)
        (b)(c).issue();
}
TESTA_DEF_JUNIT_LIKE1(MonotonicTime_assignment);

void MonotonicTime_distance(const string&)
{
    util::MonotonicTime a(2);
    util::MonotonicTime b(1);
    util::Duration i = a - b;
    TESTA_ASSERT(i.toUsec() == 1)
        (a)(b)(i).issue();
}
TESTA_DEF_JUNIT_LIKE1(MonotonicTime_distance);

void MonotonicTime_plus(const string&)
{
    util::MonotonicTime a(1);
    util::Duration inc = util::Duration::fromUsec(2);
    util::MonotonicTime b = a + inc;
    TESTA_ASSERT(b - a == inc)
        (a)(b)(inc).issue();
    util::MonotonicTime c = inc + a;
    TESTA_ASSERT(c == b)
        (a)(b)(c)(inc).issue();
    a += inc;
    TESTA_ASSERT(a == b)
        (a)(b)(inc).issue();
}
TESTA_DEF_JUNIT_LIKE1(MonotonicTime_plus);

void MonotonicTime_ordering(const string&)
{
    util::MonotonicTime a(1);
    util::MonotonicTime b(2);
    TESTA_ASSERT(a == a)
        (a).issue();
    TESTA_ASSERT(a != b)
        (a)(b).issue();
    TESTA_ASSERT(a < b)
        (a)(b).issue();
    TESTA_ASSERT(a <= b)
        (a)(b).issue();
    TESTA_ASSERT(b > a)
        (a)(b).issue();
    TESTA_ASSERT(b >= a)
        (a)(b).issue();
}
TESTA_DEF_JUNIT_LIKE1(MonotonicTime_ordering);

namespace {

template<typename X, typename Y, typename Z>
void CheckRound(X a, Y b, Z diff)
{
    TESTA_ASSERT(b - diff < a)(a)(b)(diff).issue();
    TESTA_ASSERT(a < b + diff)(a)(b)(diff).issue();
}

} // namespace

void MonotonicTime_now(const string&)
{
    util::MonotonicTime a = util::MonotonicTime::now();
    util::sleepFor(util::Duration::fromSec(1));
    util::MonotonicTime b = util::MonotonicTime::now();
    util::Duration d = b - a;
    CheckRound(d, util::Duration::fromSec(1),
        util::Duration::fromMsec(20));
}
TESTA_DEF_JUNIT_LIKE1(MonotonicTime_now);


void UtcTime_from_timeval(const string&)
{
    timeval tv;
    tv.tv_sec = 1;
    tv.tv_usec = 1;
    util::UtcTime tm = util::UtcTime::fromTimeval(tv);
    TESTA_ASSERT(tm.toUsec() == util::kUsecPerSec + 1)
        (tm).issue();
}
TESTA_DEF_JUNIT_LIKE1(UtcTime_from_timeval);

void UtcTime_ordering(const string&)
{
    util::UtcTime a = util::UtcTime::fromUsec(1);
    util::UtcTime b = util::UtcTime::fromUsec(2);
    TESTA_ASSERT(a == a)
        (a).issue();
    TESTA_ASSERT(a != b)
        (a)(b).issue();
    TESTA_ASSERT(a < b)
        (a)(b).issue();
    TESTA_ASSERT(a <= b)
        (a)(b).issue();
    TESTA_ASSERT(b > a)
        (a)(b).issue();
    TESTA_ASSERT(b >= a)
        (a)(b).issue();
}
TESTA_DEF_JUNIT_LIKE1(UtcTime_ordering);

void UtcTime_distance(const string&)
{
    util::UtcTime a = util::UtcTime::fromUsec(2);
    util::UtcTime b = util::UtcTime::fromUsec(1);
    util::Duration i = a - b;
    TESTA_ASSERT(i.toUsec() == 1)
        (a)(b).issue();
}
TESTA_DEF_JUNIT_LIKE1(UtcTime_distance);

void UtcTime_plus(const string&)
{
    util::UtcTime a = util::UtcTime::fromUsec(1);
    util::Duration inc = util::Duration::fromUsec(2);
    util::UtcTime b = a + inc;
    TESTA_ASSERT(b.toUsec() == 3)
        (a)(b)(inc).issue();
    util::UtcTime c = inc + a;
    TESTA_ASSERT(c.toUsec() == 3)
        (a)(c).issue();
    a += inc;
    TESTA_ASSERT(a.toUsec() == 3)
        (a)(inc).issue();
}
TESTA_DEF_JUNIT_LIKE1(UtcTime_plus);

void UtcTime_iso8601(const string&)
{
    util::UtcTime x = util::UtcTime::fromUsec(0);
    TESTA_ASSERT(x.toIso8601() == "1970-01-01T00:00:00.000000Z")
        (x).issue();
}
TESTA_DEF_JUNIT_LIKE1(UtcTime_iso8601);

} // namespace tablestore
} // namespace aliyun
