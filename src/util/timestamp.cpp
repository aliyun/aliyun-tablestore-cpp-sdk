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
#include "util/timestamp.hpp"
#include "util/prettyprint.hpp"
#include <deque>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <ctime>
extern "C" {
#include <sys/time.h>
}

using namespace std;

namespace aliyun {
namespace tablestore {
namespace util {

void sleepFor(const Duration& d)
{
    sleepUntil(MonotonicTime::now() + d);
}

void sleepUntil(const MonotonicTime& target)
{
    int64_t tm = target.toUsec();
    if (tm > 0) {
        timespec ts;
        ts.tv_sec = tm / kUsecPerSec;
        ts.tv_nsec = tm % kUsecPerSec * kNsecPerUsec;
        while (true) {
            int r = clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &ts, NULL);
            if (r == 0) {
                break;
            } else if (r == EINTR) {
                // interrupted by a signal handler. do nothing but retry.
            } else {
                int e = errno;
                OTS_ASSERT(false)(r)(e)(string(strerror(e)));
            }
        }
    }
}

namespace {

void fill(string* out, int64_t val, int64_t width)
{
    deque<char> digits;
    int64_t i = 0;
    for(; val > 0; ++i, val /= 10) {
        digits.push_back('0' + (val % 10));
    }
    for(; i < width; ++i) {
        digits.push_back('0');
    }

    for(; !digits.empty(); digits.pop_back()) {
        out->push_back(digits.back());
    }
}

} // namespace

void Duration::prettyPrint(string* out) const
{
    pp::prettyPrint(out, mValue / kUsecPerHour);
    out->push_back(':');
    fill(out, (mValue % kUsecPerHour) / kUsecPerMin, 2);
    out->push_back(':');
    fill(out, (mValue % kUsecPerMin) / kUsecPerSec, 2);
    out->push_back('.');
    fill(out, mValue % kUsecPerSec, 6);
}

void MonotonicTime::prettyPrint(string* out) const
{
    pp::prettyPrint(out, mValue / kUsecPerHour);
    out->push_back(':');
    fill(out, (mValue % kUsecPerHour) / kUsecPerMin, 2);
    out->push_back(':');
    fill(out, (mValue % kUsecPerMin) / kUsecPerSec, 2);
    out->push_back('.');
    fill(out, mValue % kUsecPerSec, 6);
}

MonotonicTime MonotonicTime::now()
{
    timespec tm;
    int r = clock_gettime(CLOCK_MONOTONIC, &tm);
    OTS_ASSERT(r == 0)(r)(string(strerror(errno)));
    int64_t usec = tm.tv_sec * kUsecPerSec + tm.tv_nsec / kNsecPerUsec;
    return MonotonicTime(usec);
}


namespace {

struct TimeComponent
{
    TimeComponent()
      : mYear(0), mMonth(0), mDay(0), mHour(0),
        mMinute(0), mSec(0), mUsec(0)
    {}

    int mYear;
    int mMonth;
    int mDay;
    int mHour;
    int mMinute;
    int mSec;
    int mUsec;
};

TimeComponent decompose(const UtcTime& tm)
{
    TimeComponent result;
    int64_t t = tm.toUsec();
    int64_t q = t / kUsecPerSec;
    result.mUsec = t % kUsecPerSec;
    time_t x = q;
    struct tm ti;
    struct tm* r = gmtime_r(&x, &ti);
    OTS_ASSERT(r != NULL).what("time out of range");
    result.mYear = ti.tm_year + 1900;
    result.mMonth = ti.tm_mon + 1;
    result.mDay = ti.tm_mday;
    result.mHour = ti.tm_hour;
    result.mMinute = ti.tm_min;
    result.mSec = ti.tm_sec;
    return result;
}

} // namespace

void UtcTime::toIso8601(string* out) const
{
    const TimeComponent& tc = decompose(*this);
    fill(out, tc.mYear, 4);
    out->push_back('-');
    fill(out, tc.mMonth, 2);
    out->push_back('-');
    fill(out, tc.mDay, 2);
    out->push_back('T');
    fill(out, tc.mHour, 2);
    out->push_back(':');
    fill(out, tc.mMinute, 2);
    out->push_back(':');
    fill(out, tc.mSec, 2);
    out->push_back('.');
    fill(out, tc.mUsec, 6);
    out->push_back('Z');
}

string UtcTime::toIso8601() const
{
    string res;
    res.reserve(sizeof("1970-01-01T00:00:00.000000Z"));
    toIso8601(&res);
    return res;
}

void UtcTime::prettyPrint(string* out) const
{
    toIso8601(out);
}

UtcTime UtcTime::now()
{
    timeval tv;
    int r = gettimeofday(&tv, NULL);
    OTS_ASSERT(r == 0)(r)(string(strerror(errno)));
    int64_t usec = tv.tv_sec * kUsecPerSec + tv.tv_usec;
    return UtcTime(usec);
}

} // namespace util
} // namespace tablestore
} // namespace aliyun
