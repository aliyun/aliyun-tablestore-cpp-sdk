#include "timestamp.h"
#include "string_tools.h"
#include "arithmetic.h"

#include <tr1/tuple>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <ctime>
extern "C" {
#include <sys/time.h>
}

using namespace ::std;
using namespace ::std::tr1;

namespace static_index {

void SleepFor(const Interval& usec)
{
    SleepUntil(MonotonicTime::Now(usec.mLogger) + usec);
}

void SleepUntil(const MonotonicTime& target)
{
    int64_t tm = target.mValue;
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
                OTS_ASSERT(target.mLogger, r)(e)(strerror(e));
            }
        }
    }
}

string Interval::ToString() const
{
    std::string result;
    result.reserve(sizeof("1392652800000000") // 2014-02-18T00:00:00Z
        + sizeof("IntervalInUsec()"));
    result += "IntervalInUsec(";
    result += static_index::ToString(mValue);
    result += ")";
    return result;
}

MonotonicTime MonotonicTime::Now(Logger* logger)
{
    timespec tm;
    int r = clock_gettime(CLOCK_MONOTONIC, &tm);
    OTS_ASSERT(logger, r == 0)(r)(strerror(errno));
    int64_t usec = tm.tv_sec * kUsecPerSec + tm.tv_nsec / kNsecPerUsec;
    return MonotonicTime(logger, usec);
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

TimeComponent DecomposeToUtc(Logger* logger, const UtcTime& tm)
{
    TimeComponent result;
    int64_t t = tm.ToIntInUsec();
    int64_t q = 0;
    tie(q, result.mUsec) = DivMod(t, kUsecPerSec);
    time_t x = q;
    struct tm ti;
    struct tm* r = gmtime_r(&x, &ti);
    OTS_ASSERT(logger, r != NULL)(tm).What("time out of range");
    result.mYear = ti.tm_year + 1900;
    result.mMonth = ti.tm_mon + 1;
    result.mDay = ti.tm_mday;
    result.mHour = ti.tm_hour;
    result.mMinute = ti.tm_min;
    result.mSec = ti.tm_sec;
    return result;
}

void FormatInt(string* out, int num, int digits)
{
    const int start = out->size();
    out->resize(start + digits, '0');
    for (int end = start + digits - 1;
        num > 0 && end >= start;
        num /= 10, --end)
    {
        char ch = num % 10 + '0';
        out->at(end) = ch;
    }
}

} // namespace

string UtcTime::ToISO8601() const
{
    string res;
    res.reserve(sizeof("1970-01-01T00:00:00.000000Z"));
    const TimeComponent& tc = DecomposeToUtc(mLogger, *this);
    FormatInt(&res, tc.mYear, 4);
    res.push_back('-');
    FormatInt(&res, tc.mMonth, 2);
    res.push_back('-');
    FormatInt(&res, tc.mDay, 2);
    res.push_back('T');
    FormatInt(&res, tc.mHour, 2);
    res.push_back(':');
    FormatInt(&res, tc.mMinute, 2);
    res.push_back(':');
    FormatInt(&res, tc.mSec, 2);
    res.push_back('.');
    FormatInt(&res, tc.mUsec, 6);
    res.push_back('Z');
    return res;
}

UtcTime UtcTime::Now(Logger* logger)
{
    timeval tv;
    int r = gettimeofday(&tv, NULL);
    OTS_ASSERT(logger, r == 0)(r)(strerror(errno));
    int64_t usec = tv.tv_sec * kUsecPerSec + tv.tv_usec;
    return UtcTime(logger, usec);
}

} // namespace static_index

