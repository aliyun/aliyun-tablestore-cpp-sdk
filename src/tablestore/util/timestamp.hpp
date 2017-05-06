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
#include "tablestore/util/move.hpp"
#include "tablestore/util/assert.hpp"
#include <string>
#include <stdint.h>

struct timeval;

namespace aliyun {
namespace tablestore {
namespace util {

const int64_t kNsecPerUsec = 1000;
const int64_t kUsecPerMsec = 1000;
const int64_t kUsecPerSec = 1000000;
const int64_t kUsecPerMin = kUsecPerSec * 60;
const int64_t kUsecPerHour = kUsecPerMin * 60;

class Duration;
class MonotonicTime;
class UtcTime;

void sleepFor(const Duration&);
void sleepUnitl(const MonotonicTime&);

class ComparableTime
{
protected:
    explicit ComparableTime(int64_t v) throw()
      : mValue(v)
    {}

public:
    bool operator==(const ComparableTime& ano) const throw()
    {
        return mValue == ano.mValue;
    }
    bool operator!=(const ComparableTime& ano) const throw()
    {
        return mValue != ano.mValue;
    }

    bool operator<(const ComparableTime& ano) const throw()
    {
        return mValue < ano.mValue;
    }
    bool operator>(const ComparableTime& ano) const throw()
    {
        return mValue > ano.mValue;
    }

    bool operator<=(const ComparableTime& ano) const throw()
    {
        return mValue <= ano.mValue;
    }
    bool operator>=(const ComparableTime& ano) const throw()
    {
        return mValue >= ano.mValue;
    }

protected:
    int64_t mValue;
};


class Duration : public ComparableTime
{
private:
    explicit Duration(int64_t usec) throw()
      : ComparableTime(usec)
    {}

public:
    explicit Duration() throw()
      : ComparableTime(0)
    {}
    explicit Duration(const MoveHolder<Duration>& ano) throw()
      : ComparableTime(ano->mValue)
    {}

    Duration& operator=(const MoveHolder<Duration>& ano) throw()
    {
        mValue = ano->mValue;
        return *this;
    }
    
    /*
     * templating, in order to support both integer and double/float.
     */
    template<typename T>
    static Duration fromUsec(T x) throw()
    {
        return Duration(static_cast<int64_t>(x));
    }

    template<typename T>
    static Duration fromMsec(T x) throw()
    {
        return Duration(static_cast<int64_t>(x * kUsecPerMsec));
    }

    template<typename T>
    static Duration fromSec(T x) throw()
    {
        return Duration(static_cast<int64_t>(x * kUsecPerSec));
    }

    template<typename T>
    static Duration fromMin(T x) throw()
    {
        return Duration(static_cast<int64_t>(x * kUsecPerSec * 60));
    }

    template<typename T>
    static Duration fromHour(T x) throw()
    {
        return Duration(static_cast<int64_t>(x * kUsecPerSec * 60 * 60));
    }

    Duration operator-() const throw()
    {
        return Duration(-mValue);
    }

    Duration& operator+=(const Duration& ano) throw()
    {
        int64_t newValue = mValue + ano.mValue;
        OTS_ASSERT((ano.mValue >= 0) == (newValue >= mValue))
            (mValue)
            (ano.mValue)
            .what("Duration overflow!");
        mValue = newValue;
        return *this;
    }

    Duration& operator-=(const Duration& ano) throw()
    {
        int64_t newValue = mValue - ano.mValue;
        OTS_ASSERT((newValue >= 0) == (mValue >= ano.mValue))
            (mValue)
            (ano.mValue)
            .what("Duration underflow!");
        mValue = newValue;
        return *this;
    }

    template<class T>
    Duration& operator*=(T multiple) throw()
    {
        mValue = static_cast<int64_t>(mValue * multiple);
        return *this;
    }

    int64_t toUsec() const throw()
    {
        return mValue;
    }

    int64_t toMsec() const throw()
    {
        return mValue / kUsecPerMsec;
    }

    int64_t toSec() const throw()
    {
        return mValue / kUsecPerSec;
    }

    int64_t toMin() const throw()
    {
        return mValue / kUsecPerMin;
    }

    int64_t toHour() const throw()
    {
        return mValue / kUsecPerHour;
    }

    void prettyPrint(std::string*) const;
};

inline Duration operator+(Duration a, Duration b)
{
    Duration result(a);
    result += b;
    return result;
}

inline Duration operator-(Duration a, Duration b)
{
    Duration result(a);
    result -= b;
    return result;
}

template<class T>
Duration operator*(Duration a, T multiple)
{
    Duration result(a);
    result *= multiple;
    return result;
}

template<class T>
Duration operator*(T multiple, Duration a)
{
    return a * multiple;
}


/**
 * Monotonic time is, as its name shown, monotonic.
 * This is not affected by discontinuous jumps in the system time
 * (e.g., if the system administrator manually changes the clock).
 * But its speed of increasing, although it is always positive,
 * will be affacted by adjtime(3) and NTP.
 * Besides, its start point is undefined.
 */
class MonotonicTime : public ComparableTime
{
public:
    explicit MonotonicTime(int64_t usec) throw()
      : ComparableTime(usec)
    {}
    explicit MonotonicTime() throw()
      : ComparableTime(0)
    {}
    explicit MonotonicTime(const MoveHolder<MonotonicTime>& ano) throw()
      : ComparableTime(ano->mValue)
    {}

    MonotonicTime& operator=(const MoveHolder<MonotonicTime>& ano) throw()
    {
        mValue = ano->mValue;
        return *this;
    }
    
    static MonotonicTime now();

    Duration operator-(const MonotonicTime& ano) const throw()
    {
        int64_t result = mValue - ano.mValue;
        OTS_ASSERT((result >= 0) == (mValue >= ano.mValue))
            (mValue)(ano.mValue)
            .what("MonotonicTime underflow");
        return Duration::fromUsec(result);
    }

    MonotonicTime& operator+=(const Duration& inc) throw()
    {
        int64_t newValue = mValue + inc.toUsec();
        OTS_ASSERT((inc.toUsec() >= 0) == (newValue >= mValue))
            (mValue)(inc.toUsec())
            .what("MonotonicTime overflow");
        mValue = newValue;
        return *this;
    }

    void prettyPrint(std::string*) const;

private:
    int64_t toUsec() const throw()
    {
        return mValue;
    }
    
    friend void sleepUntil(const MonotonicTime&);
};

inline MonotonicTime operator+(MonotonicTime base, Duration delta) throw()
{
    MonotonicTime result(base);
    result += delta;
    return result;
}

inline MonotonicTime operator+(Duration delta, MonotonicTime base) throw()
{
    return base + delta;
}


/**
 * This is the wall time.
 * Although it can be used to talk to both machines and persons,
 * it will be affacted by both discontinuous jumps and incremental adjustments.
 * That is to say, it is not monotonic.
 * A later one will possibly be smaller than a former one.
 *
 * Caveats:
 * Precisely speaking, this is linux timestamp, rather than UTC timestamp.
 * They differ on leap seconds. For example, after 2008-12-31T23:59:59Z,
 * there is a leap second. In UTC, this leap second is represented by
 * 2008-12-31T23:59:60Z. In linux timestamp, such representation is invalid.
 * Instead, linux timestamp makes 2008-12-31T23:59:59Z last 2 physical seconds
 * and thus 2009-01-01T00:00:00Z is the same in both UTC and linux timestamp.
 * BTW, NTP implements linux timestamp by jumping back to 2008-12-31T23:59:59Z
 * when it is 2008-12-31T23:59:60Z. So, discontinuous jump in NTP is not an
 * irrevocable flaw. It is a feature.
 */
class UtcTime : public ComparableTime
{
    explicit UtcTime(int64_t usec) throw()
      : ComparableTime(usec)
    {}
public:
    explicit UtcTime() throw()
      : ComparableTime(0)
    {}
    explicit UtcTime(const MoveHolder<UtcTime>& ano) throw()
      : ComparableTime(ano->mValue)
    {}

    UtcTime& operator=(const MoveHolder<UtcTime>& a)
    {
        mValue = a->mValue;
        return *this;
    }

    static UtcTime fromTimeval(const timeval& tv) throw()
    {
        return UtcTime(tv.tv_sec * kUsecPerSec + tv.tv_usec);
    }
    static UtcTime fromUsec(int64_t usec) throw()
    {
        return UtcTime(usec);
    }
    static UtcTime fromMsec(int64_t msec) throw()
    {
        return UtcTime(msec * kUsecPerMsec);
    }
    static UtcTime fromSec(int64_t sec) throw()
    {
        return UtcTime(sec * kUsecPerSec);
    }
    static UtcTime fromMin(int64_t min) throw()
    {
        return UtcTime(min * kUsecPerMin);
    }
    static UtcTime fromHour(int64_t hour) throw()
    {
        return UtcTime(hour * kUsecPerHour);
    }
    
    static UtcTime now();

    Duration operator-(const UtcTime& ano) const throw()
    {
        int64_t result = mValue - ano.mValue;
        OTS_ASSERT((result >= 0) == (mValue >= ano.mValue))
            (mValue)(ano.mValue)
            .what("UtcTime underflow");
        return Duration::fromUsec(result);
    }

    UtcTime& operator+=(const Duration& delta) throw()
    {
        int64_t newValue = mValue + delta.toUsec();
        OTS_ASSERT((delta.toUsec() >= 0) == (newValue >= mValue))
            (mValue)(delta)
            .what("UtcTime overflow");
        mValue = newValue;
        return *this;
    }

    int64_t toUsec() const throw()
    {
        return mValue;
    }

    int64_t toMsec() const throw()
    {
        return mValue / kUsecPerMsec;
    }

    int64_t toSec() const throw()
    {
        return mValue / kUsecPerSec;
    }

    /**
     * The result confirms to ISO 8601.
     * It slightly different with ToStream() for its timezone part is always 'Z'.
     *
     * Precisely speaking, it will be "year-month-dayThour:minute:second.usecZ",
     * where year is of 4 digits,
     * month is of 2 digits (starting with 01),
     * day is of 2 digits (starting with 01),
     * hour is of 2 digits (between 00 and 23, inclusive),
     * minute is of 2 digits (between 00 and 59, inclusive),
     * second is of 2 digits (between 00 and 59, inclusive),
     * usec is of 6 digits (padding with 0).
     * There is no whitespaces between all parts.
     */
    void toIso8601(std::string*) const;
    std::string toIso8601() const;

    void prettyPrint(std::string*) const;
};

inline UtcTime operator+(UtcTime base, Duration delta) throw()
{
    UtcTime result(base);
    result += delta;
    return result;
}

inline UtcTime operator+(Duration delta, UtcTime base) throw()
{
    return base + delta;
}

} // namespace core
} // namespace tablestore
} // namespace aliyun
