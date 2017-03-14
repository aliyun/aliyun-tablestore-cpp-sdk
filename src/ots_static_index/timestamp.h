#ifndef OTS_STATIC_INDEX_TIMESTAMP_H
#define OTS_STATIC_INDEX_TIMESTAMP_H

#include "logging_assert.h"
#include <string>
#include <stdint.h>

struct timeval;

namespace static_index {

const int64_t kNsecPerUsec = 1000;
const int64_t kUsecPerMsec = 1000;
const int64_t kMsecPerSec = 1000;
const int64_t kUsecPerSec = kMsecPerSec * kUsecPerMsec;
const int64_t kNsecPerSec = kUsecPerSec * kNsecPerUsec;

class Logger;
class Interval;
class MonotonicTime;
class UtcTime;

void SleepFor(const Interval&);
void SleepUntil(const MonotonicTime&);


class ComparableTime
{
protected:
    explicit ComparableTime(Logger* logger, int64_t v)
      : mLogger(logger),
        mValue(v)
    {}

public:
    bool operator==(const ComparableTime& ano) const
    {
        return mValue == ano.mValue;
    }
    bool operator!=(const ComparableTime& ano) const
    {
        return mValue != ano.mValue;
    }

    bool operator<(const ComparableTime& ano) const
    {
        return mValue < ano.mValue;
    }
    bool operator>(const ComparableTime& ano) const
    {
        return mValue > ano.mValue;
    }

    bool operator<=(const ComparableTime& ano) const
    {
        return mValue <= ano.mValue;
    }
    bool operator>=(const ComparableTime& ano) const
    {
        return mValue >= ano.mValue;
    }

    Logger* mLogger;

protected:
    int64_t mValue;
};

class Interval : public ComparableTime
{
public:
    Interval(Logger* logger, int64_t usec)
      : ComparableTime(logger, usec)
    {}
    explicit Interval(Logger* logger)
      : ComparableTime(logger, 0)
    {}

    /*
     * templating, in order to support both integer and double/float.
     */
    template<typename T>
    static Interval FromUsec(Logger* logger, T x)
    {
        return Interval(logger, static_cast<int64_t>(x));
    }

    template<typename T>
    static Interval FromMsec(Logger* logger, T x)
    {
        return Interval(logger, static_cast<int64_t>(x * kUsecPerMsec));
    }

    template<typename T>
    static Interval FromSec(Logger* logger, T x)
    {
        return Interval(logger, static_cast<int64_t>(x * kUsecPerSec));
    }

    template<typename T>
    static Interval FromMin(Logger* logger, T x)
    {
        return Interval(logger, static_cast<int64_t>(x * kUsecPerSec * 60));
    }

    template<typename T>
    static Interval FromHour(Logger* logger, T x)
    {
        return Interval(logger, static_cast<int64_t>(x * kUsecPerSec * 60 * 60));
    }

    Interval operator-() const
    {
        return Interval(mLogger, -mValue);
    }

    Interval& operator+=(const Interval& ano)
    {
        int64_t newValue = mValue + ano.mValue;
        OTS_ASSERT(mLogger, (ano.mValue >= 0) == (newValue >= mValue))
            (mValue)(ano.mValue)
            .What("IntervalInUsec plus overflow!");
        mValue = newValue;
        return *this;
    }

    Interval& operator-=(const Interval& ano)
    {
        int64_t newValue = mValue - ano.mValue;
        OTS_ASSERT(mLogger, (newValue >= 0) == (mValue >= ano.mValue))
            (mValue)(ano.mValue)
            .What("IntervalInUsec minus underflow!");
        mValue = newValue;
        return *this;
    }

    Interval& operator*=(double multiple)
    {
        mValue = static_cast<int64_t>(mValue * multiple);
        return *this;
    }

    int64_t ToIntInUsec() const
    {
        return mValue;
    }

    int64_t ToIntInMsec() const
    {
        return mValue / kUsecPerMsec;
    }

    int64_t ToIntInSec() const
    {
        return mValue / kUsecPerSec;
    }

    ::std::string ToString() const;
};

inline Interval operator+(
    const Interval& a,
    const Interval& b)
{
    Interval result(a);
    result += b;
    return result;
}

inline Interval operator-(
    const Interval& a,
    const Interval& b)
{
    Interval result(a);
    result -= b;
    return result;
}

inline Interval operator*(
    const Interval& a,
    double multiple)
{
    Interval result(a);
    result *= multiple;
    return result;
}

inline Interval operator*(
    double multiple,
    const Interval& a)
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
    MonotonicTime(Logger* logger, int64_t usec)
      : ComparableTime(logger, usec)
    {}
    explicit MonotonicTime(Logger* logger)
      : ComparableTime(logger, 0)
    {}

    static MonotonicTime Now(Logger*);

    Interval operator-(const MonotonicTime& ano) const
    {
        int64_t result = mValue - ano.mValue;
        OTS_ASSERT(mLogger, (result >= 0) == (mValue >= ano.mValue))
            (mValue)(ano.mValue)
            .What("MonotonicTimeInUsec underflow");
        return Interval(mLogger, result);
    }

    MonotonicTime& operator+=(const Interval& inc)
    {
        int64_t newValue = mValue + inc.ToIntInUsec();
        OTS_ASSERT(mLogger, (inc.ToIntInUsec() >= 0) == (newValue >= mValue))
            (mValue)(inc.ToIntInUsec())
            .What("MonotonicTimeInUsec overflow");
        mValue = newValue;
        return *this;
    }

    ::std::string ToString() const;

    friend void SleepUntil(const MonotonicTime&);
};

inline MonotonicTime operator+(
    const MonotonicTime& base,
    const Interval& delta)
{
    MonotonicTime result(base);
    result += delta;
    return result;
}

inline MonotonicTime operator+(
    const Interval& delta,
    const MonotonicTime& base)
{
    return base + delta;
}

/**
 * This is the wall time.
 * Although it can be used to talk to both machines and persons,
 * it will be affacted by both discontinuous jumps and incremental adjustments.
 * That is to say, it is not monotonic.
 * A later will possibly be smaller than a former.
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
public:
    UtcTime(Logger* logger, int64_t usec)
      : ComparableTime(logger, usec)
    {}
    explicit UtcTime(Logger* logger)
      : ComparableTime(logger, 0)
    {}

    static UtcTime FromTimeval(Logger* logger, const timeval& tv)
    {
        return UtcTime(logger, tv.tv_sec * kUsecPerSec + tv.tv_usec);
    }

    static UtcTime Now(Logger* logger);

    Interval operator-(const UtcTime& ano) const
    {
        int64_t result = mValue - ano.mValue;
        OTS_ASSERT(mLogger, (result >= 0) == (mValue >= ano.mValue))
            (mValue)(ano.mValue)
            .What("LocalTimeInUs underflow");
        return Interval(mLogger, result);
    }

    UtcTime& operator+=(const Interval& delta)
    {
        int64_t newValue = mValue + delta.ToIntInUsec();
        OTS_ASSERT(mLogger, (delta.ToIntInUsec() >= 0) == (newValue >= mValue))
            (mValue)(delta)
            .What("WallTimeInUsec overflow");
        mValue = newValue;
        return *this;
    }

    int64_t ToIntInUsec() const
    {
        return mValue;
    }

    int64_t ToIntInMsec() const
    {
        return mValue / kUsecPerMsec;
    }

    int64_t ToIntInSec() const
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
    ::std::string ToISO8601() const;

    ::std::string ToString() const
    {
        return ToISO8601();
    }
};

inline UtcTime operator+(
    const UtcTime& base,
    const Interval& delta)
{
    UtcTime result(base);
    result += delta;
    return result;
}

inline UtcTime operator+(
    const Interval& delta,
    const UtcTime& base)
{
    return base + delta;
}

} // namespace static_index

#endif /* OTS_STATIC_INDEX_TIMESTAMP_H */
