#include "static_index_common.h"

using namespace ::std;
using namespace ::std::tr1;
using namespace ::std::tr1::placeholders;
using namespace ::testing;
using namespace ::aliyun::openservices::ots;
using namespace ::static_index;

TEST(Exceptional, OK)
{
    static_index::Exceptional ex;
    ASSERT_EQ(ex.GetCode(), static_index::Exceptional::OTS_OK);
}

TEST(Exceptional, ClientException)
{
    static_index::Exceptional ex("hoho");
    ASSERT_EQ(ex.GetCode(), static_index::Exceptional::OTS_CLIENT_EXCEPTION);
    ASSERT_EQ(ex.GetErrorCode(), "ClientParameterError");
    ASSERT_EQ(ex.GetErrorMessage(), "hoho");
}

TEST(Exceptional, ClientExceptionFromOTSException)
{
    OTSExceptionImpl otsEx("x", "y", "this is a message", "");
    static_index::Exceptional ex(otsEx);
    ASSERT_EQ(ex.GetCode(), static_index::Exceptional::OTS_CLIENT_EXCEPTION);
    ASSERT_EQ(ex.GetErrorCode(), "y");
    ASSERT_EQ(ex.GetErrorMessage(), "this is a message");
}

TEST(Exceptional, ServiceExceptionFromOTSException)
{
    OTSExceptionImpl otsEx("x", "y", "this is a message", "Req5EDC", "500", "");
    static_index::Exceptional ex(otsEx);
    ASSERT_EQ(ex.GetCode(), static_index::Exceptional::OTS_SERVICE_EXCEPTION);
    ASSERT_EQ(ex.GetErrorCode(), "y");
    ASSERT_EQ(ex.GetErrorMessage(), "this is a message");
    ASSERT_EQ(ex.GetRequestId(), "Req5EDC");
}

TEST(Slice, LexicographicOrder)
{
    string a("a");
    string aa("aa");
    string b("b");
    string b_("b");
    EXPECT_EQ(ToSlice(a).LexicographicOrder(ToSlice(aa)), -1);
    EXPECT_EQ(ToSlice(a).LexicographicOrder(ToSlice(b)), -1);
    EXPECT_EQ(ToSlice(a).LexicographicOrder(ToSlice(a)), 0);
    EXPECT_EQ(ToSlice(b).LexicographicOrder(ToSlice(b_)), 0);
    EXPECT_EQ(ToSlice(aa).LexicographicOrder(ToSlice(a)), 1);
    EXPECT_EQ(ToSlice(b).LexicographicOrder(ToSlice(a)), 1);
}

namespace {

class Readable
{
public:
    string ToString() const
    {
        return string("i'm readable");
    }
};

} // namespace

TEST(ToString, Default)
{
    Readable r;
    ASSERT_EQ(static_index::ToString(r), string("i'm readable"));
}

TEST(ToString, Int8)
{
    {
        int8_t x = 0;
        ASSERT_EQ(static_index::ToString(x), string("0"));
    }
    {
        int8_t x = 123;
        ASSERT_EQ(static_index::ToString(x), string("123"));
    }
    {
        int8_t x = -123;
        ASSERT_EQ(static_index::ToString(x), string("-123"));
    }
    {
        int8_t x = numeric_limits<int8_t>::min();
        ASSERT_EQ(static_index::ToString(x), string("-128"));
    }
    {
        int8_t x = numeric_limits<int8_t>::max();
        ASSERT_EQ(static_index::ToString(x), string("127"));
    }
}

TEST(ToString, Uint8)
{
    uint8_t x = numeric_limits<uint8_t>::max();
    ASSERT_EQ(static_index::ToString(x), string("255"));
}

TEST(ToString, Int16)
{
    {
        int16_t x = 12345;
        ASSERT_EQ(static_index::ToString(x), string("12345"));
    }
    {
        int16_t x = -12345;
        ASSERT_EQ(static_index::ToString(x), string("-12345"));
    }
    {
        int16_t x = numeric_limits<int16_t>::min();
        ASSERT_EQ(static_index::ToString(x), string("-32768"));
    }
    {
        int16_t x = numeric_limits<int16_t>::max();
        ASSERT_EQ(static_index::ToString(x), string("32767"));
    }
}

TEST(ToString, Uint16)
{
    uint16_t x = numeric_limits<uint16_t>::max();
    ASSERT_EQ(static_index::ToString(x), string("65535"));
}

TEST(ToString, Int32)
{
    {
        int32_t x = numeric_limits<int32_t>::min();
        ASSERT_EQ(static_index::ToString(x), string("-2147483648"));
    }
    {
        int32_t x = numeric_limits<int32_t>::max();
        ASSERT_EQ(static_index::ToString(x), string("2147483647"));
    }
}

TEST(ToString, Uint32)
{
    uint32_t x = numeric_limits<uint32_t>::max();
    ASSERT_EQ(static_index::ToString(x), string("4294967295"));
}

TEST(ToString, Int64)
{
    {
        int64_t x = numeric_limits<int64_t>::min();
        ASSERT_EQ(static_index::ToString(x), string("-9223372036854775808"));
    }
    {
        int64_t x = numeric_limits<int64_t>::max();
        ASSERT_EQ(static_index::ToString(x), string("9223372036854775807"));
    }
}

TEST(ToString, Uint64)
{
    uint64_t x = numeric_limits<uint64_t>::max();
    ASSERT_EQ(static_index::ToString(x), string("18446744073709551615"));
}

TEST(ToString, Char)
{
    char x = '@';
    ASSERT_EQ(static_index::ToString(x), string("@"));
}

TEST(ToString, Str)
{
    string x = "hello";
    ASSERT_EQ(static_index::ToString(x), x);
}

TEST(ToString, CStr)
{
    const char cstr[] = "cstr";
    string expect(cstr);

    ASSERT_EQ(static_index::ToString(cstr), expect);
    {
        const char* x = cstr;
        ASSERT_EQ(static_index::ToString(x), expect);
    }
    {
        char* x = (char*) cstr;
        ASSERT_EQ(static_index::ToString(x), expect);
    }
}

TEST(ToString, Bool)
{
    ASSERT_EQ(static_index::ToString(true), string("true"));
    ASSERT_EQ(static_index::ToString(false), string("false"));
}

namespace {

class TestLogger : public static_index::Logger
{
    string* mOut;
    Logger::LogLevel mLevel;

public:
    explicit TestLogger(string* out, Logger::LogLevel lvl)
      : mOut(out),
        mLevel(lvl)
    {}

    LogLevel GetLogLevel() const
    {
        return mLevel;
    }
    void Record(LogLevel, const string& rec)
    {
        mOut->append("record: ");
        {
            size_t pos = rec.find(", ");
            for(int i = 0; i < 2; ++i) {
                assert(pos != string::npos);
                pos = rec.find(", ", pos + 2);
            }
            assert(pos != string::npos);
            mOut->append(rec.substr(pos + 2));
        }
        mOut->push_back('\n');
    }
    void Flush()
    {
        mOut->append("flush\n");
    }
};

} // namespace

TEST(Logging, Basic)
{
    string out;
    TestLogger logger(&out, static_index::Logger::DEBUG);

    string greeting("hello");
    string who("Taoda");
    OTS_LOG_DEBUG(&logger)(greeting)(who);
    ASSERT_EQ(out, string("record: greeting:hello, who:Taoda\n"));
}

TEST(Logging, What)
{
    string out;
    TestLogger logger(&out, static_index::Logger::DEBUG);

    string who("Taoda");
    OTS_LOG_DEBUG(&logger)(who).What("greeting");
    ASSERT_EQ(out, string("record: What:greeting, who:Taoda\n"));
}

TEST(Logging, BareWhat)
{
    string out;
    TestLogger logger(&out, static_index::Logger::DEBUG);

    OTS_LOG_DEBUG(&logger).What("greeting");
    ASSERT_EQ(out, string("record: What:greeting\n"));
}

TEST(Logging, Info)
{
    {
        string out;
        TestLogger logger(&out, static_index::Logger::INFO);

        string who("Taoda");
        OTS_LOG_DEBUG(&logger)(who).What("greeting");
        ASSERT_EQ(out, string());
    }
    {
        string out;
        TestLogger logger(&out, static_index::Logger::INFO);

        string who("Taoda");
        OTS_LOG_INFO(&logger)(who).What("greeting");
        ASSERT_EQ(out, string("record: What:greeting, who:Taoda\n"));
    }
}

TEST(Logging, Error)
{
    {
        string out;
        TestLogger logger(&out, static_index::Logger::ERROR);

        string who("Taoda");
        OTS_LOG_INFO(&logger)(who).What("greeting");
        ASSERT_EQ(out, string());
    }
    {
        string out;
        TestLogger logger(&out, static_index::Logger::ERROR);

        string who("Taoda");
        OTS_LOG_ERROR(&logger)(who).What("greeting");
        ASSERT_EQ(out, string("record: What:greeting, who:Taoda\n"));
    }
}

namespace static_index {

extern int64_t OTSFLAG_ASSERT_ABORT;

} // namespace static_index

TEST(Assert, Basic)
{
    FlagSetter<int64_t> aa(OTSFLAG_ASSERT_ABORT, 0);
    string out;
    TestLogger logger(&out, Logger::ERROR);

    int64_t x = 41;
    OTS_ASSERT(&logger, x == 42);
    ASSERT_EQ(out, string("record: Condition:x == 42\nflush\n"));
}

TEST(Assert, What)
{
    FlagSetter<int64_t> aa(OTSFLAG_ASSERT_ABORT, 0);
    string out;
    TestLogger logger(&out, Logger::ERROR);

    int64_t x = 41;
    OTS_ASSERT(&logger, x == 42).What("greeting");
    ASSERT_EQ(out, string("record: What:greeting, Condition:x == 42\nflush\n"));
}

TEST(Assert, Var)
{
    FlagSetter<int64_t> aa(OTSFLAG_ASSERT_ABORT, 0);
    string out;
    TestLogger logger(&out, Logger::ERROR);

    int64_t x = 41;
    OTS_ASSERT(&logger, x == 42)(x);
    ASSERT_EQ(out, string("record: Condition:x == 42, x:41\nflush\n"));
}

TEST(Interval, Cons)
{
    DummyLogger logger;
    
    EXPECT_EQ(Interval::FromHour(&logger, 1).ToIntInUsec(), 3600 * kUsecPerSec);
    EXPECT_EQ(Interval::FromMin(&logger, 1).ToIntInUsec(), 60 * kUsecPerSec);
    EXPECT_EQ(Interval::FromSec(&logger, 1).ToIntInUsec(), kUsecPerSec);
    EXPECT_EQ(Interval::FromMsec(&logger, 1).ToIntInUsec(), kUsecPerMsec);
    EXPECT_EQ(Interval::FromUsec(&logger, 1).ToIntInUsec(), 1);

    EXPECT_EQ(Interval::FromHour(&logger, 1.0).ToIntInUsec(), 3600 * kUsecPerSec);
    EXPECT_EQ(Interval::FromMin(&logger, 1.0).ToIntInUsec(), 60 * kUsecPerSec);
    EXPECT_EQ(Interval::FromSec(&logger, 1.0).ToIntInUsec(), kUsecPerSec);
    EXPECT_EQ(Interval::FromMsec(&logger, 1.0).ToIntInUsec(), kUsecPerMsec);
    EXPECT_EQ(Interval::FromUsec(&logger, 1.0).ToIntInUsec(), 1);
}

TEST(Interval, Assignment)
{
    DummyLogger logger;
    Interval a(&logger, 1);
    Interval b(&logger, 2);
    Interval c(a);
    EXPECT_TRUE(a == c);
    c = b;
    EXPECT_TRUE(b == c);
}

TEST(Interval, Plus)
{
    DummyLogger logger;
    Interval a(&logger, 1);
    Interval b(&logger, 2);
    Interval c = a + b;
    EXPECT_EQ(c.ToIntInUsec(), 3);
    b += a;
    EXPECT_EQ(b.ToIntInUsec(), 3);
}

TEST(Interval, Minus)
{
    DummyLogger logger;
    Interval a(&logger, 1);
    Interval b(&logger, 2);
    Interval c = a - b;
    EXPECT_EQ(c.ToIntInUsec(), -1);
    a -= b;
    EXPECT_EQ(a.ToIntInUsec(), -1);
}

TEST(Interval, Negate)
{
    DummyLogger logger;
    Interval a(&logger, 1);
    Interval b = -a;
    EXPECT_EQ(b.ToIntInUsec(), -1);
}

TEST(Interval, ToInt)
{
    DummyLogger logger;
    Interval a(&logger, 1002003);
    EXPECT_EQ(a.ToIntInUsec(), 1002003);
    EXPECT_EQ(a.ToIntInMsec(), 1002);
    EXPECT_EQ(a.ToIntInSec(), 1);
}

TEST(Interval, Multiply)
{
    DummyLogger logger;
    Interval a(&logger, 1);
    Interval b = a * 3;
    EXPECT_EQ(b.ToIntInUsec(), 3);
    Interval c = 3 * a;
    EXPECT_EQ(c.ToIntInUsec(), 3);
    a *= 3;
    EXPECT_EQ(a.ToIntInUsec(), 3);
}

TEST(Monotonic, Assignment)
{
    DummyLogger logger;
    MonotonicTime a(&logger, 1);
    MonotonicTime b(&logger, 2);
    MonotonicTime c(a);
    EXPECT_TRUE(a == c);
    c = b;
    EXPECT_TRUE(b == c);
}

TEST(Monotonic, Distance)
{
    DummyLogger logger;
    MonotonicTime a(&logger, 2);
    MonotonicTime b(&logger, 1);
    const Interval& i = a - b;
    EXPECT_EQ(i.ToIntInUsec(), 1);
}

TEST(Monotonic, PlusDistance)
{
    DummyLogger logger;
    MonotonicTime a(&logger, 1);
    const Interval inc(&logger, 2);
    const MonotonicTime& b = a + inc;
    EXPECT_TRUE(b - a == inc);
    const MonotonicTime& c = inc + a;
    EXPECT_TRUE(c == b);
    a += inc;
    EXPECT_TRUE(a == b);
}

TEST(Monotonic, Ordering)
{
    DummyLogger logger;
    MonotonicTime a(&logger, 1);
    MonotonicTime b(&logger, 2);
    EXPECT_EQ(a, a);
    EXPECT_NE(a, b);
    EXPECT_LT(a, b);
    EXPECT_LE(a, b);
    EXPECT_GT(b, a);
    EXPECT_GE(b, a);
}

namespace {

template<typename X, typename Y, typename Z>
void CheckRound(Logger* logger, X a, Y b, Z diff)
{
    OTS_ASSERT(logger, b - diff < a)(a)(b)(diff);
    OTS_ASSERT(logger, a < b + diff)(a)(b)(diff);
}

} // namespace

TEST(Monotonic, Now)
{
    DummyLogger logger;
    const MonotonicTime& a = MonotonicTime::Now(&logger);
    SleepFor(Interval::FromSec(&logger, 1));
    const MonotonicTime& b = MonotonicTime::Now(&logger);
    const Interval& i = b - a;
    CheckRound(&logger, i.ToIntInUsec(), kUsecPerSec, kUsecPerSec / 100);
}

TEST(UtcTime, FromTimeval)
{
    DummyLogger logger;
    timeval tv;
    tv.tv_sec = 1;
    tv.tv_usec = 1;
    UtcTime tm = UtcTime::FromTimeval(&logger, tv);
    EXPECT_EQ(tm.ToIntInUsec(), kUsecPerSec + 1);
}

TEST(UtcTime, Ordering)
{
    DummyLogger logger;
    UtcTime a(&logger, 1);
    UtcTime b(&logger, 2);
    EXPECT_EQ(a, a);
    EXPECT_NE(a, b);
    EXPECT_LT(a, b);
    EXPECT_LE(a, b);
    EXPECT_GT(b, a);
    EXPECT_GE(b, a);
}

TEST(UtcTime, Distance)
{
    DummyLogger logger;
    UtcTime a(&logger, 2);
    UtcTime b(&logger, 1);
    const Interval& i = a - b;
    EXPECT_EQ(i.ToIntInUsec(), 1);
}

TEST(UtcTime, PlusDistance)
{
    DummyLogger logger;
    UtcTime a(&logger, 1);
    const Interval inc(&logger, 2);
    const UtcTime& b = a + inc;
    EXPECT_EQ(b.ToIntInUsec(), 3);
    const UtcTime& c = inc + a;
    EXPECT_EQ(c.ToIntInUsec(), 3);
    a += inc;
    EXPECT_EQ(a.ToIntInUsec(), 3);
}

TEST(UtcTime, ToString1)
{
    DummyLogger logger;
    UtcTime x(&logger, 0);
    EXPECT_EQ(ToString(x), "1970-01-01T00:00:00.000000Z");
}

TEST(Arithmetic, DivMod)
{
    DummyLogger logger;
    const int b = 5;
    for (int a = -10; a <= 10; ++a) {
        int q = 0;
        int r = 0;
        tie(q, r) = DivMod(a, b);
        OTS_ASSERT(&logger, q*b+r==a)(a)(b)(q)(r);
        OTS_ASSERT(&logger, 0<=r && r<b)(a)(b)(q)(r);
    }
}

namespace {

void NondeterFunc(vector<int>* outs, Mutex* mutex, IRandom* rnd, int idx)
{
    SleepFor(Interval::FromMsec(rnd->GetLogger(), NextInt(rnd, 100)));
    Scoped<Mutex> g(mutex);
    outs->push_back(idx);
}

} // namespace

TEST(Threading, Nondeterministic)
{
    FileSyncLogger logger("Threading.Nondeterministic.log");
    const int kThreads = 100;
    vector<shared_ptr<Thread> > threads;
    vector<int> outputs;
    Mutex mutex(&logger);
    auto_ptr<IRandom> rnd(NewDefaultRandom(&logger));

    for(int i = 0; i < kThreads; ++i) {
        shared_ptr<Thread> t(Thread::New(&logger,
                bind(NondeterFunc, &outputs, &mutex, rnd.get(), i)));
        threads.push_back(t);
    }

    for(; !threads.empty(); threads.pop_back()) {
        threads.back()->Join();
    }

    for(int i = 0, sz = outputs.size(); i < sz; ++i) {
        OTS_LOG_DEBUG(&logger)(i)(outputs[i]).What("Outputs of threads");
    }
    
    int64_t acc = 0;
    for(int i = 0, sz = outputs.size(); i < sz; ++i) {
        acc += outputs[i];
    }
    EXPECT_EQ(acc, static_cast<int64_t>(outputs.size() * (outputs.size() - 1)) / 2);

    int64_t mismatch = 0;
    for(int i = 0, sz = outputs.size(); i < sz; ++i) {
        if (outputs[i] != i) {
            ++mismatch;
        }
    }
    EXPECT_GT(mismatch, 0);
}

namespace {

void NondeterClosure(vector<int>* outs, Mutex* mutex, Semaphore* sem, IRandom* rnd, int idx)
{
    SleepFor(Interval::FromMsec(rnd->GetLogger(), NextInt(rnd, 100)));
    {
        Scoped<Mutex> g(mutex);
        outs->push_back(idx);
    }
    sem->Post();
}

} // namespace

TEST(ThreadPool, Nondeterministic)
{
    FileSyncLogger logger("ThreadPool.Nondeterministic.log");
    const int kTasks = 100;
    vector<int> outputs;
    Mutex mutex(&logger);
    Semaphore sem(&logger, 0);
    auto_ptr<IRandom> rnd(NewDefaultRandom(&logger));
    auto_ptr<ThreadPool> threads(ThreadPool::New(&logger, 20, 200));

    for(int i = 0; i < kTasks; ++i) {
        bool ret = threads->TryEnqueue(
            bind(NondeterClosure, &outputs, &mutex, &sem, rnd.get(), i));
        OTS_ASSERT(&logger, ret);
    }
    for(int i = 0; i < kTasks; ++i) {
        sem.Wait();
    }
    threads.reset();
    
    for(int i = 0, sz = outputs.size(); i < sz; ++i) {
        OTS_LOG_DEBUG(&logger)(i)(outputs[i]).What("Outputs of threads");
    }
    
    int64_t acc = 0;
    for(int i = 0, sz = outputs.size(); i < sz; ++i) {
        acc += outputs[i];
    }
    EXPECT_EQ(acc, static_cast<int64_t>(outputs.size() * (outputs.size() - 1)) / 2);

    int64_t mismatch = 0;
    for(int i = 0, sz = outputs.size(); i < sz; ++i) {
        if (outputs[i] != i) {
            ++mismatch;
        }
    }
    EXPECT_GT(mismatch, 0);
}

namespace {

void AlarmTrigger(Mutex* mutex, string* log, const string& msg)
{
    Scoped<Mutex> g(mutex);
    log->append(msg);
}

} // namespace

TEST(AlarmClock, Abs)
{
    FileSyncLogger logger("AlarmClock.Abs.log");
    auto_ptr<IRandom> rnd(NewDefaultRandom(&logger));
    auto_ptr<ThreadPool> threads(ThreadPool::New(&logger, 20, 200));
    AlarmClock alarm(&logger, rnd.get(), threads.get());
    MonotonicTime now = MonotonicTime::Now(&logger);
    Mutex mutex(&logger);
    string log;
    alarm.AddAbsolutely(
        now + Interval::FromMsec(&logger, 100),
        bind(AlarmTrigger, &mutex, &log, string("world")));
    alarm.AddAbsolutely(
        now + Interval::FromMsec(&logger, 50),
        bind(AlarmTrigger, &mutex, &log, string("hello ")));
    SleepUntil(now + Interval::FromMsec(&logger, 150));

    Scoped<Mutex> g(&mutex);
    ASSERT_EQ(log, string("hello world"));
}

TEST(BuiltIn, Hex)
{
    EXPECT_EQ(Hex(0x0123456789abcdefULL), string("0123456789ABCDEF"));
    EXPECT_EQ(Hex(0xfedcba9876543210ULL), string("FEDCBA9876543210"));
}

TEST(BuiltIn, ShiftToUint64)
{
    EXPECT_EQ(ShiftToUint64(numeric_limits<int64_t>::min()), 0ull);
    EXPECT_EQ(ShiftToUint64(0), 0x8000000000000000ull);
    EXPECT_EQ(ShiftToUint64(numeric_limits<int64_t>::max()), 0xFFFFFFFFFFFFFFFFull);
}

TEST(BuiltIn, Crc64)
{
    EXPECT_EQ(Hex(Crc64Str(ToSlice("123456789"))), "E9C6D914C4B8D9CA");
}

TEST(Value, Comparision)
{
    DummyLogger logger;
    static_index::Value intSmall(&logger, (int64_t) 0);
    static_index::Value intLarge(&logger, (int64_t) 1);
    static_index::Value strSmall(&logger, string("a"));
    static_index::Value strLarge(&logger, string("b"));
    static_index::Value boolSmall(&logger, false);
    static_index::Value boolLarge(&logger, true);
    static_index::Value dblSmall(&logger, 0.0);
    static_index::Value dblLarge(&logger, 1.0);

    EXPECT_EQ(intSmall.Compare(intSmall), Value::EQUIV);
    EXPECT_EQ(strSmall.Compare(strSmall), Value::EQUIV);
    EXPECT_EQ(boolSmall.Compare(boolSmall), Value::EQUIV);
    EXPECT_EQ(dblSmall.Compare(dblSmall), Value::EQUIV);

    EXPECT_EQ(intSmall.Compare(intLarge), Value::SMALLER);
    EXPECT_EQ(strSmall.Compare(strLarge), Value::SMALLER);
    EXPECT_EQ(boolSmall.Compare(boolLarge), Value::SMALLER);
    EXPECT_EQ(dblSmall.Compare(dblLarge), Value::SMALLER);

    EXPECT_EQ(intLarge.Compare(intSmall), Value::LARGER);
    EXPECT_EQ(strLarge.Compare(strSmall), Value::LARGER);
    EXPECT_EQ(boolLarge.Compare(boolSmall), Value::LARGER);
    EXPECT_EQ(dblLarge.Compare(dblSmall), Value::LARGER);

    EXPECT_EQ(intSmall.Compare(strSmall), Value::TYPE_MISMATCH);
    EXPECT_EQ(intSmall.Compare(boolSmall), Value::TYPE_MISMATCH);
    EXPECT_EQ(intSmall.Compare(dblSmall), Value::TYPE_MISMATCH);
    EXPECT_EQ(strSmall.Compare(boolSmall), Value::TYPE_MISMATCH);
    EXPECT_EQ(strSmall.Compare(dblSmall), Value::TYPE_MISMATCH);
    EXPECT_EQ(boolSmall.Compare(dblSmall), Value::TYPE_MISMATCH);

    EXPECT_TRUE(intSmall == intSmall);
    EXPECT_FALSE(intSmall == intLarge);
    EXPECT_FALSE(intSmall == strSmall);
    EXPECT_FALSE(intSmall == boolSmall);
    EXPECT_FALSE(intSmall == dblSmall);
    EXPECT_TRUE(strSmall == strSmall);
    EXPECT_FALSE(strSmall == strLarge);
    EXPECT_FALSE(strSmall == intSmall);
    EXPECT_FALSE(strSmall == boolSmall);
    EXPECT_FALSE(strSmall == dblSmall);
    EXPECT_TRUE(boolSmall == boolSmall);
    EXPECT_FALSE(boolSmall == boolLarge);
    EXPECT_FALSE(boolSmall == intSmall);
    EXPECT_FALSE(boolSmall == strSmall);
    EXPECT_FALSE(boolSmall == dblSmall);
    EXPECT_TRUE(dblSmall == dblSmall);
    EXPECT_FALSE(dblSmall == dblLarge);
    EXPECT_FALSE(dblSmall == intSmall);
    EXPECT_FALSE(dblSmall == strSmall);
    EXPECT_FALSE(dblSmall == boolSmall);
}

