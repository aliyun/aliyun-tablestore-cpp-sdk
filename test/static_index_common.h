#include "ots_static_index/static_index.h"
#include "../src/ots_static_index/bulk_executor.h"
#include "../src/ots_static_index/type_delegates.h"
#include "../src/ots_static_index/threading.h"
#include "../src/ots_static_index/alarm_clock.h"
#include "../src/ots_static_index/string_tools.h"
#include "../src/ots_static_index/timestamp.h"
#include "../src/ots_static_index/random.h"
#include "../src/ots_static_index/logging_assert.h"
#include "../src/ots_static_index/arithmetic.h"
#include "../src/ots_static_index/security.h"
#include "../src/ots_static_index/foreach.h"
#include "../src/ots/ots_exception_impl.h"
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "jsoncpp/json/json.h"
#include <tr1/tuple>
#include <tr1/memory>
#include <tr1/functional>
#include <map>
#include <vector>
#include <string>
#include <limits>
#include <stdint.h>

template<typename T>
class FlagSetter
{
    T* mVar;
    T mOldVal;
    
public:
    FlagSetter(T& var, const T& newVal)
      : mVar(&var),
        mOldVal(var)
    {
        var = newVal;
    }

    ~FlagSetter()
    {
        *mVar = mOldVal;
    }
};

class DummyLogger : public static_index::Logger
{
public:
    LogLevel GetLogLevel() const;
    void Record(LogLevel, const std::string& rec);
    void Flush();
};

class FileSyncLogger : public static_index::Logger
{
    FILE* mFp;

public:
    explicit FileSyncLogger(const std::string& fn);
    ~FileSyncLogger();

    static_index::Logger::LogLevel GetLogLevel() const;
    void Record(static_index::Logger::LogLevel, const std::string& rec);
    void Flush();
};

class MockClientDelegate: public static_index::ClientDelegate
{
public:
    // MOCK_METHOD2(GetRow,
    //     void(const aliyun::openservices::ots::GetRowRequest&,
    //         aliyun::openservices::ots::GetRowResponse*));

    MOCK_METHOD2(PutRow,
        static_index::Exceptional(
            static_index::PutRowResponse*,
            const static_index::PutRowRequest&));

    // MOCK_METHOD2(UpdateRow,
    //     void(const aliyun::openservices::ots::UpdateRowRequest&,
    //         aliyun::openservices::ots::UpdateRowResponse*));

    // MOCK_METHOD2(DeleteRow,
    //     void(const aliyun::openservices::ots::DeleteRowRequest&,
    //         aliyun::openservices::ots::DeleteRowResponse*));

    MOCK_METHOD2(BatchGetRow,
        static_index::Exceptional(
            static_index::BatchGetRowResponse*,
            const static_index::BatchGetRowRequest&));

    MOCK_METHOD2(BatchWrite,
        static_index::Exceptional(
            static_index::BatchWriteResponse*,
            const static_index::BatchWriteRequest&));

    MOCK_METHOD2(GetRange,
        static_index::Exceptional(
            static_index::GetRangeResponse*,
            const static_index::GetRangeRequest&));
};

Json::Value FromJsonStr(const std::string& str);

static_index::Exceptional MockGetRange(
    static_index::GetRangeResponse* resp,
    const static_index::GetRangeRequest& req,
    static_index::Logger* logger,
    ::std::map<Json::Value, int64_t>* hits,
    const ::std::map<Json::Value, static_index::GetRangeResponse>& resps);
static_index::Exceptional MockBatchWrite(
    static_index::BatchWriteResponse* resp,
    const static_index::BatchWriteRequest& req,
    static_index::Logger* logger,
    ::std::map<Json::Value, int64_t>* hits,
    const ::std::map<Json::Value, static_index::BatchWriteResponse>& resps);
static_index::Exceptional MockBatchGetRow(
    static_index::BatchGetRowResponse* resp,
    const static_index::BatchGetRowRequest& req,
    static_index::Logger* logger,
    ::std::map<Json::Value, int64_t>* hits,
    const ::std::map<Json::Value, static_index::BatchGetRowResponse>& resps);
