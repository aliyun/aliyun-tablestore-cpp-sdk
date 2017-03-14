#ifndef OTS_STATIC_INDEX_LOGGING_ASSERT_H
#define OTS_STATIC_INDEX_LOGGING_ASSERT_H

#include "ots_static_index/logger.h"
#include "string_tools.h"
#include <utility>
#include <string>
#include <vector>

namespace static_index {

namespace details {

class LogHelper
{
public:
    /**
     * Ender of macro expansion
     */
    LogHelper& OTS_LOG_HELPER_A;
    LogHelper& OTS_LOG_HELPER_B;

    LogHelper(
        Logger::LogLevel lvl,
        Logger* logger,
        bool assert,
        const char* fn,
        int line,
        const char* func)
      : OTS_LOG_HELPER_A(*this),
        OTS_LOG_HELPER_B(*this),
        mLevel(lvl),
        mLogger(logger),
        mAssert(assert),
        mFile(fn),
        mLine(ToString(line)),
        mFunc(func)
    {
    }
    ~LogHelper();

    template<class T>
    LogHelper& Show(const char* msg, const T& x)
    {
        mValues.push_back(::std::make_pair(::std::string(msg), ToString(x)));
        return *this;
    }

    void What(const ::std::string& msg)
    {
        mWhat = msg;
    }

private:
    Logger::LogLevel mLevel;
    Logger* mLogger;
    bool mAssert;
    ::std::vector< ::std::pair< ::std::string, ::std::string> > mValues;
    ::std::string mWhat;
    ::std::string mFile;
    ::std::string mLine;
    ::std::string mFunc;
};

} // namespace details

class Loggable
{
public:
    virtual ~Loggable() {}

    virtual Logger* GetLogger() =0;
};

} // namespace static_index

#define OTS_LIKELY(x)   __builtin_expect(!!(x), 1)

#define OTS_LOG_HELPER_A(x) \
    OTS_LOG_HELPER_OP(x, B)
#define OTS_LOG_HELPER_B(x) \
    OTS_LOG_HELPER_OP(x, A)
#define OTS_LOG_HELPER_OP(x, next) \
    Show(#x, (x)). OTS_LOG_HELPER_##next

#define OTS_LOG(logger, level) \
    if ((logger)->GetLogLevel() > (level)) {}                \
    else ::static_index::details                    \
               ::LogHelper((level), logger, false, __FILE__, __LINE__, __func__). OTS_LOG_HELPER_A

#define OTS_LOG_DEBUG(logger) \
    OTS_LOG(logger, ::static_index::Logger::DEBUG)

#define OTS_LOG_INFO(logger) \
    OTS_LOG(logger, ::static_index::Logger::INFO)

#define OTS_LOG_ERROR(logger) \
    OTS_LOG(logger, ::static_index::Logger::ERROR)

#define OTS_ASSERT(logger, cond)                                  \
    if (OTS_LIKELY(cond)) {}                                            \
    else ::static_index::details                    \
               ::LogHelper(::static_index::Logger::ERROR, logger, true, __FILE__, __LINE__, __func__) \
               .Show("Condition", #cond). OTS_LOG_HELPER_A

#endif /* OTS_STATIC_INDEX_LOGGING_ASSERT_H */
