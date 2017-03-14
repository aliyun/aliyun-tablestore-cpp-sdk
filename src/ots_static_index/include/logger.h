#ifndef ALIYUN_OTS_STATIC_INDEX_LOGGER_H
#define ALIYUN_OTS_STATIC_INDEX_LOGGER_H

#include <string>

namespace static_index {

class Logger
{
public:
    enum LogLevel
    {
        DEBUG,
        INFO,
        ERROR,
    };
    
    virtual ~Logger() {}

    /**
     * 当前使用的日志等级
     */
    virtual LogLevel GetLogLevel() const =0;

    /**
     * 记录一条日志
     */
    virtual void Record(LogLevel, const ::std::string&) =0;

    /**
     * 将之前的日志刷入磁盘
     */
    virtual void Flush() =0;
};

} // namespace static_index

#endif /* ALIYUN_OTS_STATIC_INDEX_LOGGER_H */
