#ifndef OTS_EXCEPTION_H
#define OTS_EXCEPTION_H

#include <exception>
#include <string>

namespace aliyun {
namespace openservices {
namespace ots {

/**
 * @class OTSException
 * OTSException用来描述OTS操作相关的异常。所有异常都包括操作名称、错误码和错误
 * 信息。对于由OTS服务抛出的异常，可能会包括请求ID和主机ID。
 */
class OTSException : public std::exception
{
public:
    virtual ~OTSException() throw() = 0;

    /**
     * 获取操作名。
     *
     * @return 返回操作名。
     */
    virtual const std::string& GetOperation() const throw() = 0;

    /**
     * 获取错误码。
     *
     * @return 返回错误码。
     */
    virtual const std::string& GetErrorCode() const throw() = 0;
    
    /**
     * 获取错误信息。
     *
     * @return 返回错误信息。
     */
    virtual const std::string& GetErrorMessage() const throw() = 0;

    /**
     * 获取请求ID。
     *
     * @return 返回请求ID。
     */
    virtual const std::string& GetRequestId() const throw() = 0;

    /**
     * 获取格式化的异常信息。
     *
     * @return 返回格式化信息。
     */
    virtual const char* what() const throw() = 0;
};

} //end of ots
} //end of openservices
} //end of aliyun

#endif
