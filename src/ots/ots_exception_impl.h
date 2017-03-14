#ifndef OTS_EXCEPTION_IMPL_H
#define OTS_EXCEPTION_IMPL_H

#include "include/ots_exception.h"
#include <string>

#define OTS_THROW(op, ...)                                  \
    do {                                                    \
        OTSExceptionImpl tmpException(op, __VA_ARGS__, ""); \
        throw tmpException;                                 \
    } while (false)


namespace aliyun {
namespace openservices {
namespace ots {

class OTSExceptionImpl : public OTSException
{
public:
    OTSExceptionImpl(const char* operation, ...) throw();

    virtual ~OTSExceptionImpl() throw();

    /**
     * 获取操作名。
     *
     * @return 返回操作名。
     */
    virtual const std::string& GetOperation() const throw();

    /**
     * 获取错误码。
     *
     * @return 返回错误码。
     */
    virtual const std::string& GetErrorCode() const throw();
    
    /**
     * 获取错误信息。
     *
     * @return 返回错误信息。
     */
    virtual const std::string& GetErrorMessage() const throw();

    /**
     * 获取请求ID。
     *
     * @return 返回请求ID。
     */
    virtual const std::string& GetRequestId() const throw();

    /**
     * 获取请求Http Code。
     *
     * @return 返回请求Http Code。
     */
    virtual const std::string& GetHttpStatus() const throw();

    /**
     * 获取格式化的异常信息。
     *
     * @return 返回格式化信息。
     */
    virtual const char* what() const throw();

private:
    std::string mOperation;
    std::string mErrorCode;
    std::string mErrorMessage;
    std::string mRequestId;
    std::string mHttpStatus;
    std::string mWhat;
};

} //end of ots
} //end of openservices
} //end of aliyun

#endif
