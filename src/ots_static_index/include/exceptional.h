#ifndef ALIYUN_OTS_STATIC_INDEX_EXCEPTIONAL_H
#define ALIYUN_OTS_STATIC_INDEX_EXCEPTIONAL_H

#include "ots/ots_types.h"
#include "ots/ots_exception.h"
#include <tr1/tuple>
#include <tr1/memory>
#include <string>
#include <stdexcept>
#include <stdint.h>
#include <cassert>

namespace static_index {

class Exceptional
{
public:
    enum ExceptionalCode
    {
        OTS_OK,
        OTS_CLIENT_EXCEPTION,
        OTS_SERVICE_EXCEPTION,
    };

private:
    ExceptionalCode mCode;
    ::std::string mErrorCode;
    ::std::string mErrorMessage;
    ::std::string mRequestId;

public:
    explicit Exceptional()
      : mCode(OTS_OK)
    {}

    explicit Exceptional(const ::std::string& msg)
      : mCode(OTS_CLIENT_EXCEPTION),
        mErrorCode("ClientParameterError"),
        mErrorMessage(msg)
    {
    }
    explicit Exceptional(const ::aliyun::openservices::ots::OTSException& ex)
    {
        if (ex.GetRequestId().empty()) {
            mCode = OTS_CLIENT_EXCEPTION;
        } else {
            mCode = OTS_SERVICE_EXCEPTION;
            mRequestId = ex.GetRequestId();
        }
        mErrorCode = ex.GetErrorCode();
        mErrorMessage = ex.GetErrorMessage();
    }
    explicit Exceptional(const ::aliyun::openservices::ots::Error& ex)
      : mCode(OTS_SERVICE_EXCEPTION),
        mErrorCode(ex.GetCode()),
        mErrorMessage(ex.GetMessage())
    {}
    explicit Exceptional(
        ExceptionalCode code,
        const ::std::string& errCode,
        const ::std::string& errMsg)
      : mCode(code),
        mErrorCode(errCode),
        mErrorMessage(errMsg)
    {}
    explicit Exceptional(
        ExceptionalCode code,
        const ::std::string& errCode,
        const ::std::string& errMsg,
        const ::std::string& reqId)
      : mCode(code),
        mErrorCode(errCode),
        mErrorMessage(errMsg),
        mRequestId(reqId)
    {}
    
    ExceptionalCode GetCode() const throw()
    {
        return mCode;
    }
    ::std::string GetErrorCode() const throw()
    {
        return mErrorCode;
    }
    ::std::string GetErrorMessage() const throw()
    {
        return mErrorMessage;
    }
    ::std::string GetRequestId() const throw()
    {
        return mRequestId;
    }
};

} // namespace static_index

#define OTS_TRY(x) \
    do {\
        const ::static_index::Exceptional& _ex12345 = (x);                   \
        if (_ex12345.GetCode() != ::static_index::Exceptional::OTS_OK) {      \
            return _ex12345;\
        }\
    } while(false)

#endif /* ALIYUN_OTS_STATIC_INDEX_EXCEPTIONAL_H */

