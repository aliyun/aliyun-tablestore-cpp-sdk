#include "ots_exception_impl.h"

#include <vector>
#include <stdarg.h>

using namespace std;

namespace aliyun {
namespace openservices {
namespace ots {

OTSException::~OTSException() throw () {}

OTSExceptionImpl::OTSExceptionImpl(const char* operation, ...) throw()
{
    int argCount = 1;
    vector<string> args;
    args.push_back(operation);

    va_list vaList;
    va_start(vaList, operation);
    do {
        const char* tmpValue = va_arg(vaList, const char*);  
        if (*tmpValue == 0) {
            break; 
        }
        args.push_back(tmpValue);
        ++argCount;
    } while (argCount < 5);
    va_end(vaList);

    // get arguments
    for (size_t i = 0; i < args.size(); ++i) {
        switch (i) {
        case 0:
            mOperation = args[i];
            break;
        case 1:
            mErrorCode = args[i];
            break;
        case 2:
            mErrorMessage = args[i];
            break;
        case 3:
            mRequestId = args[i];
            break;
        case 4:
            mHttpStatus = args[i];
            break;
        } 
    }

    // construct the strin for what()
    mWhat = "Operation: " + mOperation + "\nErrorCode: " + mErrorCode + \
             "\nErrorMessage: " + mErrorMessage;
    if (mRequestId != "") {
        mWhat += "\nRequestId: " + mRequestId; 
    }
    if (mHttpStatus != "") {
        mWhat += "\nHttpStatus: " + mHttpStatus;
    }
}

OTSExceptionImpl::~OTSExceptionImpl() throw()
{
    //Nothing to cleanup
}

const std::string& OTSExceptionImpl::GetOperation() const throw()
{
    return mOperation;
}

const std::string& OTSExceptionImpl::GetErrorCode() const throw()
{
    return mErrorCode;
}

const std::string& OTSExceptionImpl::GetErrorMessage() const throw()
{
    return mErrorMessage;
}

const std::string& OTSExceptionImpl::GetRequestId() const throw()
{
    return mRequestId;
}

const std::string& OTSExceptionImpl::GetHttpStatus() const throw()
{
    return mHttpStatus;
}

const char* OTSExceptionImpl::what() const throw()
{
    return mWhat.c_str();
}

} //end of ots
} //end of openservices
} //end of aliyun
