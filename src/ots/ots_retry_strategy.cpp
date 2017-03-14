#include "ots_retry_strategy.h"

using namespace std;
using namespace aliyun::openservices::ots;

OTSRetryStrategy::OTSRetryStrategy(int maxRetry) 
: mMaxRetry(maxRetry) 
{
    //Nothing to do
} 

OTSRetryStrategy::~OTSRetryStrategy()
{
    //Nothing to do
}

bool OTSRetryStrategy::ShouldRetry(const string& operation, const string& code) const
{
    if (code == "OTSServiceError" || code == "OTSNetworkError") {
        if (operation == "GetRow" || operation == "GetRowsByRange" ||
            operation == "MultiGetRow" || operation == "ListTable" ||
            operation == "ListTableGroup" || operation == "GetTableMeta") {
            return true; 
        }
    }
    return false;
}

void OTSRetryStrategy::SetMaxRetry(int maxRetry)
{
    mMaxRetry = maxRetry;
}

int OTSRetryStrategy::GetMaxRetry() const
{
    return mMaxRetry;
}
