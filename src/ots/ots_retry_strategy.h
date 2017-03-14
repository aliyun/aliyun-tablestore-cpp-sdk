#ifndef OTS_RETRY_STRATEGY_H
#define OTS_RETRY_STRATEGY_H

#include <string>
#include <tr1/memory>
#include <stdint.h>
#include <sys/types.h>

namespace aliyun {
namespace openservices {
namespace ots {

const int kDefaultRetry = 3;

/**
 * OTSRetryStrategy: the default retry strategy for OTS.
 */
class OTSRetryStrategy
{
public:
    OTSRetryStrategy(int maxRetry = kDefaultRetry);

    ~OTSRetryStrategy();

    //For specified errors, it will retry for MaxRetry times.
    bool ShouldRetry(
                const std::string& operation,
                const std::string& code) const;

    //Set the maximum times for retrying
    void SetMaxRetry(int maxRetry);

    //Get the maximum times for retrying
    int GetMaxRetry() const;

private:
    //The maximum time for retrying.
    int mMaxRetry;
};
typedef std::tr1::shared_ptr<OTSRetryStrategy> OTSRetryStrategyPtr;

} //end of ots
} //end of openservices
} //end of aliyun

#endif
