#include "random.h"
#include <cstdlib>
#include <cassert>
#include <cerrno>
#include <cstring>
extern "C" {
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
}

namespace static_index {

namespace {

class Default : public IRandom
{
    Logger* mLogger;
    unsigned int mSeed;

public:
    explicit Default(Logger* logger, uint64_t x)
      : mLogger(logger),
        mSeed(x)
    {}
    explicit Default(Logger* logger)
      : mLogger(logger),
        mSeed(0)
    {
        int fd = open("/dev/urandom", O_RDONLY);
        OTS_ASSERT(mLogger, fd >= 0)(errno)(strerror(errno));
        {
            int64_t ret = read(fd, &mSeed, sizeof(mSeed));
            OTS_ASSERT(mLogger, ret >= 0)(errno)(strerror(errno));
        }
        {
            int ret = close(fd);
            OTS_ASSERT(mLogger, ret == 0)(errno)(strerror(errno));
        }
    }

    int64_t Max() const
    {
        return RAND_MAX;
    }

    int64_t Seed() const
    {
        return mSeed;
    }

    int64_t Next()
    {
        return rand_r(&mSeed);
    }

    Logger* GetLogger()
    {
        return mLogger;
    }
};

} // namespace

IRandom* NewDefaultRandom(Logger* logger)
{
    return new Default(logger);
}

IRandom* NewDefaultRandom(Logger* logger, uint64_t seed)
{
    return new Default(logger, seed);
}

int64_t NextInt(IRandom* rnd, int64_t exclusiveUpper)
{
    OTS_ASSERT(rnd->GetLogger(), exclusiveUpper > 0)(exclusiveUpper);
    return rnd->Next() % exclusiveUpper;
}

int64_t NextInt(IRandom* rnd, int64_t inclusiveUpper, int64_t exclusiveUpper)
{
    int64_t rng = exclusiveUpper - inclusiveUpper;
    OTS_ASSERT(rnd->GetLogger(), rng > 0)(rng);
    return inclusiveUpper + rnd->Next() % rng;
}

} // namespace static_index

