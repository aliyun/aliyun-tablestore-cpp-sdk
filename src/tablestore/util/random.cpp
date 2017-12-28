/*
BSD 3-Clause License

Copyright (c) 2017, Alibaba Cloud
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
#include "random.hpp"
#include "tablestore/util/assert.hpp"
#include <string>
#include <cstdlib>
#include <cstdio>
#include <cerrno>
#include <cstring>
extern "C" {
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
}

using namespace std;

namespace aliyun {
namespace tablestore {
namespace util {
namespace random {

namespace {

class Default : public Random
{
public:
    explicit Default(unsigned int seed)
      : mSeed(seed)
    {}
    explicit Default()
      : mSeed(0)
    {
        int fd = open("/dev/urandom", O_RDONLY);
        OTS_ASSERT(fd >= 0)(errno)(string(strerror(errno)));
        {
            int64_t ret = read(fd, &mSeed, sizeof(mSeed));
            OTS_ASSERT(ret >= 0)(errno)(string(strerror(errno)));
        }
        {
            int ret = close(fd);
            OTS_ASSERT(ret == 0)(errno)(string(strerror(errno)));
        }
    }

    int64_t upperBound() const
    {
        return RAND_MAX;
    }

    uint64_t seed() const
    {
        return mSeed;
    }

    int64_t next()
    {
        return rand_r(&mSeed);
    }

private:
    unsigned int mSeed;
};

} // namespace

Random* newDefault()
{
    return new Default();
}

Random* newDefault(uint64_t seed)
{
    return new Default(seed);
}

int64_t nextInt(Random& rng, int64_t exclusiveUpper)
{
    OTS_ASSERT(exclusiveUpper > 0)(exclusiveUpper);
    return rng.next() % exclusiveUpper;
}

int64_t nextInt(Random& rng, int64_t inclusiveLower, int64_t exclusiveUpper)
{
    int64_t range = exclusiveUpper - inclusiveLower;
    OTS_ASSERT(range > 0)(range);
    return inclusiveLower + rng.next() % range;
}

} // namespace random
} // namespace util
} // namespace tablestore
} // namespace aliyun
