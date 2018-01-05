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
#include "tablestore/util/timestamp.hpp"
#include <boost/random/mersenne_twister.hpp>
#include <string>
#include <deque>

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
    {
        init();
    }
    explicit Default()
      : mSeed(UtcTime::now().toUsec())
    {
        init();
    }

    uint64_t upperBound() const
    {
        uint64_t up = mRng.max();
        return up + 1;
    }

    uint64_t next()
    {
        return mRng();
    }

    uint64_t seed() const
    {
        return mSeed;
    }

private:
    void init()
    {
        mRng.seed(static_cast<uint32_t>(mSeed));
    }

private:
    uint64_t mSeed;
    boost::mt19937 mRng;
};

uint64_t nextUint(Random& rng, uint64_t upper)
{
    const uint64_t bound = rng.upperBound();
    if (upper <= bound) {
        return rng.next() % upper;
    } else {
        uint64_t oldUpper = upper;
        deque<uint64_t> segs;
        for(; upper > bound; upper /= bound) {
            segs.push_back(rng.next());
        }
        segs.push_back(rng.next() % upper);
        uint64_t res = 0;
        for(; !segs.empty(); segs.pop_back()) {
            res *= bound;
            res += segs.back();
        }
        return res % oldUpper;
    }
}

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
    return nextUint(rng, exclusiveUpper);
}

int64_t nextInt(Random& rng, int64_t inclusiveLower, int64_t exclusiveUpper)
{
    uint64_t range = exclusiveUpper;
    if (inclusiveLower >= 0) {
        range -= inclusiveLower;
    } else {
        range += static_cast<uint64_t>(-inclusiveLower);
    }
    OTS_ASSERT(range > 0)(range);
    int64_t res = nextUint(rng, range);
    res += inclusiveLower;
    return res;
}

} // namespace random
} // namespace util
} // namespace tablestore
} // namespace aliyun
