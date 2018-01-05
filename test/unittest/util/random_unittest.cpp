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
#include "tablestore/util/random.hpp"
#include "tablestore/util/foreach.hpp"
#include "testa/testa.hpp"
#include <tr1/tuple>
#include <iostream>
#include <memory>
#include <deque>
#include <string>
#include <limits>
#include <cstdio>
#include <stdint.h>

using namespace std;
using namespace std::tr1;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {

namespace {

void genSamples(random::Random& rng, deque<uint64_t>& samples, const uint64_t bits)
{
    for(int64_t i = 0; i < 20000; ++i) {
        samples.push_back(random::nextInt(rng, 1ULL << bits));
    }
}

void initCounts(const uint64_t bits, deque<int64_t>& counts1d, deque<deque<int64_t> >& counts2d)
{
    counts1d.clear();
    for(uint64_t i = 0; i < bits; ++i) {
        counts1d.push_back(0);
    }

    counts2d.clear();
    for(uint64_t i = 0; i < bits; ++i) {
        counts2d.push_back(deque<int64_t>());
        deque<int64_t>& last = counts2d.back();
        for(uint64_t j = 0; j < bits; ++j) {
            last.push_back(0);
        }
    }
}

void count1d(
    deque<int64_t>& counts1d,
    const uint64_t bits,
    const deque<uint64_t>& samples)
{
    FOREACH_ITER(i, samples) {
        uint64_t x = *i;
        for(uint64_t j = 0; j < bits; ++j) {
            if (x & (1ULL << j)) {
                ++counts1d[j];
            }
        }
    }
}

void count2d(
    deque<deque<int64_t> >& counts2d,
    const uint64_t bits,
    const deque<uint64_t>& samples)
{
    FOREACH_ITER(i, samples) {
        uint64_t x = *i;
        for(uint64_t j = 0; j < bits; ++j) {
            if (x & (1ULL << j)) {
                for(uint64_t k = j + 1; k < bits; ++k) {
                    if (x & (1ULL << k)) {
                        ++counts2d[j][k];
                    }
                }
            }
        }
    }
}

void check1d(
    const uint64_t bits,
    const uint64_t sampleSize,
    const deque<int64_t>& counts1d)
{
    double avg = 1.0 * sampleSize / 2;
    int64_t lower = 0.95 * avg;
    int64_t upper = 1.05 * avg;
    for(uint64_t i = 0; i < bits; ++i) {
        int64_t x = counts1d[i];
        TESTA_ASSERT(x >= lower && x <= upper)
            (x)
            (lower)
            (upper)
            (i)
            .issue();
    }
}

void check2d(
    const uint64_t bits,
    const uint64_t sampleSize,
    const deque<deque<int64_t> >& counts2d)
{
    double avg = 1.0 * sampleSize / 4;
    int64_t lower = 0.95 * avg;
    int64_t upper = 1.05 * avg;
    for(uint64_t i = 0; i < bits; ++i) {
        for(uint64_t j = i + 1; j < bits; ++j) {
            int64_t x = counts2d[i][j];
            TESTA_ASSERT(x >= lower && x <= upper)
                (x)
                (lower)
                (upper)
                (i)
                (j)
                .issue();
        }
    }
}

void Random_Small(const string&)
{
    const uint64_t kBits = 8;
    auto_ptr<random::Random> rng(random::newDefault());
    deque<uint64_t> samples;
    genSamples(*rng, samples, kBits);
    deque<int64_t> counts1d;
    deque<deque<int64_t> > counts2d;
    initCounts(kBits, counts1d, counts2d);

    count1d(counts1d, kBits, samples);
    count2d(counts2d, kBits, samples);

    check1d(kBits, samples.size(), counts1d);
    check2d(kBits, samples.size(), counts2d);
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(Random_Small);

namespace {
void Random_Large(const string&)
{
    const uint64_t kBits = 48;
    auto_ptr<random::Random> rng(random::newDefault());
    cerr << rng->upperBound() << endl;
    deque<uint64_t> samples;
    genSamples(*rng, samples, kBits);
    deque<int64_t> counts1d;
    deque<deque<int64_t> > counts2d;
    initCounts(kBits, counts1d, counts2d);

    count1d(counts1d, kBits, samples);
    count2d(counts2d, kBits, samples);

    check1d(kBits, samples.size(), counts1d);
    check2d(kBits, samples.size(), counts2d);
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(Random_Large);

} // namespace tablestore
} // namespace aliyun
