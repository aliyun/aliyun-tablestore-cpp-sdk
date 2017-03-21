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
#include "util/optional.hpp"
#include "testa/testa.hpp"
#include "screen_logger.hpp"
#include <tr1/functional>
#include <deque>
#include <string>

using namespace std;
using namespace std::tr1;
using namespace std::tr1::placeholders;

namespace aliyun {
namespace tablestore {

void Optional_transfer()
{
    deque<int> xs;
    xs.push_back(1);
    util::Optional<deque<int> > opCopy(xs);
    TESTA_ASSERT(pp::prettyPrint(xs) == "[1]" && pp::prettyPrint(*opCopy) == "[1]")
        (xs)
        (*opCopy)
        .issue();

    util::Optional<deque<int> > opMove(util::move(xs));
    TESTA_ASSERT(pp::prettyPrint(xs) == "[]" && pp::prettyPrint(*opMove) == "[1]")
        (xs)
        (*opMove)
        .issue();
}
TESTA_DEF_JUNIT_LIKE1(Optional_transfer);

namespace {

int inc(int x)
{
    return x + 1;
}

} // namespace

void Optional_apply()
{
    {
        util::Optional<int> in;
        util::Optional<int> res = in.apply(inc).apply(inc);
        TESTA_ASSERT(!res.present()).issue();
    }
    {
        function<int(int)> xinc = bind(inc, _1);
        util::Optional<int> in(0);
        util::Optional<int> res = in.apply(xinc).apply(xinc);
        TESTA_ASSERT(*res == 2)
            (*res)
            .issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(Optional_apply);

} // namespace tablestore
} // namespace aliyun
