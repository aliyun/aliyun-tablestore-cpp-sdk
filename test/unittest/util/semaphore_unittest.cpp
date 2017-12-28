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
#include "tablestore/util/threading.hpp"
#include "testa/testa.hpp"
#include <tr1/functional>
#include <string>

using namespace std;
using namespace std::tr1;

namespace aliyun {
namespace tablestore {

namespace {

void run(
    string* log,
    const string& msg,
    util::Semaphore* upstream,
    util::Semaphore* downstream)
{
    if (upstream != NULL) {
        upstream->wait();
    }
    log->append(msg);
    if (downstream != NULL) {
        downstream->post();
    }
}

void Semaphore_chain(const string&)
{
    util::Semaphore start(0);
    util::Semaphore middle(0);
    string log;
    util::Thread t2(
        bind(run, &log, "t2", &middle, (util::Semaphore*) NULL));
    util::Thread t1(
        bind(run, &log, "t1,", &start, &middle));
    start.post();
    t1.join();
    t2.join();

    TESTA_ASSERT(log == "t1,t2")
        (log).issue();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(Semaphore_chain);

namespace {
void Semaphore_PositiveInit(const string&)
{
    util::Semaphore start(1);
    string log;
    util::Thread t(
        bind(run, &log, "t", &start, (util::Semaphore*) NULL));
    t.join();

    TESTA_ASSERT(log == "t")
        (log).issue();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(Semaphore_PositiveInit);

namespace {
void Semaphore_timeout(const string&)
{
    util::Semaphore sem(0);
    util::MonotonicTime start = util::MonotonicTime::now();
    util::Semaphore::Status ret = sem.waitFor(util::Duration::fromMsec(10));
    util::Duration dur = util::MonotonicTime::now() - start;
    TESTA_ASSERT(ret == util::Semaphore::kTimeout)
        (ret).issue();
    TESTA_ASSERT(dur > util::Duration::fromMsec(10))
        (dur).issue();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(Semaphore_timeout);

} // namespace tablestore
} // namespace aliyun
