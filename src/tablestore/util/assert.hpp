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
#ifndef TABLESTORE_UTIL_ASSERT_HPP
#define TABLESTORE_UTIL_ASSERT_HPP

#include "assert.ipp"

#define OTS_LIKELY(x)   __builtin_expect(!!(x), 1)

#define OTS_ASSERT_PINGPONG_A(x)                        \
    OTS_ASSERT_PINGPONG_OP(x, B)
#define OTS_ASSERT_PINGPONG_B(x) \
    OTS_ASSERT_PINGPONG_OP(x, A)
#define OTS_ASSERT_PINGPONG_OP(x, next) \
    append(#x, pp::prettyPrint(x)). OTS_ASSERT_PINGPONG_##next

#define OTS_ASSERT(cond)                                        \
    if (OTS_LIKELY(cond)) {}                                            \
    else aliyun::tablestore::util::impl::AssertHelper(__FILE__, __LINE__, __func__) \
             .append("Condition", #cond). OTS_ASSERT_PINGPONG_A

#endif /* TABLESTORE_UTIL_ASSERT_HPP */
