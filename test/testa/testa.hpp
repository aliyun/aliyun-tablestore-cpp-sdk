/*
This file is picked from project testa [https://github.com/TimeExceed/testa.git]
Copyright (c) 2017, Taoda (tyf00@aliyun.com)
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice, this
  list of conditions and the following disclaimer in the documentation and/or
  other materials provided with the distribution.

* Neither the name of the {organization} nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#ifndef TESTA_HPP
#define TESTA_HPP

#include "testa.ipp"

#define TESTA_PINGPONG_A(x) \
    TESTA_PINGPONG_OP(x, B)
#define TESTA_PINGPONG_B(x) \
    TESTA_PINGPONG_OP(x, A)
#define TESTA_PINGPONG_OP(x, next) \
    append(#x, pp::prettyPrint(x)). TESTA_PINGPONG_##next

#define TESTA_ASSERT(cond) \
    if (cond) {\
        ::testa::CaseFailIssuer(true, #cond, __FILE__, __LINE__).issue(); \
    } else ::testa::CaseFailIssuer(false, #cond, __FILE__, __LINE__). TESTA_PINGPONG_A

#define TESTA_DEF_EQ_WITH_TB(caseName, caseTb, trialFn, oracleFn) \
    testa::EqCase cs##caseName(#caseName, (caseTb), (trialFn), (oracleFn))

#define TESTA_DEF_EQ_1(caseName, trialFn, oracleFn, in0)                 \
    testa::EqCase cs##caseName(#caseName, (trialFn), (oracleFn), (in0))

#define TESTA_DEF_EQ_2(caseName, trialFn, oracleFn, in0, in1)        \
    testa::EqCase cs##caseName(#caseName, (trialFn), (oracleFn), (in0), (in1))

#define TESTA_DEF_EQ_3(caseName, trialFn, oracleFn, in0, in1, in2)       \
    testa::EqCase cs##caseName(#caseName, (trialFn), (oracleFn), \
        (in0), (in1), (in2))

#define TESTA_DEF_VERIFY_WITH_TB(caseName, caseTb, caseVerfier, trialFn) \
    testa::VerifyCase cs##caseName(#caseName, (caseTb), (caseVerfier), (trialFn))

#define TESTA_DEF_JUNIT_LIKE2(caseName, tbVerifier) \
    testa::VerifyCase cs##caseName(#caseName, tbVerifier)

#define TESTA_DEF_JUNIT_LIKE1(caseName) \
    TESTA_DEF_JUNIT_LIKE2(caseName, caseName)

#endif /* TESTA_HPP */
