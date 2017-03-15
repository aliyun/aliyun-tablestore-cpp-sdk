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

#include "util/iterator.hpp"
#include "testa/testa.hpp"
#include <tr1/type_traits>
#include <deque>
#include <iostream>

using namespace std;
using namespace std::tr1;

namespace aliyun {
namespace tablestore {

void StlContainerIterator(const string&)
{
    TESTA_ASSERT(is_const<remove_reference<util::StlContainerIterator<const deque<int> >::ElemType>::type>::value).issue();
    TESTA_ASSERT(is_const<remove_reference<util::StlContainerIterator<const deque<int>&>::ElemType>::type>::value).issue();
    TESTA_ASSERT(!is_const<remove_reference<util::StlContainerIterator<deque<int> >::ElemType>::type>::value).issue();
    TESTA_ASSERT(!is_const<remove_reference<util::StlContainerIterator<deque<int>&>::ElemType>::type>::value).issue();

    deque<int> xs;
    xs.push_back(1);

    util::StlContainerIterator<deque<int> > it = util::iterate(xs);
    TESTA_ASSERT(it.valid()).issue();
    TESTA_ASSERT(it.get() == 1)
        (it.get())
        .issue();
    it.get() = 2;
    TESTA_ASSERT(it.get() == 2)
        (it.get())
        .issue();
    
    it.moveNext();
    TESTA_ASSERT(!it.valid()).issue();
}
TESTA_DEF_JUNIT_LIKE1(StlContainerIterator);

} // namespace tablestore
} // namespace aliyun
