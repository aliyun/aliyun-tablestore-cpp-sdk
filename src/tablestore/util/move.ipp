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

#include "tablestore/util/metaprogramming.hpp"
#include <tr1/type_traits>
#include <tr1/memory>
#include <string>

namespace aliyun {
namespace tablestore {
namespace util {
namespace impl {

struct ClearSwapable {};
struct Copyable {};

template<class T>
struct MoveCategory<T, typename mp::EnableIf<std::tr1::is_arithmetic<T>::value, void>::Type>
{
    typedef Copyable Category;
};
template<class T>
struct MoveCategory<T, typename mp::EnableIf<std::tr1::is_enum<T>::value, void>::Type>
{
    typedef Copyable Category;
};

template<class T>
struct MoveCategory<T, typename mp::EnableIfExists<typename T::value_type, void>::Type>
{
    typedef ClearSwapable Category;
};

struct SharedPtr {};

template<class T>
struct MoveCategory<T, typename mp::EnableIf<std::tr1::is_same<T, std::tr1::shared_ptr<typename T::element_type> >::value, void>::Type>
{
    typedef SharedPtr Category;
};

template<class T>
struct MoveAssignment<Moveable, T>
{
    void operator()(T* to, const MoveHolder<T>& from) const throw()
    {
        *to = from;
    }
};

template<class T>
struct MoveAssignment<ClearSwapable, T>
{
    void operator()(T* to, const MoveHolder<T>& from) const throw()
    {
        to->clear();
        to->swap(*from);
    }
};

template<class T>
struct MoveAssignment<Copyable, T>
{
    void operator()(T* to, const MoveHolder<T>& from) const throw()
    {
        *to = *from;
    }
};

template<class T>
struct MoveAssignment<SharedPtr, T>
{
    void operator()(T* to, const MoveHolder<T>& from) const
    {
        T empty;
        to->swap(empty);
        to->swap(*from);
    }
};

} // namespace impl
} // namespace util
} // namespace tablestore
} // namespace aliyun
