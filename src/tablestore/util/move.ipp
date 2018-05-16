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
#include <memory>
#include <string>

namespace aliyun {
namespace tablestore {
namespace util {
namespace impl {

struct Copyable {};
struct SmartPtr {};
struct ClearSwapable {};
struct Function {};

template<class T>
struct MoveCategory<
    T,
    typename mp::VoidIf<mp::IsScalar<T>::value>::Type>
{
    typedef Copyable Category;
};

template<class T, void (T::*)()>
struct _IsClear
{
    typedef void Type;
};
template<class T, class E = void>
struct HasClear
{
    static const bool value = false;
};
template<class T>
struct HasClear<T, typename _IsClear<T, &T::clear>::Type>
{
    static const bool value = true;
};

template<class T, void (T::*)(T&)>
struct _IsSwap
{
    typedef void Type;
};
template<class T, class E = void>
struct HasSwap
{
    static const bool value = false;
};
template<class T>
struct HasSwap<T, typename _IsSwap<T, &T::swap>::Type>
{
    static const bool value = true;
};

template<class T>
struct MoveCategory<
    T,
    typename mp::VoidIf<HasClear<T>::value && HasSwap<T>::value>::Type>
{
    typedef ClearSwapable Category;
};

template<class T>
struct MoveCategory<
    T,
    typename mp::VoidIf<mp::IsSmartPtr<T>::value>::Type>
{
    typedef SmartPtr Category;
};

template<class T>
struct MoveCategory<
    T,
    typename mp::VoidIfExists<typename T::result_type>::Type>
{
    typedef Function Category;
};

template<class T>
struct MoveAssign<Moveable, T>
{
    void operator()(T& to, const MoveHolder<T>& from) const throw()
    {
        to = from;
    }
};

template<class T>
struct MoveAssign<ClearSwapable, T>
{
    void operator()(T& to, const MoveHolder<T>& from) const throw()
    {
        to.clear();
        to.swap(*from);
    }
};

template<class T>
struct MoveAssign<Copyable, T>
{
    void operator()(T& to, const MoveHolder<T>& from) const throw()
    {
        to = *from;
    }
};

template<class T>
struct MoveAssign<SmartPtr, T>
{
    void operator()(T& to, const MoveHolder<T>& from) const
    {
        if (to.get() != from->get()) {
            to = *from;
            from->reset();
        }
    }
};

template<class T>
struct MoveAssign<Function, T>
{
    void operator()(T& to, const MoveHolder<T>& from) const
    {
        std::swap(to, *from);
        *from = T();
    }
};

} // namespace impl
} // namespace util
} // namespace tablestore
} // namespace aliyun
