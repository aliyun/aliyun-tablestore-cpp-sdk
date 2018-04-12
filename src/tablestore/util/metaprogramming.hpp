#pragma once
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

#ifndef TABLESTORE_UTIL_METAPROGRAMMING_HPP
#define TABLESTORE_UTIL_METAPROGRAMMING_HPP

#if __cplusplus < 201103L
#include <tr1/type_traits>
#include <stdint.h>
#else
#include <type_traits>
#include <cstdint>
#endif

namespace mp {

template<class T> class TellType;

template<class Enable>
struct VoidIfExists
{
    typedef void Type;
};

template<bool enable>
struct VoidIf;
template<>
struct VoidIf<true>
{
    typedef void Type;
};

template<class T>
struct IsInteger
{
    static const bool value = false;
};
template<>
struct IsInteger<int8_t>
{
    static const bool value = true;
};
template<>
struct IsInteger<uint8_t>
{
    static const bool value = true;
};
template<>
struct IsInteger<int16_t>
{
    static const bool value = true;
};
template<>
struct IsInteger<uint16_t>
{
    static const bool value = true;
};
template<>
struct IsInteger<int32_t>
{
    static const bool value = true;
};
template<>
struct IsInteger<uint32_t>
{
    static const bool value = true;
};
template<>
struct IsInteger<int64_t>
{
    static const bool value = true;
};
template<>
struct IsInteger<uint64_t>
{
    static const bool value = true;
};


template<class T>
#if __cplusplus < 201103L
struct IsScalar: public std::tr1::is_scalar<T> {};
#else
struct IsScalar: public std::is_scalar<T> {};
#endif

template<class T>
#if __cplusplus < 201103L
struct IsSigned: public std::tr1::is_signed<T> {};
#else
struct IsSigned: public std::is_signed<T> {};
#endif

template<class T>
#if __cplusplus < 201103L
struct IsUnsigned: public std::tr1::is_unsigned<T> {};
#else
struct IsUnsigned: public std::is_unsigned<T> {};
#endif

#if __cplusplus < 201103L
template<class T>
struct MakeUnsigned;

template<>
struct MakeUnsigned<int8_t>
{
    typedef uint8_t Type;
};
template<>
struct MakeUnsigned<int16_t>
{
    typedef uint16_t Type;
};
template<>
struct MakeUnsigned<int32_t>
{
    typedef uint32_t Type;
};
template<>
struct MakeUnsigned<int64_t>
{
    typedef uint64_t Type;
};

#else

template<class T>
struct MakeUnsigned
{
    typedef typename std::make_unsigned<T>::type Type;
};
#endif

template<class T0, class T1>
#if __cplusplus < 201103L
struct IsSame: public std::tr1::is_same<T0, T1> {};
#else
struct IsSame: public std::is_same<T0, T1> {};
#endif

template<class T>
#if __cplusplus < 201103L
struct IsFloatingPoint: public std::tr1::is_floating_point<T> {};
#else
struct IsFloatingPoint: public std::is_floating_point<T> {};
#endif

template<class T>
struct RemoveCvref
{
#if __cplusplus < 201103L
    typedef typename std::tr1::remove_cv<
        typename std::tr1::remove_reference<T>::type>::type Type;
#else
    typedef typename std::remove_cv<
        typename std::remove_reference<T>::type>::type Type;
#endif
};

template<class T, class E=void>
struct IsSmartPtr
{
    static const bool value = false;
};

template<class T>
struct IsSmartPtr<
    T,
    typename VoidIfExists<typename T::element_type>::Type>
{
    static const bool value = true;
};

template<class T, class E=void>
struct IsSeq
{
    static const bool value = false;
};
template<class T>
struct IsSeq<
    T,
    typename mp::VoidIfExists<typename T::const_iterator>::Type>
{
    static const bool value = true;
};

template<class T, class E=void>
struct IsAssociative
{
    static const bool value = false;
};
template<class T>
struct IsAssociative<
    T,
    typename mp::VoidIfExists<typename T::mapped_type>::Type>
{
    static const bool value = true;
};



} // namespace mp
#endif
