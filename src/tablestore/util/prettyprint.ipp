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

#include "metaprogramming.hpp"

#if __cplusplus < 201103L
#include <tr1/type_traits>
#include <tr1/tuple>
#include <stdint.h>
#else
#include <type_traits>
#include <tuple>
#include <cstdint>
#endif

namespace pp {
namespace impl {

struct SignedInteger
{
    static void prettyPrint(std::string&, int64_t);
};
struct UnsignedInteger
{
    static void prettyPrint(std::string&, uint64_t);
};
struct Boolean
{
    static void prettyPrint(std::string&, bool);
};
struct Character
{
    static void prettyPrint(std::string&, char);
};

template<class T>
#if __cplusplus < 201103L
struct PrettyPrinterCategory<T, typename mp::EnableIf<std::tr1::is_integral<T>::value, void>::Type>
#else
struct PrettyPrinterCategory<T, typename mp::EnableIf<std::is_integral<T>::value, void>::Type>
#endif
{
    template<class T0, class Enable = void>
    struct Tester
    {
        typedef UnsignedInteger Category;
    };
    template<class T0>
#if __cplusplus < 201103L
    struct Tester<T0, typename mp::EnableIf<std::tr1::is_signed<T0>::value, void>::Type>
#else
    struct Tester<T0, typename mp::EnableIf<std::is_signed<T0>::value && !std::is_same<T0, char>::value, void>::Type>
#endif
    {
        typedef SignedInteger Category;
    };
    template<class T0>
#if __cplusplus < 201103L
    struct Tester<T0, typename mp::EnableIf<std::tr1::is_same<T0, bool>::value, void>::Type>
#else
    struct Tester<T0, typename mp::EnableIf<std::is_same<T0, bool>::value, void>::Type>
#endif
    {
        typedef Boolean Category;
    };
    template<class T0>
#if __cplusplus < 201103L
    struct Tester<T0, typename mp::EnableIf<std::tr1::is_same<T0, char>::value, void>::Type>
#else
    struct Tester<T0, typename mp::EnableIf<std::is_same<T0, char>::value, void>::Type>
#endif
    {
        typedef Character Category;
    };

    typedef typename Tester<T>::Category Category;
};

template<class T>
struct PrettyPrinter<SignedInteger, T>
{
    void operator()(std::string& out, T x) const
    {
        SignedInteger::prettyPrint(out, x);
    }
};
template<class T>
struct PrettyPrinter<UnsignedInteger, T>
{
    void operator()(std::string& out, T x) const
    {
        UnsignedInteger::prettyPrint(out, x);
    }
};
template<class T>
struct PrettyPrinter<Boolean, T>
{
    void operator()(std::string& out, T x) const
    {
        Boolean::prettyPrint(out, x);
    }
};
template<class T>
struct PrettyPrinter<Character, T>
{
    void operator()(std::string& out, T x) const
    {
        out.push_back('\'');
        Character::prettyPrint(out, x);
        out.push_back('\'');
    }
};


struct Floating
{
    static void prettyPrint(std::string&, double);
};

template<class T>
#if __cplusplus < 201103L
struct PrettyPrinterCategory<T, typename mp::EnableIf<std::tr1::is_floating_point<T>::value, void>::Type>
#else
struct PrettyPrinterCategory<T, typename mp::EnableIf<std::is_floating_point<T>::value, void>::Type>
#endif
{
    typedef Floating Category;
};

template<class T>
struct PrettyPrinter<Floating, T>
{
    void operator()(std::string& out, T x) const
    {
        Floating::prettyPrint(out, x);
    }
};


class StlStr {};
class StlSeq {};
class StlMap {};

template<class T>
struct PrettyPrinterCategory<T, typename mp::EnableIfExists<typename T::value_type, void>::Type>
{
    template<class T0, class Enable = void>
    struct SeqMap
    {
        typedef StlSeq Category;
    };
    template<class T0>
    struct SeqMap<T0, typename mp::EnableIfExists<typename T0::key_type, void>::Type>
    {
        template<class T1, class Enable = void>
        struct SetDetecter
        {
            typedef StlSeq Category;
        };
        template<class T1>
        struct SetDetecter<T1, typename mp::EnableIfExists<typename T1::first_type, void>::Type>
        {
            typedef StlMap Category;
        };

        typedef typename SetDetecter<T0>::Category Category;
    };
    template<class T0>
#if __cplusplus < 201103L
    struct SeqMap<T0, typename mp::EnableIf<std::tr1::is_same<T0, std::string>::value, void>::Type>
#else
    struct SeqMap<T0, typename mp::EnableIf<std::is_same<T0, std::string>::value, void>::Type>
#endif
    {
        typedef StlStr Category;
    };

    typedef typename SeqMap<T>::Category Category;
};

template<class T>
struct PrettyPrinter<StlSeq, T>
{
    void operator()(std::string& out, const T& xs) const
    {
        if (xs.empty()) {
            out.append("[]");
            return;
        }
        out.push_back('[');
        typename T::const_iterator it = xs.begin();
        prettyPrint(out, *it);
        for(++it; it != xs.end(); ++it) {
            out.push_back(',');
            prettyPrint(out, *it);
        }
        out.push_back(']');
    }
};


template<class T>
struct PrettyPrinter<StlMap, T>
{
    void operator()(std::string& out, const T& xs) const
    {
        if (xs.empty()) {
            out.append("{}");
            return;
        }
        out.push_back('{');
        typename T::const_iterator it = xs.begin();
        prettyPrint(out, it->first);
        out.push_back(':');
        prettyPrint(out, it->second);
        for(++it; it != xs.end(); ++it) {
            out.push_back(',');
            prettyPrint(out, it->first);
            out.push_back(':');
            prettyPrint(out, it->second);
        }
        out.push_back('}');
    }
};

template<>
struct PrettyPrinter<StlStr, std::string>
{
    void operator()(std::string& out, const std::string& s) const;
};


class StlSmartPtr {};

template<class T>
struct PrettyPrinterCategory<T, typename mp::EnableIfExists<typename T::element_type, void>::Type>
{
    typedef StlSmartPtr Category;
};

template<class T>
struct PrettyPrinter<StlSmartPtr, T>
{
    void operator()(std::string& out, const T& ptr) const
    {
        if (ptr.get() == NULL) {
            out.append("null");
        } else {
            pp::prettyPrint(out, *ptr);
        }
    }
};


class StlTuple {};

#if __cplusplus < 201103L

template<class T>
struct PrettyPrinterCategory<T, typename mp::EnableIfExists<typename std::tr1::tuple_element<0, T>::type, void>::Type>
{
    typedef StlTuple Category;
};
template<>
struct PrettyPrinterCategory<std::tr1::tuple<>, void>
{
    typedef StlTuple Category;
};

template<class T, int idx, bool stop>
struct TuplePrettyPrinter
{
    void operator()(std::string& out, const T& tp) const
    {
        if (idx > 0) {
            out.push_back(',');
        }
        out.append(prettyPrint(std::tr1::get<idx>(tp)));
        TuplePrettyPrinter<T, idx + 1, idx + 1 >= std::tr1::tuple_size<T>::value> nxt;
        nxt(out, tp);
    }
};

template<class T, int idx>
struct TuplePrettyPrinter<T, idx, true>
{
    void operator()(std::string& out, const T& tp) const
    {}
};

template<class T>
struct PrettyPrinter<StlTuple, T>
{
    void operator()(std::string& out, const T& tp) const
    {
        out.push_back('(');
        TuplePrettyPrinter<T, 0, 0 == std::tr1::tuple_size<T>::value> head;
        head(out, tp);
        out.push_back(')');
    }
};

#else

template<class T>
struct PrettyPrinterCategory<T, typename mp::EnableIfExists<typename std::tuple_element<0, T>::type, void>::Type>
{
    typedef StlTuple Category;
};
template<>
struct PrettyPrinterCategory<std::tuple<>, void>
{
    typedef StlTuple Category;
};

template<class T, int idx, bool stop>
struct TuplePrettyPrinter
{
    void operator()(std::string& out, const T& tp) const
    {
        if (idx > 0) {
            out.push_back(',');
        }
        out.append(prettyPrint(std::get<idx>(tp)));
        TuplePrettyPrinter<T, idx + 1, idx + 1 >= std::tuple_size<T>::value> nxt;
        nxt(out, tp);
    }
};

template<class T, int idx>
struct TuplePrettyPrinter<T, idx, true>
{
    void operator()(std::string& out, const T& tp) const
    {}
};

template<class T>
struct PrettyPrinter<StlTuple, T>
{
    void operator()(std::string& out, const T& tp) const
    {
        out.push_back('(');
        TuplePrettyPrinter<T, 0, 0 == std::tuple_size<T>::value> head;
        head(out, tp);
        out.push_back(')');
    }
};

#endif

struct CString {};

template<>
struct PrettyPrinterCategory<const char*, void>
{
    typedef CString Category;
};

template<int N>
struct PrettyPrinterCategory<const char[N], void>
{
    typedef CString Category;
};

template<class CStrLike>
struct PrettyPrinter<CString, CStrLike>
{
    void operator()(std::string& out, const CStrLike cs) const
    {
        out.append(cs);
    }
};

} // namespace impl
} // namespace pp
