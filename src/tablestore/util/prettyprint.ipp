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
#include <string>
#include <deque>
#include <cstdlib>
    
#if __cplusplus < 201103L
#include <tr1/tuple>
#include <stdint.h>
#else
#include <tuple>
#include <cstdint>
#endif

namespace pp {
namespace impl {

template<class T, void (T::*)(std::string&) const>
struct IsPrettyPrintable
{
    typedef void Type;
};

template<class T>
struct PrettyPrinter<
    T,
    typename IsPrettyPrintable<T, &T::prettyPrint>::Type>
{
    void operator()(std::string& out, const T& x) const
    {
        x.prettyPrint(out);
    }
};

template<>
struct PrettyPrinter<bool, void>
{
    void operator()(std::string& out, bool x) const
    {
        static const char kTrue[] = "true";
        static const char kFalse[] = "false";
        if (x) {
            out.append(kTrue, sizeof(kTrue) - 1);
        } else {
            out.append(kFalse, sizeof(kFalse) - 1);
        }
    };
};

inline void toHex(std::string& out, uint8_t x)
{
    static const char kAlphabet[] = "0123456789ABCDEF";
    if (x > 15) {
        ::abort();
    }
    out.push_back(kAlphabet[x]);
}

struct Char
{
    static void p(std::string& out, char x)
    {
        uint8_t xx = x;
        if (xx >= 32 && xx <= 127) {
            if (x == '\'') {
                out.push_back('\\');
                out.push_back('\'');
            } else if (x == '"') {
                out.push_back('\\');
                out.push_back('"');
            } else {
                out.push_back(x);
            }
        } else {
            out.push_back('\\');
            out.push_back('x');
            toHex(out, xx >> 4);
            toHex(out, xx & 0xF);
        }
    };
};

template<>
struct PrettyPrinter<char, void>
{
    void operator()(std::string& out, char x) const
    {
        out.push_back('\'');
        Char::p(out, x);
        out.push_back('\'');
    };
};

template<class T>
struct PrettyPrinter<
    T,
    typename mp::VoidIf<mp::IsInteger<T>::value && mp::IsUnsigned<T>::value>::Type>
{
    void operator()(std::string& out, T x) const
    {
        if (x == 0) {
            out.push_back('0');
        } else {
            std::deque<uint8_t> xs;
            for(; x > 0; x /= 10) {
                xs.push_back(x % 10);
            }
            for(; !xs.empty(); xs.pop_back()) {
                out.push_back('0' + xs.back());
            }
        }
    };
};

template<>
struct PrettyPrinter<std::string, void>
{
    void operator()(std::string& out, const std::string& x) const
    {
        out.push_back('\"');
        for(std::string::const_iterator i = x.begin(); i != x.end(); ++i) {
            Char::p(out, *i);
        }
        out.push_back('\"');
    }
};

template<class T>
struct PrettyPrinter<
    T,
    typename mp::VoidIf<mp::IsInteger<T>::value && mp::IsSigned<T>::value>::Type>
{
    void operator()(std::string& out, T x) const
    {
        typename mp::MakeUnsigned<T>::Type y = x >= 0 ? x : -x;
        if (x < 0) {
            out.push_back('-');
        }
        pp::prettyPrint(out, y);
    };
};

struct Floating
{
    static void p(std::string&, double);
};

template<class T>
struct PrettyPrinter<
    T,
    typename mp::VoidIf<mp::IsFloatingPoint<T>::value>::Type>
{
    void operator()(std::string& out, T x) const
    {
        Floating::p(out, x);
    }
};

template<class T>
struct PrettyPrinter<
    T,
    typename mp::VoidIf<
        mp::IsSeq<T>::value
        && !mp::IsAssociative<T>::value
        && !mp::IsSame<T, std::string>::value>::Type>
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
struct PrettyPrinter<
    T,
    typename mp::VoidIf<mp::IsAssociative<T>::value>::Type>
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

template<class T>
struct PrettyPrinter<
    T,
    typename mp::VoidIf<mp::IsSmartPtr<T>::value>::Type>
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


#if __cplusplus < 201103L
template<class T>
struct _TupleSize
{
    static const std::size_t value = std::tr1::tuple_size<T>::value;
};
template<class T, std::size_t n>
const typename std::tr1::tuple_element<n, T>::type& _getTuple(const T& xs)
{
    return std::tr1::get<n>(xs);
}
#else
template<class T>
struct _TupleSize
{
    static const std::size_t value = std::tuple_size<T>::value;
};
template<class T, std::size_t n>
const typename std::tuple_element<n, T>::type& _getTuple(const T& xs)
{
    return std::get<n>(xs);
}
#endif

template<class T, std::size_t idx, bool end>
struct _TuplePrettyPrint
{
    void operator()(std::string& out, const T& xs) const
    {
        pp::prettyPrint(out, _getTuple<T, idx>(xs));
        if (idx + 1 == _TupleSize<T>::value) {
            return;
        }
        out.push_back(',');
        _TuplePrettyPrint<
            T,
            idx + 1,
            idx + 1 == _TupleSize<T>::value> p;
        p(out, xs);
    }
};

template<class T, std::size_t idx>
struct _TuplePrettyPrint<T, idx, true>
{
    void operator()(std::string& out, const T& xs) const
    {}
};

#if __cplusplus < 201103L
template<std::size_t n>
struct __TupleSize
{
    static const bool value = (n >= 0);
};

template<class T>
struct PrettyPrinter<
    T,
    typename mp::VoidIf<__TupleSize<std::tr1::tuple_size<T>::value>::value>::Type>
{
    void operator()(std::string& out, const T& xs) const
    {
        out.push_back('(');
        _TuplePrettyPrint<T, 0, std::tr1::tuple_size<T>::value == 0> p;
        p(out, xs);
        out.push_back(')');
    }

};
#else
template<class T>
struct PrettyPrinter<
    T,
    typename mp::VoidIfExists<typename std::tuple_size<T>::value_type>::Type>
{
    void operator()(std::string& out, const T& xs) const
    {
        out.push_back('(');
        _TuplePrettyPrint<T, 0, std::tuple_size<T>::value == 0> p;
        p(out, xs);
        out.push_back(')');
    }

};
#endif

template<int n>
struct PrettyPrinter<const char[n], void>
{
    void operator()(std::string& out, const char* cs) const
    {
        out.append(cs, n);
    }
};

template<>
struct PrettyPrinter<const char*, void>
{
    void operator()(std::string& out, const char* cs) const
    {
        out.append(cs);
    }
};

} // namespace impl
} // namespace pp
