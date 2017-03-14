#ifndef OTS_STATIC_INDEX_STRING_TOOLS_H
#define OTS_STATIC_INDEX_STRING_TOOLS_H

#include <limits>
#include <algorithm>
#include <string>
#include <cassert>
#include <stdint.h>
#include <cstdio>

namespace static_index {

namespace details {

template<typename T>
class ConvertToStr
{
public:
    ::std::string Convert(const T& x) const
    {
        return x.ToString();
    }
};

template<>
class ConvertToStr< ::std::string>
{
public:
    ::std::string Convert(const ::std::string& x) const
    {
        return x;
    }
};

template< ::std::size_t Size>
class ConvertToStr<char[Size]>
{
public:
    ::std::string Convert(const char* x) const
    {
        return ::std::string(x);
    }
};

template<>
class ConvertToStr<const char*>
{
public:
    ::std::string Convert(const char* x) const
    {
        return ::std::string(x);
    }
};

template<>
class ConvertToStr<char*>
{
public:
    ::std::string Convert(const char* x) const
    {
        return ::std::string(x);
    }
};

template<typename T>
void PositiveToStr(::std::string* out, T x, int radix)
{
    static const char kAlphabet[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    assert(radix > 1);
    assert(radix <= 36);
    assert(x >= 0);
    int64_t initPos = out->size();

    if (x == 0) {
        out->push_back('0');
        return;
    }

    for(; x > 0; x /= radix) {
        out->push_back(kAlphabet[x % radix]);
    }

    char* dat = const_cast<char*>(out->data());
    for(int64_t l = initPos, r = out->size() - 1; l < r; ++l, --r) {
        ::std::swap(dat[l], dat[r]);
    }
}

template<typename T>
void IntToStr(::std::string* out, T x, int radix)
{
    const T kHighest = ((T) 1) << (::std::numeric_limits<T>::digits - 1);
    if (x & kHighest) {
        out->push_back('-');
        x = ~x;
        ++x;
    }
    PositiveToStr(out, x, radix);
}

template<>
class ConvertToStr<int8_t>
{
public:
    ::std::string Convert(int8_t x) const
    {
        ::std::string res;
        res.reserve(::std::numeric_limits<int8_t>::digits10);
        IntToStr<uint8_t>(&res, x, 10);
        return res;
    }
};

template<>
class ConvertToStr<uint8_t>
{
public:
    ::std::string Convert(uint8_t x) const
    {
        ::std::string res;
        res.reserve(::std::numeric_limits<uint8_t>::digits10);
        PositiveToStr(&res, x, 10);
        return res;
    }
};

template<>
class ConvertToStr<int16_t>
{
public:
    ::std::string Convert(int16_t x) const
    {
        ::std::string res;
        res.reserve(::std::numeric_limits<int16_t>::digits10);
        IntToStr<uint16_t>(&res, x, 10);
        return res;
    }
};

template<>
class ConvertToStr<uint16_t>
{
public:
    ::std::string Convert(uint16_t x) const
    {
        ::std::string res;
        res.reserve(::std::numeric_limits<uint16_t>::digits10);
        PositiveToStr(&res, x, 10);
        return res;
    }
};

template<>
class ConvertToStr<int32_t>
{
public:
    ::std::string Convert(int32_t x) const
    {
        ::std::string res;
        res.reserve(::std::numeric_limits<int32_t>::digits10);
        IntToStr<uint32_t>(&res, x, 10);
        return res;
    }
};

template<>
class ConvertToStr<uint32_t>
{
public:
    ::std::string Convert(uint32_t x) const
    {
        ::std::string res;
        res.reserve(::std::numeric_limits<uint32_t>::digits10);
        PositiveToStr(&res, x, 10);
        return res;
    }
};

template<>
class ConvertToStr<int64_t>
{
public:
    ::std::string Convert(int64_t x) const
    {
        ::std::string res;
        res.reserve(::std::numeric_limits<int64_t>::digits10);
        IntToStr<uint64_t>(&res, x, 10);
        return res;
    }
};

template<>
class ConvertToStr<uint64_t>
{
public:
    ::std::string Convert(uint64_t x) const
    {
        ::std::string res;
        res.reserve(::std::numeric_limits<uint64_t>::digits10);
        PositiveToStr(&res, x, 10);
        return res;
    }
};

template<>
class ConvertToStr<char>
{
public:
    ::std::string Convert(char x) const
    {
        ::std::string res;
        res.reserve(2);
        res.push_back(x);
        return res;
    }
};

template<>
class ConvertToStr<bool>
{
    static const ::std::string kTrue;
    static const ::std::string kFalse;

public:
    ::std::string Convert(bool x) const
    {
        if (x) {
            return kTrue;
        } else {
            return kFalse;
        }
    }
};

template<>
class ConvertToStr<double>
{
public:
    ::std::string Convert(double x) const;
};

} // namespace details

template<typename T>
::std::string ToString(const T& x)
{
    details::ConvertToStr<T> c;
    return c.Convert(x);
}

inline ::std::string Hex(uint64_t x)
{
    static const char kAlphabet[] = "0123456789ABCDEF";

    ::std::string res(16, '0');

    int i = 15;
    for(; x > 0 && i >= 0; x /= 16, --i) {
        res[i] = kAlphabet[x % 16];
    }
    assert(i >= 0 || x == 0);

    return res;
}

inline bool IsBracket(char c)
{
    return c == '(' || c == ')';
}
    
inline bool IsBlank(char c)
{
    return c == ' ' || c == '\t' || c == '\n';
}

} // namespace static_index

#endif /* OTS_STATIC_INDEX_STRING_TOOLS_H */

