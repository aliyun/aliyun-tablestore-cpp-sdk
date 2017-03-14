#ifndef OTS_STATIC_INDEX_SLICE_H
#define OTS_STATIC_INDEX_SLICE_H

#include <set>
#include <map>
#include <string>
#include <cstring>
#include <cassert>
#include <stdint.h>

namespace static_index {

class Slice
{
    const uint8_t* mStart;
    const uint8_t* mEnd;

public:
    Slice()
      : mStart(NULL),
        mEnd(NULL)
    {}
    explicit Slice(const uint8_t* start, const uint8_t* end)
      : mStart(start),
        mEnd(end)
    {
        assert(start != NULL);
        assert(end != NULL);
        assert(start <= end);
    }

    Slice Subslice(int64_t start) const
    {
        assert(start <= Size());
        if (mStart == NULL) {
            return *this;
        } else {
            return Slice(mStart + start, mEnd);
        }
    }

    Slice Subslice(int64_t start, int64_t len) const
    {
        assert(start <= Size());
        assert(start + len <= Size());
        if (mStart == NULL) {
            return *this;
        } else {
            return Slice(mStart + start, mStart + start + len);
        }
    }

    const uint8_t* Start() const
    {
        return mStart;
    }
    const uint8_t* End() const
    {
        return mEnd;
    }
    int64_t Size() const
    {
        return mEnd - mStart;
    }
    ::std::string ToString() const
    {
        return ::std::string((char*) mStart, Size());
    }

    int QuasiLexicographicOrder(const Slice& b) const
    {
        int64_t sa = Size();
        int64_t sb = b.Size();
        if (sa < sb) {
            return -1;
        } else if (sa > sb) {
            return 1;
        } else {
            if (Start() == b.Start()) {
                return 0;
            } else {
                return ::memcmp(Start(), b.Start(), sa);
            }
        }
    }

    int LexicographicOrder(const Slice& b) const;
};

namespace details {

template<typename T>
class AnyToSlice
{};

template<>
class AnyToSlice< ::std::string>
{
public:
    Slice operator()(const ::std::string& str) const
    {
        const uint8_t* b = (uint8_t*) str.data();
        const uint8_t* e = b + str.size();
        return Slice(b, e);
    }
};

template<>
class AnyToSlice<const ::std::string&>
{
public:
    Slice operator()(const ::std::string& str) const
    {
        const uint8_t* b = (uint8_t*) str.data();
        const uint8_t* e = b + str.size();
        return Slice(b, e);
    }
};

template<int size>
class AnyToSlice<char[size]>
{
public:
    Slice operator()(const char* str) const
    {
        const uint8_t* b = (uint8_t*) str;
        const uint8_t* e = b + size - 1;
        return Slice(b, e);
    }
};

} // namespace details

template<typename T>
Slice ToSlice(const T& x)
{
    details::AnyToSlice<T> proc;
    return proc(x);
}

inline bool Prefix(const Slice& orig, const Slice& pat)
{
    if (orig.Size() < pat.Size()) {
        return false;
    }
    int c = ::memcmp(orig.Start(), pat.Start(), pat.Size());
    return c == 0;
}

inline bool Ending(const Slice& orig, const Slice& pat)
{
    if (orig.Size() < pat.Size()) {
        return false;
    }
    int64_t offset = orig.Size() - pat.Size();
    int c = ::memcmp(orig.Start() + offset, pat.Start(), pat.Size());
    return c == 0;
}

inline bool operator==(const Slice& a, const Slice& b)
{
    return a.QuasiLexicographicOrder(b) == 0;
}

inline bool operator!=(const Slice& a, const Slice& b)
{
    return !(a == b);
}

template<typename T>
class QuasiLexicographicOrderLess
{
};

template<>
class QuasiLexicographicOrderLess<Slice>
{
public:
    bool operator()(const Slice& a, const Slice& b) const
    {
        return a.QuasiLexicographicOrder(b) < 0;
    }
};

template<>
class QuasiLexicographicOrderLess< ::std::string >
{
public:
    bool operator()(const ::std::string& a, const ::std::string& b) const
    {
        return ToSlice(a).QuasiLexicographicOrder(ToSlice(b)) < 0;
    }
};

template<typename K, typename V>
class QloMap: public ::std::map<K, V, QuasiLexicographicOrderLess<K> >
{
public:
    explicit QloMap()
      : ::std::map<K, V, QuasiLexicographicOrderLess<K> >()
    {}
};

template<typename T>
class QloSet: public ::std::set<T, QuasiLexicographicOrderLess<T> >
{
public:
    explicit QloSet()
      : ::std::set<T, QuasiLexicographicOrderLess<T> >()
    {}
};

} // namespace static_index

#endif /* OTS_STATIC_INDEX_SLICE_H */
