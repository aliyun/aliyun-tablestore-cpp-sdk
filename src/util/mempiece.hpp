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
#pragma once

#include "util/move.hpp"
#include "util/assert.hpp"
#include <string>
#include <algorithm>
#include <cstring>
#include <stdint.h>

namespace aliyun {
namespace tablestore {
namespace util {

namespace impl {

template<class T, class Enable = void>
struct ToMemPiece {};

} // namspace impl

/**
 * A delegate to a piece of continuous memory which is not owned by the delegate.
 */
class MemPiece: public Moveable
{
public:
    explicit MemPiece()
      : mData(NULL),
        mLen(0)
    {}
    explicit MemPiece(const void* data, int64_t len)
      : mData(static_cast<const uint8_t*>(data)),
        mLen(len)
    {}

    explicit MemPiece(const MoveHolder<MemPiece>& ano)
      : mData(ano->data()),
        mLen(ano->length())
    {}

    MemPiece& operator=(const MoveHolder<MemPiece>& ano)
    {
        mData = ano->data();
        mLen = ano->length();
        return *this;
    }

    template<class T>
    static MemPiece from(const T& x)
    {
        impl::ToMemPiece<T> p;
        return p(x);
    }
    
    const uint8_t* data() const throw()
    {
        return mData;
    }

    int64_t length() const throw()
    {
        return mLen;
    }

    MemPiece subpiece(int64_t start) const
    {
        OTS_ASSERT(start <= length())
            (start)(length());
        if (data() == NULL) {
            return MemPiece();
        } else {
            return MemPiece(data() + start, length() - start);
        }
    }

    MemPiece subpiece(int64_t start, int64_t len) const
    {
        OTS_ASSERT(start <= length())
            (start)(len)(length());
        OTS_ASSERT(start + len <= length())
            (start)(len)(length());
        if (data() == NULL) {
            return MemPiece();
        } else {
            return MemPiece(data() + start, len);
        }
    }

    uint8_t get(int64_t idx) const
    {
        OTS_ASSERT(idx < length())
            (idx)(length());
        return data()[idx];
    }

    void toStr(std::string* out) const
    {
        out->append(
            static_cast<const char*>(static_cast<const void*>(data())),
            length());
    }

    std::string toStr() const
    {
        std::string res;
        res.reserve(length() + 1);
        toStr(&res);
        return res;
    }

    void prettyPrint(std::string*) const;

    bool startsWith(const MemPiece& b) const throw()
    {
        if (length() < b.length()) {
            return false;
        }
        if (b.length() == 0) {
            return true;
        }
        if (data() == b.data()) {
            return true;
        }
        int c = ::memcmp(data(), b.data(), b.length());
        return c == 0;
    }

    bool endsWith(const MemPiece& b) const throw()
    {
        if (length() < b.length()) {
            return false;
        }
        if (b.length() == 0) {
            return true;
        }
        if (data() + length() == b.data() + b.length()) {
            return true;
        }
        int c = ::memcmp(data() + length() - b.length(), b.data(), b.length());
        return c == 0;
    }
    
private:
    const uint8_t* mData;
    int64_t mLen;
};

inline int lexicographicOrder(const MemPiece& a, const MemPiece& b)
{
    int64_t asz = a.length();
    int64_t bsz = b.length();
    int64_t sz = std::min(asz, bsz);
    if (sz == 0) {
        if (asz > 0) {
            return 1;
        } else if (bsz > 0) {
            return -1;
        } else {
            return 0;
        }
    }
    int c = ::memcmp(a.data(), b.data(), sz);
    if (c != 0) {
        return c;
    } else {
        if (asz > bsz) {
            return 1;
        } else if (bsz > asz) {
            return -1;
        } else {
            return 0;
        }
    }
}

inline int quasilexicographicOrder(const MemPiece& a, const MemPiece& b)
{
    int64_t asz = a.length();
    int64_t bsz = b.length();
    if (asz < bsz) {
        return -1;
    } else if (asz > bsz) {
        return 1;
    } else if (asz == 0) {
        return 0;
    } else {
        return ::memcmp(a.data(), b.data(), asz);
    }
}

inline bool operator==(const MemPiece& a, const MemPiece& b)
{
    return quasilexicographicOrder(a, b) == 0;
}

inline bool operator!=(const MemPiece& a, const MemPiece& b)
{
    return quasilexicographicOrder(a, b) != 0;
}


template<class T, class Enable = void>
struct LexicographicLess {};

template<>
struct LexicographicLess<std::string, void>
{
    bool operator()(const std::string& a, const std::string& b) const
    {
        return lexicographicOrder(MemPiece::from(a), MemPiece::from(b)) < 0;
    }
};

template<>
struct LexicographicLess<const std::string, void>
{
    bool operator()(const std::string& a, const std::string& b) const
    {
        LexicographicLess<std::string> p;
        return p(a, b);
    }
};

template<>
struct LexicographicLess<MemPiece, void>
{
    bool operator()(const MemPiece& a, const MemPiece& b) const
    {
        return lexicographicOrder(a, b) < 0;
    }
};

template<>
struct LexicographicLess<const MemPiece, void>
{
    bool operator()(const MemPiece& a, const MemPiece& b) const
    {
        return lexicographicOrder(a, b) < 0;
    }
};


template<class T, class Enable = void>
struct QuasiLexicographicLess {};

template<>
struct QuasiLexicographicLess<std::string, void>
{
    bool operator()(const std::string& a, const std::string& b) const
    {
        return quasilexicographicOrder(MemPiece::from(a), MemPiece::from(b)) < 0;
    }
};

template<>
struct QuasiLexicographicLess<const std::string, void>
{
    bool operator()(const std::string& a, const std::string& b) const
    {
        QuasiLexicographicLess<std::string> p;
        return p(a, b);
    }
};

template<>
struct QuasiLexicographicLess<MemPiece, void>
{
    bool operator()(const MemPiece& a, const MemPiece& b) const
    {
        return quasilexicographicOrder(a, b) < 0;
    }
};

template<>
struct QuasiLexicographicLess<const MemPiece, void>
{
    bool operator()(const MemPiece& a, const MemPiece& b) const
    {
        return quasilexicographicOrder(a, b) < 0;
    }
};

} // namespace util
} // namespace tablestore
} // namespace aliyun

#include "mempiece.ipp"

