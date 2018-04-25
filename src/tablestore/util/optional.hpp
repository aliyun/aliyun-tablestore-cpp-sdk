#pragma once
#ifndef TABLESTORE_UTIL_OPTIONAL_HPP
#define TABLESTORE_UTIL_OPTIONAL_HPP
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
#include "tablestore/util/assert.hpp"
#include "tablestore/util/move.hpp"
#include "tablestore/util/metaprogramming.hpp"
#include <tr1/functional>
#include <tr1/type_traits>
#include <stdint.h>

namespace aliyun {
namespace tablestore {
namespace util {

template<class T, class E = void>
class Optional;

namespace impl {
template<class T>
struct SmallTrivial
{
    static const bool value = mp::IsTrivial<T>::value && (sizeof(T) <= sizeof(int64_t));
};

template<class T>
struct Apply
{
    template<class U, class V>
    Optional<U> apply(const std::tr1::function<Optional<U>(V)>& fn) const
    {
        const T& thiz = static_cast<const T&>(*this);
        if (thiz.present()) {
            return fn(*thiz);
        } else {
            return Optional<U>();
        }
    }

    template<class U, class V>
    Optional<U> apply(Optional<U> (*fn)(V)) const
    {
        const T& thiz = static_cast<const T&>(*this);
        if (thiz.present()) {
            return fn(*thiz);
        } else {
            return Optional<U>();
        }
    }
};

} // namespace impl

template<class T>
class Optional<
    T,
    typename mp::VoidIf<impl::SmallTrivial<T>::value>::Type>
  : public impl::Apply<Optional<T> >
{
public:
    explicit Optional()
      : mPresent(false),
        mValue()
    {}

    template<class U>
    explicit Optional(U v)
      : mPresent(true),
        mValue(v)
    {}

    explicit Optional(const MoveHolder<T>& v)
      : mPresent(true),
        mValue(*v)
    {}

    template<class U>
    Optional(const Optional<U>& v)
      : mPresent(v.mPresent),
        mValue()
    {
        if (v.present()) {
            mValue = *v;
        }
    }

    explicit Optional(const MoveHolder<Optional<T> >& a)
      : mPresent(false),
        mValue()
    {
        *this = a;
    }

    template<class U>
    Optional<T>& operator=(const Optional<U>& a)
    {
        if (&a != this) {
            reset();
            if (a.present()) {
                reset(*a);
            }
        }
        return *this;
    }

    Optional<T>& operator=(const MoveHolder<Optional<T> >& v)
    {
        reset();
        if (v->present()) {
            reset(**v);
        }
        return *this;
    }

    bool present() const throw()
    {
        return mPresent;
    }

    void reset()
    {
        mPresent = false;
        mValue = T();
    }

    void reset(const T& v)
    {
        mPresent = true;
        mValue = v;
    }

    void reset(const MoveHolder<T>& v)
    {
        mPresent = true;
        mValue = *v;
    }

    T& operator*() const throw()
    {
        OTS_ASSERT(present());
        return const_cast<T&>(mValue);
    }

    T* operator->() const throw()
    {
        OTS_ASSERT(present());
        return &const_cast<T&>(mValue);
    }

private:
    bool mPresent;
    T mValue;
};

template<class T>
class Optional<
    T,
    typename mp::VoidIf<mp::IsReference<T>::value>::Type>
  : public impl::Apply<Optional<T> >
{
    typedef typename mp::RemoveRef<T>::Type Tp;

public:
    explicit Optional()
      : mPtr(NULL)
    {}

    template<class U>
    explicit Optional(U& v)
      : mPtr(NULL)
    {
        mPtr = &v;
    }

    template<class U>
    Optional(const Optional<U>& v)
      : mPtr(NULL)
    {
        if (v.present()) {
            mPtr = &v;
        }
    }

    explicit Optional(const MoveHolder<Optional<T> >& a)
      : mPtr(NULL)
    {
        if (a->present()) {
            mPtr = &**a;
        }
    }

    template<class U>
    Optional<T>& operator=(const Optional<U>& a)
    {
        if (&a != this) {
            reset();
            if (a.present()) {
                reset(*a);
            }
        }
        return *this;
    }

    Optional<T>& operator=(const MoveHolder<Optional<T> >& v)
    {
        reset();
        if (v->present()) {
            reset(**v);
        }
        return *this;
    }

    bool present() const throw()
    {
        return mPtr != NULL;
    }

    void reset()
    {
        mPtr = NULL;
    }

    void reset(T v)
    {
        mPtr = &v;
    }

    Tp& operator*() const throw()
    {
        OTS_ASSERT(present());
        return *mPtr;
    }

    Tp* operator->() const throw()
    {
        OTS_ASSERT(present());
        return mPtr;
    }

private:
    Tp* mPtr;
};

template<class T>
class Optional<
    T,
    typename mp::VoidIf<!impl::SmallTrivial<T>::value && !mp::IsReference<T>::value>::Type>
  : public impl::Apply<Optional<T> >
{
public:
    ~Optional()
    {
        reset();
    }

    explicit Optional()
      : mPtr(NULL)
    {}

    explicit Optional(const T& v)
      : mPtr(NULL)
    {
        reset(v);
    }

    Optional(const Optional<T>& v)
      : mPtr(NULL)
    {
        if (v.present()) {
            reset(*v);
        }
    }

    explicit Optional(const MoveHolder<T>& a)
      : mPtr(NULL)
    {
        reset(a);
    }
    
    explicit Optional(const MoveHolder<Optional<T> >& a)
      : mPtr(NULL)
    {
        *this = a;
    }

    Optional<T>& operator=(const Optional<T>& a)
    {
        if (&a != this) {
            reset();
            if (a.present()) {
                reset(*a);
            }
        }
        return *this;
    }

    Optional<T>& operator=(const MoveHolder<Optional<T> >& v)
    {
        reset();
        if (v->present()) {
            reset(util::move(**v));
        }
        return *this;
    }

    bool present() const throw()
    {
        return mPtr != NULL;
    }

    void reset()
    {
        if (mPtr != NULL) {
            mPtr->~T();
            mPtr = NULL;
        }
    }

    void reset(const T& v)
    {
        reset();
        mPtr = new (mBuf) T(v);
    }

    void reset(const MoveHolder<T>& v)
    {
        if (&*v != mPtr) {
            reset();
            mPtr = new (mBuf) T();
            moveAssign(*mPtr, v);
        }
    }

    T& operator*() const throw()
    {
        OTS_ASSERT(present());
        return *mPtr;
    }

    T* operator->() const throw()
    {
        OTS_ASSERT(present());
        return mPtr;
    }

private:
    T* mPtr;
    uint8_t mBuf[sizeof(T)];
};

} // namespace util
} // namespace tablestore
} // namespace aliyun
#endif
