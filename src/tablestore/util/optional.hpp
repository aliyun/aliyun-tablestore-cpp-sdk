#pragma once
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
#include <tr1/functional>

namespace aliyun {
namespace tablestore {
namespace util {

template<class T>
class Optional
{
public:
    explicit Optional()
      : mPresent(false),
        mValue()
    {}
    explicit Optional(const T& v)
      : mPresent(true),
        mValue(v)
    {}
    Optional(const Optional<T>& v)
      : mPresent(v.mPresent),
        mValue()
    {
        if (v.present()) {
            mValue = *v;
        }
    }
    explicit Optional(const MoveHolder<T>& v)
      : mPresent(true),
        mValue()
    {
        *this = v;
    }
    explicit Optional(const MoveHolder<Optional<T> >& a)
      : mPresent(a.get().mPresent),
        mValue()
    {
        *this = a;
    }
    
    Optional<T>& operator=(const Optional<T>& ano)
    {
        if (ano.present()) {
            mPresent = true;
            mValue = ano.mValue;
        } else {
            mPresent = false;
            mValue = T();
        }
        return *this;
    }
    Optional<T>& operator=(const T& v)
    {
        mPresent = true;
        mValue = v;
        return *this;
    }
    Optional<T>& operator=(const MoveHolder<T>& v)
    {
        mPresent = true;
        moveAssign(&mValue, v);
        return *this;
    }
    Optional<T>& operator=(const MoveHolder<Optional<T> >& v)
    {
        if (v->present()) {
            mPresent = true;
            moveAssign(&mValue, util::move(v->mValue));
        } else {
            reset();
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
        T v = T();
        moveAssign(&mValue, util::move(v));
    }

    const T& operator*() const throw()
    {
        OTS_ASSERT(present());
        return mValue;
    }

    T& operator*() throw()
    {
        OTS_ASSERT(present());
        return mValue;
    }

    const T* operator->() const throw()
    {
        OTS_ASSERT(present());
        return &mValue;
    }

    T* operator->() throw()
    {
        OTS_ASSERT(present());
        return &mValue;
    }
   
    const MoveHolder<T>& transfer()
    {
        OTS_ASSERT(present());
        mPresent = false;
        return util::move(mValue);
    }
    
    template<class U, class V>
    Optional<U> apply(const std::tr1::function<U(V)>& fn) const
    {
        if (present()) {
            return Optional<U>(fn(**this));
        } else {
            return Optional<U>();
        }
    }

    template<class U, class V>
    Optional<U> apply(U (*fn)(V)) const
    {
        if (present()) {
            return Optional<U>(fn(**this));
        } else {
            return Optional<U>();
        }
    }

private:
    bool mPresent;
    T mValue;
};

} // namespace util
} // namespace tablestore
} // namespace aliyun
