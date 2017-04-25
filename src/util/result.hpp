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

#include "util/assert.hpp"
#include "util/move.hpp"

namespace aliyun {
namespace tablestore {
namespace util {

template<class OkType, class ErrType>
class Result
{
    enum ResultType
    {
        OK,
        ERROR,
    };

public:
    explicit Result()
      : mResultType(OK),
        mOkValue(),
        mErrValue()
    {}

    explicit Result(const util::MoveHolder<Result<OkType, ErrType> >& a)
    {
        *this = a;
    }

    Result<OkType, ErrType>& operator=(
        const util::MoveHolder<Result<OkType, ErrType> >& a)
    {
        util::moveAssign(&mResultType, util::move(a->mResultType));
        util::moveAssign(&mOkValue, util::move(a->mOkValue));
        util::moveAssign(&mErrValue, util::move(a->mErrValue));
        return *this;
    }

    void reset()
    {
        mResultType = OK;
        {
            OkType empty = OkType();
            util::moveAssign(&mOkValue, util::move(empty));
        }
        {
            ErrType empty = ErrType();
            util::moveAssign(&mErrValue, util::move(empty));
        }
    }

    bool ok() const
    {
        return mResultType == OK;
    }

    const OkType& okValue() const
    {
        OTS_ASSERT(ok());
        return mOkValue;
    }

    OkType* mutableOkValue()
    {
        mResultType = OK;
        {
            ErrType empty = ErrType();
            util::moveAssign(&mErrValue, util::move(empty));
        }
        return &mOkValue;
    }

    const ErrType& errValue() const
    {
        OTS_ASSERT(!ok());
        return mErrValue;
    }

    ErrType* mutableErrValue()
    {
        mResultType = ERROR;
        {
            OkType empty = OkType();
            util::moveAssign(&mOkValue, util::move(empty));
        }
        return &mErrValue;
    }
    
private:
    ResultType mResultType;
    OkType mOkValue;
    ErrType mErrValue;
};

} // namespace util
} // namespace tablestore
} // namespace aliyun
