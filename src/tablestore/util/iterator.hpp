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

#include "tablestore/util/optional.hpp"
#include <tr1/type_traits>

namespace aliyun {
namespace tablestore {
namespace util {

/**
 * An interface to iterators.
 * Usually, its usage is like:
 *
 * auto_ptr<Iterator<T, Err> > iter(...);
 * for(;;) {
 *     Optional<Err> err = iter->moveNext();
 *     if (err.present()) {
 *         // moveNext() is possibly backboned by IO
 *         // So it may fail.
 *         break;
 *     }
 *     if (!iter->valid()) {
 *         // meets the end of this iteration
 *         break;
 *     }
 *     const T& val = iter->get();
 *     // do something with val
 * }
 */
template<class T, class Err>
class Iterator
{
public:
    typedef T ElemType;

    virtual ~Iterator() {}

    virtual bool valid() const throw() =0;
    virtual T get() throw() =0;
    /**
     * Move to next valid element if exists.
     * If any error occurs on the way, wraps it into an Optional and returns;
     * otherwise, returns an absent Optional.
     */
    virtual Optional<Err> moveNext() =0;
};

} // namespace util
} // namespace tablestore
} // namespace aliyun

#include "iterator.ipp"

namespace aliyun {
namespace tablestore {
namespace util {

template<class C>
class StlContainerIterator
  : public Iterator<typename impl::IteratorTraits<C>::ElemType, int>
{
public:
    typedef typename impl::IteratorTraits<C>::ElemType ElemType;
    typedef typename impl::IteratorTraits<C>::IteratorType IteratorType;

    explicit StlContainerIterator(C& container)
      : mContainer(container),
        mIter(container.begin())
    {}

    bool valid() const throw()
    {
        return mIter != mContainer.end();
    }

    ElemType get() throw()
    {
        return *mIter;
    }

    /**
     * Always returns an absent Optional
     */
    Optional<int> moveNext()
    {
        ++mIter;
        return Optional<int>();
    }
    
private:
    C& mContainer;
    IteratorType mIter;
};

template<class C>
StlContainerIterator<C> iterate(C& xs)
{
    return StlContainerIterator<C>(xs);
}

} // namespace util
} // namespace tablestore
} // namespace aliyun
