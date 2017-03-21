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

/**
 * This is emulation of a part of C++11 move semantics, say, move assignment.
 *
 * Here is an example. Let us swap two values:
 * template<class T> void swap(T& obja, T& objb) {
 *     T tmp = obja; 
 *     obja = objb;  
 *     objb = tmp;   
 * }
 * In this routine, we copy obja twice. It is costly if obja is a large data 
 * structure. Moreover, if T is uncopyable, i.e., holder of some resources,
 * this swap routine is not applicable.
 *
 * To conquer this, C++11 introduces move sematics. We here provides a
 * simplified version which is easy to emulate in C++98.
 *
 * A type is moveable, if it implements both the move ctor and the move assignment.
 * For example, if T is moveable, then T must have the following two functions:
 * T(const MoveHolder<T>&) throw();            // move ctor
 * T& operator=(const MoveHolder<T>&) throw(); // move assignment
 * After move ctor or move assignment is invoked, the original object is on
 * a specific state which is destructive but nobody cares about, and thus
 * usually cheap to destruct.
 * 
 * Move is a placeholder in order to distinguish move ctor and move assignment
 * from copy ctor and copy assignment. As C++11, we also provides a helper
 * function Move in deducing types. Then, we can rewrite the swap routine as:
 * template<T> void swap(T& obja, T& objb) {
 *     T tmp = move(obja); // (1)
 *     obja = move(objb);  // (2)
 *     objb = move(tmp);   // (3)
 * }
 * After (1), content of obja is moved into, rather than copy into, tmp, 
 * and obja is cheap to destruct in executing (2). In other words, we swap 
 * contents of obja and objb by moving into/from a thirdparty tmp.
 * No heavy copying at all.
 *
 * To be compatible with old types, we provide external move assignments.
 * If the underlying type is moveable, move assignments are invoked; otherwise,
 * or some light-weight operation complying with move semantics will be invoked 
 * if there is. With help of them, our swap routine can be in this form:
 * template<T> void swap(T& obja, T& objb) {
 *     T tmp; // must be cheap
 *     moveAssign(&tmp, move(obja));
 *     moveAssign(&obja, move(objb));
 *     moveAssign(&objb, move(tmp));
 * }
 */

namespace aliyun {
namespace tablestore {
namespace util {

template<class T>
class MoveHolder
{
public:
    explicit MoveHolder(T& val)
      : mValue(&val)
    {}

    T& operator*() const
    {
        return *mValue;
    }

    T* operator->() const
    {
        return mValue;
    }

private:
    mutable T* mValue;
};

template<class T>
MoveHolder<T> move(T& val)
{
    return MoveHolder<T>(val);
}

/**
 * No more than a placeholder to make move tester happy.
 * Any class inherited from Moveable must provides:
 * T(const MoveHolder<T>&) throw();            // move ctor
 * T& operator=(const MoveHolder<T>&) throw(); // move assignment
 */
struct Moveable {};

// external move assignments
namespace impl {

template<class Category, class T>
struct MoveAssignment {};

template<class T, class Enable = void>
struct MoveCategory {};

} // namespace impl

template<class T>
void moveAssign(T* to, const MoveHolder<T>& from) throw()
{
    typedef typename impl::MoveCategory<T>::Category Category;
    impl::MoveAssignment<Category, T> f;
    f(to, from);
}

} // namespace util
} // namespace tablestore
} // namespace aliyun

#include "move.ipp"
