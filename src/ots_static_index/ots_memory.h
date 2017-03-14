#ifndef OTS_STATIC_INDEX_MEMORY_H
#define OTS_STATIC_INDEX_MEMORY_H

#include "noncopyable.h"
#include <cassert>

namespace static_index {

template<typename T>
class ScopedArray : private Noncopyable
{
    T* mArray;

public:
    explicit ScopedArray(T* arr)
      : mArray(arr)
    {}
    ~ScopedArray()
    {
        delete[] mArray;
    }
    T& operator[](size_t idx) const
    {
        return mArray[idx];
    }
};

template<typename T>
class UniquePtr : private Noncopyable
{
    T* mPtr;

public:
    UniquePtr()
      : mPtr(NULL)
    {}
    explicit UniquePtr(T* obj)
      : mPtr(obj)
    {}
    ~UniquePtr()
    {
        delete mPtr;
    }
    explicit UniquePtr(UniquePtr& ano)
    {
        mPtr = ano.mPtr;
        ano.mPtr = NULL;
    }

    void Swap(UniquePtr& ano)
    {
        T* p = mPtr;
        mPtr = ano.mPtr;
        ano.mPtr = p;
    }

    void Reset(T* p)
    {
        delete mPtr;
        mPtr = p;
    }
    
    T* Get() const
    {
        return mPtr;
    }
    T* operator->() const
    {
        return mPtr;
    }
    T& operator*() const
    {
        return *mPtr;
    }
};

} // namespace static_index

#endif /* OTS_STATIC_INDEX_MEMORY_H */
