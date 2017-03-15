/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#ifndef OTS_COMMON_H
#define OTS_COMMON_H

#include "ots_exception.h"
#include <string>

namespace aliyun {
namespace tablestore {

/**
 * @brief OptionalValue
 *
 * 可选值模板。
 */
template <typename T>
class OptionalValue
{
public:

    OptionalValue()
        : mIsSet(false)
    {
    }

    OptionalValue(const T& value)
        : mValue (value)
        , mIsSet(true)
    {
    }

    void SetValue(const T& value)
    {
        mValue = value;
        mIsSet = true;
    }

    const T& GetValue() const
    {
        if (!mIsSet) {
            throw OTSClientException("Optional value is not set.");
        }
        return mValue;
    }

    bool HasValue() const
    {
        return mIsSet;
    }

private:

    T mValue;

    bool mIsSet;
};

/**
 * @brief Iterator
 *
 * 迭代器模板。
 */
template <typename T>
class Iterator
{
public:

    virtual ~Iterator() {}

    virtual bool HasNext() = 0;

    virtual T Next() = 0;
};

namespace impl {

/**
 * A concrete implementation must provide a both specialized and public function, 
 * whose signature must be
 * void ToStringImpl<ConcreteT>::operator()(std::string* out, const ConcreteT&) const;
 */
template<typename T>
class ToStringImpl
{
public:
};

} // namespace impl

template<typename T>
void ToString(std::string* out, const T& x)
{
    impl::ToStringImpl<T> imp;
    imp(out, x);
}

template<typename T>
std::string ToString(const T& x)
{
    std::string res;
    ToString<T>(&res, x);
    return res;
}

} // end of tablestore
} // end of aliyun

#endif
