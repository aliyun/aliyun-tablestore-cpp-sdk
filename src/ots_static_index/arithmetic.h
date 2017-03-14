#ifndef OTS_STATIC_INDEX_ARITHMETIC_H
#define OTS_STATIC_INDEX_ARITHMETIC_H

#include <tr1/tuple>
#include <cassert>

namespace static_index {

/**
 * @return quotient (q) and reminder (r) of a and b, i.e.,
 *         q*b+r=a, where 0<=r<b.
 *
 * b must be strictly greater than 0.
 */
template<typename T>
::std::tr1::tuple<T, T> DivMod(T a, T b)
{
    assert(b > 0);
    if (a >= 0) {
        return std::tr1::make_tuple(a / b, a % b);
    } else {
        T q = (a + 1) / b - 1;
        T r = (a + 1) % b + b - 1;
        return std::tr1::make_tuple(q, r);
    }
}

inline uint64_t ShiftToUint64(int64_t x)
{
    const uint64_t kMask = 0x8000000000000000;
    uint64_t r = x;
    r = (~r & kMask) | (r & ~kMask);
    return r;
}

} // namespace static_index

#endif /* OTS_STATIC_INDEX_ARITHMETIC_H */
