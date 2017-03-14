#include "slice.h"
#include <algorithm>

namespace static_index {

int Slice::LexicographicOrder(const Slice& b) const
{
    int64_t sa = Size();
    int64_t sb = b.Size();
    int64_t minSz = ::std::min(sa, sb);
    int r = ::memcmp(Start(), b.Start(), minSz);
    if (r != 0) {
        return r;
    } else if (sa < sb) {
        return -1;
    } else if (sa == sb) {
        return 0;
    } else {
        return 1;
    }
}

} // namespace static_index
