#include "security.h"
#include "random.h"
#include <limits>
#include <cstdio>

using namespace ::std;

namespace static_index {

void FillUuid(IRandom* rnd, string* out)
{
    const char format[] = "%08x-%04x-%04x-%04x-%04x%08x";
    const uint32_t kLeastUint16Mask = 0x0ffff;

    uint32_t r1 = NextInt(rnd, 0x100000000ll);
    uint32_t r2 = NextInt(rnd, 0x100000000ll);
    uint32_t r3 = NextInt(rnd, 0x100000000ll);
    uint32_t r4 = NextInt(rnd, 0x100000000ll);

    int64_t sz = out->size();
    out->resize(sz + 36);
    char* buf = const_cast<char*>(out->data()) + sz;
    snprintf(buf, 37, format,
        r1,
        (uint16_t) (r2 >> 16), (uint16_t)(r2 & kLeastUint16Mask),
        (uint16_t) (r3 >> 16), (uint16_t)(r3 & kLeastUint16Mask),
        r4);
}

} // namespace static_index
