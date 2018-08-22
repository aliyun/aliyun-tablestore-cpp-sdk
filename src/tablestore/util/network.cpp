#include "network.hpp"
#include "tablestore/util/assert.hpp"
#include <cstring>
#if defined(_MSC_VER)
#include <winsock.h>
#else
#include <cerrno>
extern "C" {
#include <unistd.h>
}
#endif

using namespace std;

namespace aliyun {
namespace tablestore {
namespace util {

string getHostName()
{
    string res;
    // SUSv2 or Windows guarantees that "Host names are limited to 255 bytes".
    res.reserve(256); 
    int r = gethostname(const_cast<char*>(res.data()), res.capacity());
#if defined(_MSC_VER)
    OTS_ASSERT(r == 0)
        (r)
        (WSAGetLastError())
        .what("failed to fetch hostname.");
#else
    OTS_ASSERT(r == 0)
        (r)
        (errno)
        .what("failed to fetch hostname.");
#endif
    size_t n = strnlen(res.data(), res.capacity());
    res.resize(n);
    return res;
}

} // namespace util
} // namespace tablestore
} // namespace aliyun
