#ifndef OTS_STATIC_INDEX_RANDOM_H
#define OTS_STATIC_INDEX_RANDOM_H

#include "logging_assert.h"
#include <stdint.h>

namespace static_index {

class Logger;

class IRandom : public Loggable
{
public:
    virtual ~IRandom()
    {}

    virtual int64_t Next() =0;
    virtual int64_t Max() const =0;
    virtual int64_t Seed() const =0;
};

IRandom* NewDefaultRandom(Logger*);
IRandom* NewDefaultRandom(Logger*, uint64_t seed);

int64_t NextInt(IRandom*, int64_t exclusiveUpper);
int64_t NextInt(IRandom*, int64_t inclusiveLower, int64_t exclusiveUpper);

} // namespace static_index

#endif /* OTS_STATIC_INDEX_RANDOM_H */
