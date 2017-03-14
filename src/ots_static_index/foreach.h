#ifndef OTS_STATIC_INDEX_FOREACH_H
#define OTS_STATIC_INDEX_FOREACH_H

#define FOREACH_ITER(it, container)\
    for(typeof((container).begin()) it((container).begin()); it != (container).end(); ++it)

#endif /* OTS_STATIC_INDEX_FOREACH_H */
