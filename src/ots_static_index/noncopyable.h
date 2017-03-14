#ifndef OTS_STATIC_INDEX_NONCOPYABLE_H
#define OTS_STATIC_INDEX_NONCOPYABLE_H

namespace static_index {

class Noncopyable
{
private:
    Noncopyable(const Noncopyable&);
    Noncopyable& operator=(const Noncopyable&);

public:
    Noncopyable()
    {}
};

} // namespace static_index

#endif /* OTS_STATIC_INDEX_NONCOPYABLE_H */
