#ifndef ALIYUN_OTS_STATIC_INDEX_THREAD_POOL_H
#define ALIYUN_OTS_STATIC_INDEX_THREAD_POOL_H

#include <tr1/functional>
#include <stdint.h>

namespace static_index {

class Logger;

class ThreadPool
{
public:
    typedef ::std::tr1::function<void ()> Closure;

    /**
     * 创建一个线程池。
     */
    static ThreadPool* New(Logger*, int64_t threadSize, int64_t queueSize);

    virtual ~ThreadPool()
    {}

    /**
     * 添加一个任务。
     *
     * 如果添加成功，会返回true；否则返回false。
     */
    virtual bool TryEnqueue(const Closure&) =0;

    /**
     * 待处理的任务数。
     */
    virtual int64_t CountItems() const =0;
};

} // namespace static_index

#endif /* ALIYUN_OTS_STATIC_INDEX_THREAD_POOL_H */
