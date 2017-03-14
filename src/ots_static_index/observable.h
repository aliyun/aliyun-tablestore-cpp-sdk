#ifndef OTS_STATIC_INDEX_OBSERVABLE_H
#define OTS_STATIC_INDEX_OBSERVABLE_H

#include "threading.h"
#include "foreach.h"
#include <tr1/memory>
#include <deque>
#include <set>

namespace static_index {
class Logger;

template<typename Out, typename Err>
class Observable;

template<typename In, typename Err>
class Observer
{
protected:
    typedef Observable<In, Err> Upstream;

public:
    virtual ~Observer() {}

    virtual void OnObserve(Upstream*)
    {}
    virtual void OnNext(Upstream*, const In&) =0;
    virtual void OnCompletion(Upstream*) =0;
    virtual void OnError(Upstream*, const Err&) =0;
};

template<typename Out, typename Err>
class Observable
{
protected:
    typedef Observer<Out, Err> Downstream;
    
public:
    virtual ~Observable() {}

    virtual void ObserveBy(const ::std::tr1::shared_ptr<Downstream>& d) =0;
};

template<typename In, typename Err>
class DefaultObserver: public Observer<In, Err>
{
    typedef typename Observer<In, Err>::Upstream Upstream;

protected:
    Mutex mMutex;
    ::std::set<Upstream*> mUpstreams;

public:
    explicit DefaultObserver(Logger* logger)
      : mMutex(logger)
    {}

    void OnObserve(Upstream* up)
    {
        Scoped<Mutex> g(&mMutex);
        mUpstreams.insert(up);
    }
    void OnNext(Upstream* up, const In& in)
    {
        {
            Scoped<Mutex> g(&mMutex);
            if (mUpstreams.count(up) == 0) {
                return;
            }
        }
        OnInnerNext(in);
    }
    void OnCompletion(Upstream* up)
    {
        {
            Scoped<Mutex> g(&mMutex);
            int64_t erased = mUpstreams.erase(up);
            if (erased == 0) {
                return;
            }
            if (!mUpstreams.empty()) {
                return;
            }
        }
        OnInnerCompletion();
    }
    void OnError(Upstream* up, const Err& err)
    {
        {
            Scoped<Mutex> g(&mMutex);
            if (mUpstreams.count(up) == 0) {
                return;
            }
            mUpstreams.clear();
        }
        OnInnerError(err);
    }

protected:
    virtual void OnInnerNext(const In&) =0;
    virtual void OnInnerCompletion() =0;
    virtual void OnInnerError(const Err&) =0;
};

template<typename Out, typename Err>
class DefaultObservable: public Observable<Out, Err>
{
    typedef typename Observable<Out, Err>::Downstream Downstream;

protected:
    ::std::deque< ::std::tr1::shared_ptr<Downstream> > mDownstreams;

public:
    void ObserveBy(const ::std::tr1::shared_ptr<Downstream>& d)
    {
        mDownstreams.push_back(d);
        d->OnObserve(this);
    }

protected:
    void NotifyNext(const Out& res)
    {
        FOREACH_ITER(i, mDownstreams) {
            (*i)->OnNext(this, res);
        }
    }

    void NotifyCompletion()
    {
        FOREACH_ITER(i, mDownstreams) {
            (*i)->OnCompletion(this);
        }
        mDownstreams.clear();
    }

    void NotifyError(const Err& err)
    {
        FOREACH_ITER(i, mDownstreams) {
            (*i)->OnError(this, err);
        }
        mDownstreams.clear();
    }
};

} // namespace static_index

#endif /* OTS_STATIC_INDEX_OBSERVABLE_H */
