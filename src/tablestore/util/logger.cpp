/*
BSD 3-Clause License

Copyright (c) 2017, Alibaba Cloud
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
#include "logger.hpp"
#include "tablestore/util/mempiece.hpp"
#include "tablestore/util/threading.hpp"
#include "tablestore/util/foreach.hpp"
#include <boost/thread/thread.hpp>
#include <map>
#include <cstdio>

using namespace std;
using namespace std::tr1;

namespace aliyun {
namespace tablestore {
namespace util {

namespace {

class SinkerCenterImpl: public SinkerCenter
{
public:
    explicit SinkerCenterImpl();
    ~SinkerCenterImpl();

    Sinker* registerSinker(const string&, Sinker*);
    void flushAll();

private:
    typedef map<string, Sinker*, QuasiLexicographicLess<string> > Sinkers;

private:
    Mutex mMutex;
    Sinkers mSinkers;
};

SinkerCenterImpl::SinkerCenterImpl()
{}

SinkerCenterImpl::~SinkerCenterImpl()
{
    ScopedLock lock(mMutex);
    FOREACH_ITER(i, mSinkers) {
        delete i->second;
    }
}

Sinker* SinkerCenterImpl::registerSinker(const string& key, Sinker* sinker)
{
    Sinker* old = NULL;
    {
        ScopedLock lock(mMutex);
        Sinkers::iterator i = mSinkers.find(key);
        if (i == mSinkers.end()) {
            mSinkers.insert(make_pair(key, sinker));
        } else {
            old = i->second;
        }
    }
    return old;
}

void SinkerCenterImpl::flushAll()
{
    deque<Sinker*> copy;
    {
        ScopedLock lock(mMutex);
        FOREACH_ITER(i, mSinkers) {
            copy.push_back(i->second);
        }
    }
    for(; !copy.empty(); copy.pop_front()) {
        copy.front()->flush();
    }
}

} // namespace

shared_ptr<SinkerCenter> SinkerCenter::singleton()
{
    static shared_ptr<SinkerCenter> sCenter(new SinkerCenterImpl());
    return sCenter;
}


namespace {

struct RecordImpl: public Record
{
    shared_ptr<Semaphore> mFlusher;

    Logger::LogLevel mLogLevel;
    UtcTime mTimestamp;
    boost::thread::id mTid;
    string mMsg;

    explicit RecordImpl()
      : mLogLevel(Logger::kDebug)
    {}

    explicit RecordImpl(const string& msg)
      : mLogLevel(Logger::kDebug),
        mTimestamp(),
        mMsg(msg)
    {}

    void prettyPrint(string& out) const;

    bool isFlusher() const
    {
        return mFlusher.get() != NULL;
    }

    void flush()
    {
        mFlusher->post();
    }
};

void RecordImpl::prettyPrint(string& out) const
{
    out.append("TS:");
    out.append(mTimestamp.toIso8601());

    out.append("\tTID:");
    ostringstream oss;
    oss << mTid;
    out.append(oss.str());

    out.append("\tLEVEL=");
    switch(mLogLevel) {
    case Logger::kDebug:
        out.append("Logger::kDebug");
        break;
    case Logger::kInfo:
        out.append("Logger::kInfo");
        break;
    case Logger::kError:
        out.append("Logger::kError");
        break;
    }

    out.append("\t");
    out.append(mMsg);
}

class StdErrSinkerImpl: public Sinker
{

public:
    explicit StdErrSinkerImpl();
    ~StdErrSinkerImpl();

    void sink(Record*);
    void flush();

private:
    void sinker();

private:
    boost::atomic<bool> mStopper;
    Semaphore mSinkerSem;
    Thread mSinkerThread;

    Mutex mMutex;
    deque<RecordImpl*> mRecords;
};

StdErrSinkerImpl::StdErrSinkerImpl()
  : mStopper(false),
    mSinkerSem(0)
{
    Thread t(bind(&StdErrSinkerImpl::sinker, this));
    moveAssign(mSinkerThread, util::move(t));
}

StdErrSinkerImpl::~StdErrSinkerImpl()
{
    flush();
    mStopper.store(true, boost::memory_order_release);
    mSinkerSem.post();
    mSinkerThread.join();

    for(; !mRecords.empty(); mRecords.pop_front()) {
        delete mRecords.front();
    }
}

void StdErrSinkerImpl::sinker()
{
    Duration interval = Duration::fromMsec(1);
    string buf;
    for(;;) {
        mSinkerSem.waitFor(interval);
        if (mStopper.load(boost::memory_order_acquire)) {
            break;
        }

        deque<RecordImpl*> records;
        {
            ScopedLock _(mMutex);
            std::swap(records, mRecords);
        }
        string buf;
        for(; !records.empty(); records.pop_front()) {
            RecordImpl* rec = records.front();
            if (rec->isFlusher()) {
                fflush(stderr);
                rec->flush();
            } else {
                buf.clear();
                pp::prettyPrint(buf, *rec);
                fprintf(stderr, "%s\n", buf.c_str());
            }
            delete rec;
        }
    }
}

void StdErrSinkerImpl::sink(Record* rec)
{
    RecordImpl* recImpl = dynamic_cast<RecordImpl*>(rec);
    OTS_ASSERT(recImpl != NULL);

    ScopedLock lock(mMutex);
    recImpl->mTimestamp = UtcTime::now();
    mRecords.push_back(recImpl);
}

void StdErrSinkerImpl::flush()
{
    shared_ptr<Semaphore> sem(new Semaphore(0));
    {
        RecordImpl* recImpl = new RecordImpl();
        recImpl->mFlusher = sem;
        ScopedLock lock(mMutex);
        mRecords.push_back(recImpl);
    }
    mSinkerSem.post();
    sem->wait();
}

class DefaultLogger: public Logger
{
public:
    explicit DefaultLogger(
        const string& key,
        Logger::LogLevel lvl,
        Sinker* sinker)
      : mKey(key),
        mLevel(lvl),
        mSinker(sinker)
    {}

public:
    LogLevel level() const
    {
        return mLevel;
    }

    void record(LogLevel lvl, const string& msg)
    {
        RecordImpl* recImpl = new RecordImpl();
        recImpl->mMsg = msg;
        recImpl->mLogLevel = lvl;
        recImpl->mTid = boost::this_thread::get_id();
        mSinker->sink(recImpl);
    }

    Logger* spawn(const string& key)
    {
        return spawn(key, mLevel);
    }
    Logger* spawn(const string& key, LogLevel lvl)
    {
        string subkey;
        subkey.reserve(mKey.size() + key.size() + 2);
        subkey.append(mKey);
        subkey.push_back('/');
        subkey.append(key);
        return new DefaultLogger(key, lvl, mSinker);
    }

private:
    const string mKey;
    const LogLevel mLevel;
    Sinker* mSinker;
};

} // namespace

Logger* createLogger(const std::string& loggerKey, Logger::LogLevel lvl)
{
    shared_ptr<SinkerCenter> center = SinkerCenter::singleton();
    Sinker* sinker = new StdErrSinkerImpl();
    Sinker* old = center->registerSinker("StdErrSinkerImpl", sinker);
    if (old == NULL) {
        return new DefaultLogger(loggerKey, lvl, sinker);
    } else {
        delete sinker;
        return new DefaultLogger(loggerKey, lvl, old);
    }
}

} // namespace util
} // namespace tablestore
} // namespace aliyun

