#include "logging_assert.h"
#include "ots_static_index/logger.h"
#include <cstdlib>
#include <cstdio>
#include <stdint.h>

using namespace ::std;

namespace static_index {

int64_t OTSFLAG_ASSERT_ABORT = 1; // 0: do nothing, 1: abort; 2: exit

namespace details {

namespace {

int64_t CalcSize(
    const string& file,
    const string& line,
    const string& func,
    const string& what,
    const vector<pair<string, string> >& xs)
{
    int64_t res = 0;

    res += 5; // "FILE:"
    res += file.size();
    res += 2; // ", "
    res += 5; // "LINE:"
    res += line.size();
    res += 2; // ", "
    res += 9; // "FUNCTION:"
    res += func.size();
    res += 2; // ", "
    
    if (!what.empty()) {
        res += 5; // "what:"
        res += what.size();
        if (!xs.empty()) {
            res += 2; // ", "
        }
    }
    
    if (!xs.empty()) {
        res += (xs.size() - 1) * 2; // ", "
        for(int64_t i = 0, sz = xs.size(); i < sz; ++i) {
            res += xs[i].first.size();
            res += 1; // ":"
            res += xs[i].second.size();
        }
    }
    
    if (res < 0) {
        ::abort();
    }
    return res;
}

} // namespace

LogHelper::~LogHelper()
{
    const string kSep(", ");

    int64_t size = CalcSize(mFile, mLine, mFunc, mWhat, mValues);
    string res;
    res.reserve(size + 1); // +1 for tailing '\0'

    res.append("FILE:");
    res.append(mFile);
    res.append(kSep);

    res.append("LINE:");
    res.append(mLine);
    res.append(kSep);

    res.append("FUNCTION:");
    res.append(mFunc);
    res.append(kSep);
    
    if (!mWhat.empty()) {
        res.append("What:");
        res.append(mWhat);
        if (!mValues.empty()) {
            res.append(", ");
        }
    }

    for(int64_t i = 0, sz = mValues.size(); i < sz; ++i) {
        if (i > 0) {
            res.append(", ");
        }
        res.append(mValues[i].first);
        res.push_back(':');
        res.append(mValues[i].second);
    }

    mLogger->Record(mLevel, res);
    if (mAssert) {
        mLogger->Flush();
        if (OTSFLAG_ASSERT_ABORT == 1) {
            ::fprintf(::stderr, "Assertion fails: %s\n", res.c_str());
            ::abort();
        } else if (OTSFLAG_ASSERT_ABORT == 2) {
            ::fprintf(::stderr, "Assertion fails: %s\n", res.c_str());
            ::_Exit(1);
        }
    }
}

} // namespace details

} // namespace static_index

