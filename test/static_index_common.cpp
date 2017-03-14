#include "static_index_common.h"
#include <tr1/tuple>
#include <sstream>
#include <iostream>

using namespace ::std;
using namespace ::std::tr1;

static_index::Logger::LogLevel DummyLogger::GetLogLevel() const
{
    return Logger::DEBUG;
}

void DummyLogger::Record(LogLevel, const std::string& rec)
{}

void DummyLogger::Flush()
{}


FileSyncLogger::FileSyncLogger(const std::string& fn)
{
    mFp = fopen(fn.c_str(), "w");
    assert(mFp != NULL);
}

FileSyncLogger::~FileSyncLogger()
{
    int ret = fclose(mFp);
    assert(ret == 0);
    (void) ret;
}

static_index::Logger::LogLevel FileSyncLogger::GetLogLevel() const
{
    return static_index::Logger::DEBUG;
}

void FileSyncLogger::Record(static_index::Logger::LogLevel, const std::string& rec)
{
    int ret = fprintf(mFp, "%s\n", rec.c_str());
    assert(ret >= 0);
    ret = fflush(mFp);
    assert(ret == 0);
    (void) ret;
}

void FileSyncLogger::Flush()
{}

Json::Value FromJsonStr(const string& str)
{
    istringstream in(str);
    Json::Value res;
    in >> res;
    return res;
}

static_index::Exceptional MockGetRange(
    static_index::GetRangeResponse* resp,
    const static_index::GetRangeRequest& req,
    static_index::Logger* logger,
    map<Json::Value, int64_t>* hits,
    const map<Json::Value, static_index::GetRangeResponse>& resps)
{
    Json::Value jReq = Jsonize(logger, req);
    jReq.removeMember("Tracker");
    map<Json::Value, static_index::GetRangeResponse>::const_iterator it =
        resps.find(jReq);
    OTS_ASSERT(logger, it != resps.end())(jReq.toStyledString());
    *resp = it->second;
    ++(*hits)[jReq];
    return static_index::Exceptional();
}

static_index::Exceptional MockBatchGetRow(
    static_index::BatchGetRowResponse* resp,
    const static_index::BatchGetRowRequest& req,
    static_index::Logger* logger,
    map<Json::Value, int64_t>* hits,
    const map<Json::Value, static_index::BatchGetRowResponse>& resps)
{
    Json::Value jReq = Jsonize(logger, req);
    jReq.removeMember("Tracker");
    for(int i = 0, sz = jReq["GetRows"].size(); i < sz; ++i) {
        jReq["GetRows"][i].removeMember("Tracker");
    }
    map<Json::Value, static_index::BatchGetRowResponse>::const_iterator it =
        resps.find(jReq);
    OTS_ASSERT(logger, it != resps.end())(jReq.toStyledString());
    *resp = it->second;
    OTS_ASSERT(logger, resp->mGetRows.size() == req.mGetRows.size())
        (resp->mGetRows.size())
        (req.mGetRows.size());
    for(int i = 0, sz = req.mGetRows.size(); i < sz; ++i) {
        get<2>(resp->mGetRows[i]) = get<1>(req.mGetRows[i]);
    }
    ++(*hits)[jReq];
    return static_index::Exceptional();
}

static_index::Exceptional MockBatchWrite(
    static_index::BatchWriteResponse* resp,
    const static_index::BatchWriteRequest& req,
    static_index::Logger* logger,
    map<Json::Value, int64_t>* hits,
    const map<Json::Value, static_index::BatchWriteResponse>& resps)
{
    Json::Value jReq = Jsonize(logger, req);
    jReq.removeMember("Tracker");
    if (jReq.isMember("PutRows")) {
        for(int i = 0, sz = jReq["PutRows"].size(); i < sz; ++i) {
            jReq["PutRows"][i].removeMember("Tracker");
        }
    }
    if (jReq.isMember("DelRows")) {
        for(int i = 0, sz = jReq["DelRows"].size(); i < sz; ++i) {
            jReq["DelRows"][i].removeMember("Tracker");
        }
    }

    map<Json::Value, static_index::BatchWriteResponse>::const_iterator it =
        resps.find(jReq);
    OTS_ASSERT(logger, it != resps.end())(static_index::ToString(jReq));
    *resp = it->second;
    OTS_ASSERT(logger, resp->mPutRows.size() == req.mPutRows.size())
        (resp->mPutRows.size())
        (req.mPutRows.size());
    for(int64_t i = 0, sz = req.mPutRows.size(); i < sz; ++i) {
        get<1>(resp->mPutRows[i]) = get<1>(req.mPutRows[i]);
    }
    OTS_ASSERT(logger, resp->mDelRows.size() == req.mDelRows.size())
        (resp->mDelRows.size())
        (req.mDelRows.size());
    for(int64_t i = 0, sz = req.mDelRows.size(); i < sz; ++i) {
        get<1>(resp->mDelRows[i]) = get<1>(req.mDelRows[i]);
    }
    ++(*hits)[jReq];
    return static_index::Exceptional();
}
