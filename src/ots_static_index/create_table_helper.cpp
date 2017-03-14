#include "ots_static_index/logger.h"
#include "def_ast.h"
#include "timestamp.h"
#include "logging_assert.h"
#include "string_tools.h"
#include "slice.h"
#include "foreach.h"
#include "ots/ots_client.h"
#include "jsoncpp/json/value.h"
#include "jsoncpp/json/reader.h"
#include <tr1/tuple>
#include <fstream>
#include <memory>
#include <string>
#include <deque>
#include <cstdio>
#include <cstdlib>
#include <cassert>

using namespace ::std;
using namespace ::std::tr1;
using namespace ::static_index;

namespace {

bool AnyHelp(const deque<string>& args)
{
    FOREACH_ITER(i, args) {
        Slice arg = ToSlice(*i);
        if (Prefix(arg, ToSlice("-h")) || Prefix(arg, ToSlice("--help"))) {
            return true;
        }
    }
    return false;
}

void Help(char* exe)
{
    printf("%s --schema=schema.json --config=config.json\n", exe);
    printf("--schema=schema.json\tspecify a schema file\n");
    printf("--config=config.json\tspecify a config file, see config.json.template for an example\n");
}

string ParseFilename(const deque<string>& args, const string& expect)
{
    Slice prefix = ToSlice(expect);
    FOREACH_ITER(i, args) {
        Slice arg = ToSlice(*i);
        if (Prefix(arg, prefix)) {
            return arg.Subslice(prefix.Size()).ToString();
        }
    }
    return string();
}

Json::Value ParseSchema(const deque<string>& args)
{
    const string& fn = ParseFilename(args, "--schema=");
    if (fn.empty()) {
        return Json::Value();
    }
    ifstream fin(fn.c_str());
    if (!fin) {
        fprintf(stderr, "cannot read schema file: %s\n", fn.c_str());
        _Exit(1);
    }
    Json::Value res;
    Json::Reader b;
    bool ok = b.parse(fin, res, false);
    if (!ok) {
        fprintf(stderr, "Error from parse schema");
        _Exit(1);
    }
    return res;
}


Json::Value ParseConfig(const deque<string>& args)
{
    const string& fn = ParseFilename(args, "--config=");
    if (fn.empty()) {
        return Json::Value();
    }
    ifstream fin(fn.c_str());
    if (!fin) {
        fprintf(stderr, "cannot read config file: %s\n", fn.c_str());
        _Exit(1);
    }
    Json::Value res;
    Json::Reader b;
    bool ok = b.parse(fin, res, false);
    if (!ok) {
        fprintf(stderr, "Error from parse config");
        _Exit(1);
    }
    return res;
}

class ScreenSyncLogger : public Logger
{
    Logger::LogLevel mLogLevel;

public:
    explicit ScreenSyncLogger(Logger::LogLevel lvl)
      : mLogLevel(lvl)
    {
    }
    ~ScreenSyncLogger()
    {
    }

    Logger::LogLevel GetLogLevel() const
    {
        return mLogLevel;
    }

    void Record(Logger::LogLevel, const string& rec)
    {
        int ret = printf("%s\n", rec.c_str());
        assert(ret >= 0);
        ret = fflush(stdout);
        assert(ret == 0);
        (void) ret;
    }
    void Flush()
    {}
};

aliyun::openservices::ots::OTSClient* NewOTSClient(const Json::Value& cfg)
{
    aliyun::openservices::ots::OTSConfig ocfg;
    ocfg.mEndPoint = cfg["EndPoint"].asString();
    ocfg.mAccessId = cfg["AccessId"].asString();
    ocfg.mAccessKey = cfg["AccessKey"].asString();
    ocfg.mInstanceName = cfg["InstanceName"].asString();
    ocfg.mRequestTimeout = 5;
    ocfg.mMaxErrorRetry = 0;
    ocfg.mRequestCompressType = aliyun::openservices::ots::COMPRESS_NO;
    ocfg.mResponseCompressType = aliyun::openservices::ots::COMPRESS_NO;
    return new aliyun::openservices::ots::OTSClient(ocfg);
}

aliyun::openservices::ots::ColumnType ToOTSValueType(Logger* logger, Value::Type ty)
{
    switch(ty) {
    case Value::INTEGER: return aliyun::openservices::ots::INTEGER;
    case Value::STRING: return aliyun::openservices::ots::STRING;
    case Value::BOOLEAN: return aliyun::openservices::ots::BOOLEAN;
    case Value::DOUBLE: return aliyun::openservices::ots::DOUBLE;
    default:
        OTS_ASSERT(logger, false)((int) ty).What("unknown value type");
        return aliyun::openservices::ots::INTEGER;
    }
}

void ParsePkeys(
    Logger* logger,
    deque<tuple<string, aliyun::openservices::ots::ColumnType> >* pkeys,
    const deque<shared_ptr<ast::Node> >& pkey)
{
    FOREACH_ITER(i, pkey) {
        tuple<string, aliyun::openservices::ots::ColumnType> pk;
        const string& name = (*i)->mName;
        get<0>(pk) = name;
        get<1>(pk) = ToOTSValueType(logger, (*i)->mDataType);
        pkeys->push_back(pk);
    }
}

string PrettyPrint(
    const deque<tuple<string, aliyun::openservices::ots::ColumnType> >& pkeys)
{
    string res;
    FOREACH_ITER(i, pkeys) {
        if (!res.empty()) {
            res.push_back(',');
        }
        string name;
        aliyun::openservices::ots::ColumnType type;
        tie(name, type) = *i;
        res.append(name);
        res.push_back(':');
        switch(type) {
        case aliyun::openservices::ots::INTEGER: {
            res.append("Int");
            break;
        }
        case aliyun::openservices::ots::STRING: {
            res.append("Str");
            break;
        }
        case aliyun::openservices::ots::DOUBLE: {
            res.append("Double");
            break;
        }
        case aliyun::openservices::ots::BOOLEAN: {
            res.append("Bool");
            break;
        }
        default: {
            res.append(ToString((int) type));
        }
        }
    }
    return res;
}

void CreateTable(
    Logger* logger,
    aliyun::openservices::ots::OTSClient* client,
    const TableMeta& meta)
{
    const string& tblName = meta.mTableName;
    OTS_LOG_DEBUG(logger)(tblName).What("create table");
    deque<tuple<string, aliyun::openservices::ots::ColumnType> > pkeys;
    ParsePkeys(logger, &pkeys, meta.mPrimaryKey);
    OTS_LOG_INFO(logger)(tblName)(PrettyPrint(pkeys)).What("creating table");

    aliyun::openservices::ots::OTSFactory fac;
    auto_ptr<aliyun::openservices::ots::CreateTableRequest> req(
        fac.NewCreateTableRequest());

    auto_ptr<aliyun::openservices::ots::ReservedThroughput> rt(
        fac.NewReservedThroughput());
    auto_ptr<aliyun::openservices::ots::CapacityUnit> cu(
        fac.NewCapacityUnit(0, 0));
    rt->SetCapacityUnit(cu.release());
    req->SetReservedThroughput(rt.release());

    auto_ptr<aliyun::openservices::ots::TableMeta> tm(fac.NewTableMeta());
    tm->SetTableName(tblName);
    FOREACH_ITER(i, pkeys) {
        string name;
        aliyun::openservices::ots::ColumnType type;
        tie(name, type) = *i;
        auto_ptr<aliyun::openservices::ots::ColumnSchema> cs(
            fac.NewColumnSchema(name, type));
        tm->AddPrimaryKeySchema(cs.release());
    }
    req->SetTableMeta(tm.release());

    SleepFor(Interval::FromSec(logger, 1));
    try {
        client->CreateTable(*req);
    }
    catch(const aliyun::openservices::ots::OTSException& ex) {
        if (ex.GetErrorCode() != "OTSObjectAlreadyExist") {
            throw;
        } else {
            OTS_LOG_INFO(logger)
                (tblName)
                .What("Table has already been there. Reconstruct it.");
            client->DeleteTable(tblName);
            client->CreateTable(*req);
        }
    }
}

void Create(
    Logger* logger, 
    aliyun::openservices::ots::OTSClient* client,
    const CollectionMeta& schema)
{
    FOREACH_ITER(i, schema.mTableMetas) {
        CreateTable(logger, client, i->second);
    }
}

} // namespace

int main(int argc, char** argv) {
    deque<string> args;
    for(int i = 1; i < argc; ++i) {
        args.push_back(string(argv[i]));
    }

    if (args.empty()) {
        Help(argv[0]);
        return 1;
    }
    if (AnyHelp(args)) {
        Help(argv[0]);
        return 0;
    }

    const Json::Value& schema = ParseSchema(args);
    if (schema.isNull()) {
        Help(argv[0]);
        return 1;
    }

    const Json::Value& config = ParseConfig(args);
    if (config.isNull()) {
        Help(argv[0]);
        return 1;
    }

    ScreenSyncLogger logger(Logger::INFO);
    auto_ptr<aliyun::openservices::ots::OTSClient> otsclient(NewOTSClient(config));
    OTS_ASSERT(&logger, schema.isArray());
    for(int i = 0, sz = schema.size(); i < sz; ++i) {
        const Json::Value& sub = schema[i];
        CollectionMeta cmeta;
        ParseSchema(&logger, &cmeta, sub);
        Create(&logger, otsclient.get(), cmeta);
    }

    return 0;
}
