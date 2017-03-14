#include "static_index_common.h"

using namespace ::std;
using namespace ::std::tr1;
using namespace ::std::tr1::placeholders;
using namespace ::testing;
using namespace ::aliyun::openservices::ots;
using namespace ::static_index;

namespace {

void AssertEq(
    Logger* logger,
    const vector<Json::Value>& real,
    const set<string>& expect)
{
    OTS_LOG_DEBUG(logger)(real.size()).What("AssertEq");
    FOREACH_ITER(i, real) {
        OTS_LOG_DEBUG(logger)(ToString(*i));
    }
    OTS_ASSERT(logger, real.size() == expect.size())
        (expect.size());
    FOREACH_ITER(i, real) {
        const string& s = ToString(*i);
        OTS_ASSERT(logger, expect.count(s) > 0)(s);
    }
}

} // namespace

TEST(Find, ScanAll)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ],"
        "    \"RequiredAttributes\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ]"
        "  }"
        "}"
        "]";
    FileSyncLogger logger("Find.ScanAll.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    static_index::GetRangeRequest req;
    static_index::GetRangeResponse resp;
    const char kResponse[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {"
        "          \"Name\": \"pkey\","
        "          \"Value\": 0"
        "        }"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {"
        "          \"Name\": \"pkey\","
        "          \"Value\": 1"
        "        }"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {"
        "          \"Name\": \"pkey\","
        "          \"Value\": 2"
        "        }"
        "      ]"
        "    }"
        "  ]"
        "}";
    Unjsonize(&logger, &resp, FromJsonStr(kResponse));
    EXPECT_CALL(client, GetRange(_, _))
        .WillOnce(DoAll(SetArgPointee<0>(resp), SaveArg<1>(&req), Return(Exceptional())));
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));
    
    vector<Json::Value> result;
    Json::Value condition = Json::objectValue;
    Json::Value projection = Json::arrayValue;
    Json::Value order = Json::objectValue;
    int r = sind->Find(
        "test",
        projection,
        condition,
        0,
        0,
        order,
        result);
    OTS_ASSERT(&logger, r == FICUS_SUCC);
    ASSERT_EQ(result.size(), 3ull);
    EXPECT_EQ(result[0].toStyledString(), "{\n   \"pkey\" : 0\n}\n");
    EXPECT_EQ(result[1].toStyledString(), "{\n   \"pkey\" : 1\n}\n");
    EXPECT_EQ(result[2].toStyledString(), "{\n   \"pkey\" : 2\n}\n");

    EXPECT_EQ(Jsonize(&logger, req).toStyledString(), "{\n"
        "   \"Limit\" : 5000,\n"
        "   \"Start\" : [\n"
        "      {\n"
        "         \"Name\" : \"pkey\",\n"
        "         \"Value\" : \"-inf\"\n"
        "      }\n"
        "   ],\n"
        "   \"Stop\" : [\n"
        "      {\n"
        "         \"Name\" : \"pkey\",\n"
        "         \"Value\" : \"+inf\"\n"
        "      }\n"
        "   ],\n"
        "   \"TableName\" : \"test\"\n"
        "}\n");
}

TEST(Find, WrongTable)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ],"
        "    \"RequiredAttributes\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ]"
        "  }"
        "}"
        "]";
    FileSyncLogger logger("Find.WrongTable.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));
    vector<Json::Value> result;
    Json::Value condition = Json::objectValue;
    Json::Value projection = Json::arrayValue;
    Json::Value order = Json::objectValue;
    int r = sind->Find(
        "hoho",
        projection,
        condition,
        0,
        0,
        order,
        result);
    OTS_ASSERT(&logger, r != FICUS_SUCC);
}

TEST(Find, Continuation)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ],"
        "    \"RequiredAttributes\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ]"
        "  }"
        "}"
        "]";
    FileSyncLogger logger("Find.Continuation.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    static_index::GetRangeRequest req;
    map<Json::Value, static_index::GetRangeResponse> resps;
    const char kRequest0[] = "{"
        "  \"TableName\": \"test\","
        "  \"Limit\": 5000,"
        "  \"Start\": ["
        "    {\"Name\": \"pkey\", \"Value\": \"-inf\"}"
        "  ],"
        "  \"Stop\": ["
        "    {\"Name\": \"pkey\", \"Value\": \"+inf\"}"
        "  ]"
        "}";
    const char kResponse0[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {"
        "          \"Name\": \"pkey\","
        "          \"Value\": 0"
        "        }"
        "      ]"
        "    }"
        "  ],"
        "  \"Next\": ["
        "    {\"Name\": \"pkey\", \"Value\": 1}"
        "  ]"
        "}";
    const char kRequest1[] = "{"
        "  \"TableName\": \"test\","
        "  \"Limit\": 5000,"
        "  \"Start\": ["
        "    {\"Name\": \"pkey\", \"Value\": 1}"
        "  ],"
        "  \"Stop\": ["
        "    {\"Name\": \"pkey\", \"Value\": \"+inf\"}"
        "  ]"
        "}";
    const char kResponse1[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {"
        "          \"Name\": \"pkey\","
        "          \"Value\": 1"
        "        }"
        "      ]"
        "    }"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest1)]), FromJsonStr(kResponse1));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, GetRange(_, _))
        .WillRepeatedly(Invoke(bind(&MockGetRange, _1, _2, &logger, &hits, cref(resps))));
    
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));
    vector<Json::Value> result;
    Json::Value condition = Json::objectValue;
    Json::Value projection = Json::arrayValue;
    Json::Value order = Json::objectValue;
    int r = sind->Find(
        "test",
        projection,
        condition,
        0,
        0,
        order,
        result);
    OTS_ASSERT(&logger, r == FICUS_SUCC);
    ASSERT_EQ(result.size(), 2ull);
    EXPECT_EQ(result[0].toStyledString(), "{\n   \"pkey\" : 0\n}\n");
    EXPECT_EQ(result[1].toStyledString(), "{\n   \"pkey\" : 1\n}\n");
    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

TEST(Find, Composited)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"hash_pkey\","
        "        \"Type\": \"Composited\","
        "        \"Definition\": \"($Hex pkey)\""
        "      }"
        "    ],"
        "    \"RequiredAttributes\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ]"
        "  }"
        "}"
        "]";
    FileSyncLogger logger("Find.Composited.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    static_index::GetRangeRequest req;
    map<Json::Value, static_index::GetRangeResponse> resps;
    const char kRequest0[] = "{"
        "  \"TableName\": \"test\","
        "  \"Limit\": 5000,"
        "  \"Start\": ["
        "    {\"Name\": \"hash_pkey\", \"Value\": \"0000000000000000\"}"
        "  ],"
        "  \"Stop\": ["
        "    {\"Name\": \"hash_pkey\", \"Value\": \"FFFFFFFFFFFFFFFF\\u0000\"}"
        "  ]"
        "}";
    const char kResponse0[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {"
        "          \"Name\": \"hash_pkey\","
        "          \"Value\": \"0000000000000000\""
        "        }"
        "      ],"
        "      \"Attributes\": ["
        "        {"
        "          \"Name\": \"pkey\","
        "          \"Value\": 0"
        "        }"
        "      ]"
        "    }"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, GetRange(_, _))
        .WillRepeatedly(Invoke(bind(&MockGetRange, _1, _2, &logger, &hits, cref(resps))));
    
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));
    vector<Json::Value> result;
    Json::Value condition = Json::objectValue;
    Json::Value projection = Json::arrayValue;
    Json::Value order = Json::objectValue;
    int r = sind->Find(
        "test",
        projection,
        condition,
        0,
        0,
        order,
        result);
    ASSERT_EQ(r, FICUS_SUCC);
    ASSERT_EQ(result.size(), 1ull);
    EXPECT_EQ(ToString(result[0]), "{\"pkey\":0}\n");
    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

TEST(Find, Projection)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ],"
        "    \"RequiredAttributes\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ]"
        "  }"
        "}"
        "]";
    FileSyncLogger logger("Find.Projection.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    static_index::GetRangeRequest req;
    map<Json::Value, static_index::GetRangeResponse> resps;
    const char kRequest0[] = "{"
        "  \"TableName\": \"test\","
        "  \"Limit\": 5000,"
        "  \"Start\": ["
        "    {\"Name\": \"pkey\", \"Value\": \"-inf\"}"
        "  ],"
        "  \"Stop\": ["
        "    {\"Name\": \"pkey\", \"Value\": \"+inf\"}"
        "  ]"
        "}";
    const char kResponse0[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {"
        "          \"Name\": \"pkey\","
        "          \"Value\": 0"
        "        }"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"greeting\", \"Value\": \"hello\"},"
        "        {\"Name\": \"smile\", \"Value\": \"hoho\"}"
        "      ]"
        "    }"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, GetRange(_, _))
        .WillRepeatedly(Invoke(bind(&MockGetRange, _1, _2, &logger, &hits, cref(resps))));
    
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));
    vector<Json::Value> result;
    Json::Value condition = Json::objectValue;
    Json::Value projection = FromJsonStr("[\"greeting\"]");
    Json::Value order = Json::objectValue;
    int r = sind->Find(
        "test",
        projection,
        condition,
        0,
        0,
        order,
        result);
    ASSERT_EQ(r, FICUS_SUCC);
    ASSERT_EQ(result.size(), 1ull);
    EXPECT_EQ(ToString(result[0]), "{\"greeting\":\"hello\"}\n");
    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

TEST(Find, AttrExact)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ],"
        "    \"RequiredAttributes\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ]"
        "  }"
        "}"
        "]";
    FileSyncLogger logger("Find.AttrExact.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    static_index::GetRangeRequest req;
    map<Json::Value, static_index::GetRangeResponse> resps;
    const char kRequest0[] = "{"
        "  \"TableName\": \"test\","
        "  \"Limit\": 5000,"
        "  \"Start\": ["
        "    {\"Name\": \"pkey\", \"Value\": \"-inf\"}"
        "  ],"
        "  \"Stop\": ["
        "    {\"Name\": \"pkey\", \"Value\": \"+inf\"}"
        "  ]"
        "}";
    const char kResponse0[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {"
        "          \"Name\": \"pkey\","
        "          \"Value\": 0"
        "        }"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"greeting\", \"Value\": \"hello\"}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {"
        "          \"Name\": \"pkey\","
        "          \"Value\": 1"
        "        }"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"greeting\", \"Value\": \"hi\"}"
        "      ]"
        "    }"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, GetRange(_, _))
        .WillRepeatedly(Invoke(bind(&MockGetRange, _1, _2, &logger, &hits, cref(resps))));
    
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));
    vector<Json::Value> result;
    Json::Value condition = FromJsonStr("{\"greeting\":\"hello\"}");
    Json::Value projection = Json::arrayValue;
    Json::Value order = Json::objectValue;
    int r = sind->Find(
        "test",
        projection,
        condition,
        0,
        0,
        order,
        result);
    ASSERT_EQ(r, FICUS_SUCC);
    ASSERT_EQ(result.size(), 1ull);
    EXPECT_EQ(ToString(result[0]), "{\"greeting\":\"hello\",\"pkey\":0}\n");
    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

TEST(Find, AttrRange_LeftOpenRightClose)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ],"
        "    \"RequiredAttributes\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ]"
        "  }"
        "}"
        "]";
    FileSyncLogger logger("Find.AttrRange_LeftOpenRightClose.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    static_index::GetRangeRequest req;
    map<Json::Value, static_index::GetRangeResponse> resps;
    const char kRequest0[] = "{"
        "  \"TableName\": \"test\","
        "  \"Limit\": 5000,"
        "  \"Start\": ["
        "    {\"Name\": \"pkey\", \"Value\": \"-inf\"}"
        "  ],"
        "  \"Stop\": ["
        "    {\"Name\": \"pkey\", \"Value\": \"+inf\"}"
        "  ]"
        "}";
    const char kResponse0[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 0}"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"zzZ\", \"Value\": 0}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 1}"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"zzZ\", \"Value\": 1}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 2}"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"zzZ\", \"Value\": 2}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 3}"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"zzZ\", \"Value\": 3}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 4}"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"zzZ\", \"Value\": 4}"
        "      ]"
        "    }"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, GetRange(_, _))
        .WillRepeatedly(Invoke(bind(&MockGetRange, _1, _2, &logger, &hits, cref(resps))));
    
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));
    vector<Json::Value> result;
    Json::Value condition = FromJsonStr("{\"zzZ\":{\"$gt\":1, \"$lte\": 3}}");
    Json::Value projection = Json::arrayValue;
    Json::Value order = Json::objectValue;
    int r = sind->Find(
        "test",
        projection,
        condition,
        0,
        0,
        order,
        result);
    ASSERT_EQ(r, FICUS_SUCC);
    ASSERT_EQ(result.size(), 2ull);
    EXPECT_EQ(ToString(result[0]), "{\"pkey\":2,\"zzZ\":2}\n");
    EXPECT_EQ(ToString(result[1]), "{\"pkey\":3,\"zzZ\":3}\n");
    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

TEST(Find, AttrRange_LeftCloseRightOpen)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ],"
        "    \"RequiredAttributes\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ]"
        "  }"
        "}"
        "]";
    FileSyncLogger logger("Find.AttrRange_LeftCloseRightOpen.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    static_index::GetRangeRequest req;
    map<Json::Value, static_index::GetRangeResponse> resps;
    const char kRequest0[] = "{"
        "  \"TableName\": \"test\","
        "  \"Limit\": 5000,"
        "  \"Start\": ["
        "    {\"Name\": \"pkey\", \"Value\": \"-inf\"}"
        "  ],"
        "  \"Stop\": ["
        "    {\"Name\": \"pkey\", \"Value\": \"+inf\"}"
        "  ]"
        "}";
    const char kResponse0[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 0}"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"zzZ\", \"Value\": 0}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 1}"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"zzZ\", \"Value\": 1}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 2}"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"zzZ\", \"Value\": 2}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 3}"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"zzZ\", \"Value\": 3}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 4}"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"zzZ\", \"Value\": 4}"
        "      ]"
        "    }"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, GetRange(_, _))
        .WillRepeatedly(Invoke(bind(&MockGetRange, _1, _2, &logger, &hits, cref(resps))));
    
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));
    vector<Json::Value> result;
    Json::Value condition = FromJsonStr("{\"zzZ\":{\"$gte\":1, \"$lt\": 3}}");
    Json::Value projection = Json::arrayValue;
    Json::Value order = Json::objectValue;
    int r = sind->Find(
        "test",
        projection,
        condition,
        0,
        0,
        order,
        result);
    ASSERT_EQ(r, FICUS_SUCC);
    ASSERT_EQ(result.size(), 2ull);
    EXPECT_EQ(ToString(result[0]), "{\"pkey\":1,\"zzZ\":1}\n");
    EXPECT_EQ(ToString(result[1]), "{\"pkey\":2,\"zzZ\":2}\n");
    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

TEST(Find, AttrRange_NotEqual)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ],"
        "    \"RequiredAttributes\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ]"
        "  }"
        "}"
        "]";
    FileSyncLogger logger("Find.AttrRange_NotEqual.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    static_index::GetRangeRequest req;
    map<Json::Value, static_index::GetRangeResponse> resps;
    const char kRequest0[] = "{"
        "  \"TableName\": \"test\","
        "  \"Limit\": 5000,"
        "  \"Start\": ["
        "    {\"Name\": \"pkey\", \"Value\": \"-inf\"}"
        "  ],"
        "  \"Stop\": ["
        "    {\"Name\": \"pkey\", \"Value\": \"+inf\"}"
        "  ]"
        "}";
    const char kResponse0[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 0}"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"zzZ\", \"Value\": 0}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 1}"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"zzZ\", \"Value\": 1}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 2}"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"zzZ\", \"Value\": 2}"
        "      ]"
        "    }"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, GetRange(_, _))
        .WillRepeatedly(Invoke(bind(&MockGetRange, _1, _2, &logger, &hits, cref(resps))));
    
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));
    vector<Json::Value> result;
    Json::Value condition = FromJsonStr("{\"zzZ\":{\"$ne\":1}}");
    Json::Value projection = Json::arrayValue;
    Json::Value order = Json::objectValue;
    int r = sind->Find(
        "test",
        projection,
        condition,
        0,
        0,
        order,
        result);
    ASSERT_EQ(r, FICUS_SUCC);
    ASSERT_EQ(result.size(), 2ull);
    EXPECT_EQ(ToString(result[0]), "{\"pkey\":0,\"zzZ\":0}\n");
    EXPECT_EQ(ToString(result[1]), "{\"pkey\":2,\"zzZ\":2}\n");
    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

TEST(Find, AttrRange_Equal)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ],"
        "    \"RequiredAttributes\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ]"
        "  }"
        "}"
        "]";
    FileSyncLogger logger("Find.AttrRange_NotEqual.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    static_index::GetRangeRequest req;
    map<Json::Value, static_index::GetRangeResponse> resps;
    const char kRequest0[] = "{"
        "  \"TableName\": \"test\","
        "  \"Limit\": 5000,"
        "  \"Start\": ["
        "    {\"Name\": \"pkey\", \"Value\": \"-inf\"}"
        "  ],"
        "  \"Stop\": ["
        "    {\"Name\": \"pkey\", \"Value\": \"+inf\"}"
        "  ]"
        "}";
    const char kResponse0[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 0}"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"zzZ\", \"Value\": 0}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 1}"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"zzZ\", \"Value\": 1}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 2}"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"zzZ\", \"Value\": 2}"
        "      ]"
        "    }"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, GetRange(_, _))
        .WillRepeatedly(Invoke(bind(&MockGetRange, _1, _2, &logger, &hits, cref(resps))));
    
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));
    vector<Json::Value> result;
    Json::Value condition = FromJsonStr("{\"zzZ\":{\"$eq\":1}}");
    Json::Value projection = Json::arrayValue;
    Json::Value order = Json::objectValue;
    int r = sind->Find(
        "test",
        projection,
        condition,
        0,
        0,
        order,
        result);
    ASSERT_EQ(r, FICUS_SUCC);
    ASSERT_EQ(result.size(), 1ull);
    EXPECT_EQ(ToString(result[0]), "{\"pkey\":1,\"zzZ\":1}\n");
    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

TEST(Find, AttrRange_In)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ],"
        "    \"RequiredAttributes\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ]"
        "  }"
        "}"
        "]";
    FileSyncLogger logger("Find.AttrRange_In.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    static_index::GetRangeRequest req;
    map<Json::Value, static_index::GetRangeResponse> resps;
    const char kRequest0[] = "{"
        "  \"TableName\": \"test\","
        "  \"Limit\": 5000,"
        "  \"Start\": ["
        "    {\"Name\": \"pkey\", \"Value\": \"-inf\"}"
        "  ],"
        "  \"Stop\": ["
        "    {\"Name\": \"pkey\", \"Value\": \"+inf\"}"
        "  ]"
        "}";
    const char kResponse0[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 0}"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"zzZ\", \"Value\": 0}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 1}"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"zzZ\", \"Value\": 1}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 2}"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"zzZ\", \"Value\": 2}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 3}"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"zzZ\", \"Value\": 3}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 4}"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"zzZ\", \"Value\": 4}"
        "      ]"
        "    }"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, GetRange(_, _))
        .WillRepeatedly(Invoke(bind(&MockGetRange, _1, _2, &logger, &hits, cref(resps))));
    
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));
    vector<Json::Value> result;
    Json::Value condition = FromJsonStr("{\"zzZ\":{\"$in\":[1,3]}}");
    Json::Value projection = Json::arrayValue;
    Json::Value order = Json::objectValue;
    int r = sind->Find(
        "test",
        projection,
        condition,
        0,
        0,
        order,
        result);
    ASSERT_EQ(r, FICUS_SUCC);
    ASSERT_EQ(result.size(), 2ull);
    EXPECT_EQ(ToString(result[0]), "{\"pkey\":1,\"zzZ\":1}\n");
    EXPECT_EQ(ToString(result[1]), "{\"pkey\":3,\"zzZ\":3}\n");
    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

TEST(Find, Ordering_Inc)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ],"
        "    \"RequiredAttributes\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ]"
        "  }"
        "}"
        "]";
    FileSyncLogger logger("Find.Ordering_Inc.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    static_index::GetRangeRequest req;
    map<Json::Value, static_index::GetRangeResponse> resps;
    const char kRequest0[] = "{"
        "  \"TableName\": \"test\","
        "  \"Limit\": 5000,"
        "  \"Start\": ["
        "    {\"Name\": \"pkey\", \"Value\": \"-inf\"}"
        "  ],"
        "  \"Stop\": ["
        "    {\"Name\": \"pkey\", \"Value\": \"+inf\"}"
        "  ]"
        "}";
    const char kResponse0[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 0}"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"zzZ\", \"Value\": 3}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 1}"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"zzZ\", \"Value\": 2}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 2}"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"zzZ\", \"Value\": 1}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 3}"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"zzZ\", \"Value\": 0}"
        "      ]"
        "    }"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, GetRange(_, _))
        .WillRepeatedly(Invoke(bind(&MockGetRange, _1, _2, &logger, &hits, cref(resps))));
    
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));
    vector<Json::Value> result;
    Json::Value condition = Json::objectValue;
    Json::Value projection = Json::arrayValue;
    Json::Value order = FromJsonStr("{\"zzZ\": 1}");
    int r = sind->Find(
        "test",
        projection,
        condition,
        0,
        0,
        order,
        result);
    ASSERT_EQ(r, FICUS_SUCC);
    ASSERT_EQ(result.size(), 4ull);
    EXPECT_EQ(ToString(result[0]), "{\"pkey\":3,\"zzZ\":0}\n");
    EXPECT_EQ(ToString(result[1]), "{\"pkey\":2,\"zzZ\":1}\n");
    EXPECT_EQ(ToString(result[2]), "{\"pkey\":1,\"zzZ\":2}\n");
    EXPECT_EQ(ToString(result[3]), "{\"pkey\":0,\"zzZ\":3}\n");
    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

TEST(Find, Ordering_Dec)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ],"
        "    \"RequiredAttributes\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ]"
        "  }"
        "}"
        "]";
    FileSyncLogger logger("Find.Ordering_Inc.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    static_index::GetRangeRequest req;
    map<Json::Value, static_index::GetRangeResponse> resps;
    const char kRequest0[] = "{"
        "  \"TableName\": \"test\","
        "  \"Limit\": 5000,"
        "  \"Start\": ["
        "    {\"Name\": \"pkey\", \"Value\": \"-inf\"}"
        "  ],"
        "  \"Stop\": ["
        "    {\"Name\": \"pkey\", \"Value\": \"+inf\"}"
        "  ]"
        "}";
    const char kResponse0[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 0}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 1}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 2}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 3}"
        "      ]"
        "    }"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, GetRange(_, _))
        .WillRepeatedly(Invoke(bind(&MockGetRange, _1, _2, &logger, &hits, cref(resps))));
    
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));
    vector<Json::Value> result;
    Json::Value condition = Json::objectValue;
    Json::Value projection = Json::arrayValue;
    Json::Value order = FromJsonStr("{\"pkey\": -1}");
    int r = sind->Find(
        "test",
        projection,
        condition,
        0,
        0,
        order,
        result);
    ASSERT_EQ(r, FICUS_SUCC);
    ASSERT_EQ(result.size(), 4ull);
    EXPECT_EQ(ToString(result[0]), "{\"pkey\":3}\n");
    EXPECT_EQ(ToString(result[1]), "{\"pkey\":2}\n");
    EXPECT_EQ(ToString(result[2]), "{\"pkey\":1}\n");
    EXPECT_EQ(ToString(result[3]), "{\"pkey\":0}\n");
    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

TEST(Find, StartLimit)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ],"
        "    \"RequiredAttributes\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ]"
        "  }"
        "}"
        "]";
    FileSyncLogger logger("Find.StartLimit.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    static_index::GetRangeRequest req;
    map<Json::Value, static_index::GetRangeResponse> resps;
    const char kRequest0[] = "{"
        "  \"TableName\": \"test\","
        "  \"Limit\": 5000,"
        "  \"Start\": ["
        "    {\"Name\": \"pkey\", \"Value\": \"-inf\"}"
        "  ],"
        "  \"Stop\": ["
        "    {\"Name\": \"pkey\", \"Value\": \"+inf\"}"
        "  ]"
        "}";
    const char kResponse0[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 0}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 1}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 2}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 3}"
        "      ]"
        "    }"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, GetRange(_, _))
        .WillRepeatedly(Invoke(bind(&MockGetRange, _1, _2, &logger, &hits, cref(resps))));
    
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));
    vector<Json::Value> result;
    Json::Value condition = Json::objectValue;
    Json::Value projection = Json::arrayValue;
    Json::Value order = Json::objectValue;
    int r = sind->Find(
        "test",
        projection,
        condition,
        1,
        2,
        order,
        result);
    ASSERT_EQ(r, FICUS_SUCC);
    ASSERT_EQ(result.size(), 2ull);
    EXPECT_EQ(ToString(result[0]), "{\"pkey\":1}\n");
    EXPECT_EQ(ToString(result[1]), "{\"pkey\":2}\n");
    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

namespace {

Exceptional MockGetRange1(
    static_index::GetRangeResponse* resp,
    const static_index::GetRangeRequest& req,
    Logger* logger,
    deque<static_index::GetRangeRequest>* reqs,
    const map<Json::Value, static_index::GetRangeResponse>& resps)
{
    reqs->push_back(req);
    Json::Value jReq = Jsonize(logger, req);
    jReq.removeMember("Tracker");
    map<Json::Value, static_index::GetRangeResponse>::const_iterator it =
        resps.find(jReq);
    OTS_ASSERT(logger, it != resps.end())(jReq.toStyledString());
    *resp = it->second;
    return Exceptional();
}

} // namespace

TEST(Find, QuickQuit)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ],"
        "    \"RequiredAttributes\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ]"
        "  }"
        "}"
        "]";
    FileSyncLogger logger("Find.QuickQuit.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    static_index::GetRangeRequest req;
    map<Json::Value, static_index::GetRangeResponse> resps;
    const char kRequest0[] = "{"
        "  \"TableName\": \"test\","
        "  \"Limit\": 5000,"
        "  \"Start\": ["
        "    {\"Name\": \"pkey\", \"Value\": \"-inf\"}"
        "  ],"
        "  \"Stop\": ["
        "    {\"Name\": \"pkey\", \"Value\": \"+inf\"}"
        "  ]"
        "}";
    const char kResponse0[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {"
        "          \"Name\": \"pkey\","
        "          \"Value\": 0"
        "        }"
        "      ]"
        "    }"
        "  ],"
        "  \"Next\": ["
        "    {\"Name\": \"pkey\", \"Value\": 1}"
        "  ]"
        "}";
    const char kRequest1[] = "{"
        "  \"TableName\": \"test\","
        "  \"Limit\": 5000,"
        "  \"Start\": ["
        "    {\"Name\": \"pkey\", \"Value\": 1}"
        "  ],"
        "  \"Stop\": ["
        "    {\"Name\": \"pkey\", \"Value\": \"+inf\"}"
        "  ]"
        "}";
    const char kResponse1[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {"
        "          \"Name\": \"pkey\","
        "          \"Value\": 1"
        "        }"
        "      ]"
        "    }"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest1)]), FromJsonStr(kResponse1));
    deque<static_index::GetRangeRequest> reqs;
    EXPECT_CALL(client, GetRange(_, _))
        .WillRepeatedly(Invoke(bind(&MockGetRange1, _1, _2, &logger, &reqs, cref(resps))));

    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));
    vector<Json::Value> result;
    Json::Value condition = Json::objectValue;
    Json::Value projection = Json::arrayValue;
    Json::Value order = Json::objectValue;
    sind->Find(
        "test",
        projection,
        condition,
        0,
        1,
        order,
        result);
    ASSERT_EQ(reqs.size(), 1ull);
    EXPECT_EQ(ToString(Jsonize(&logger, reqs[0])), ToString(FromJsonStr(kRequest0)));
}

TEST(Find, OnPrimaryTable)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ],"
        "    \"RequiredAttributes\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ]"
        "  }"
        "}"
        "]";
    FileSyncLogger logger("Find.OnPrimaryTable.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    static_index::GetRangeRequest req;
    map<Json::Value, static_index::GetRangeResponse> resps;
    const char kRequest0[] = "{"
        "  \"TableName\": \"test\","
        "  \"Limit\": 5000,"
        "  \"Start\": ["
        "    {\"Name\": \"pkey\", \"Value\": 1}"
        "  ],"
        "  \"Stop\": ["
        "    {\"Name\": \"pkey\", \"Value\": 3}"
        "  ]"
        "}";
    const char kResponse0[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 1}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 2}"
        "      ]"
        "    }"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, GetRange(_, _))
        .WillRepeatedly(Invoke(bind(&MockGetRange, _1, _2, &logger, &hits, cref(resps))));
    
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));
    vector<Json::Value> result;
    Json::Value condition = Json::objectValue;
    condition["pkey"]["$gte"] = 1;
    condition["pkey"]["$lte"] = 2;
    Json::Value projection = Json::arrayValue;
    Json::Value order = Json::objectValue;
    int r = sind->Find(
        "test",
        projection,
        condition,
        0,
        0,
        order,
        result);
    ASSERT_EQ(r, FICUS_SUCC);
    ASSERT_EQ(result.size(), 2ull);
    EXPECT_EQ(ToString(result[0]), "{\"pkey\":1}\n");
    EXPECT_EQ(ToString(result[1]), "{\"pkey\":2}\n");
    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

TEST(Find, OnPrimaryTable_Hex)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"composited\","
        "        \"Type\": \"Composited\","
        "        \"Definition\": \"($Hex pkey)\""
        "      },"
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ],"
        "    \"RequiredAttributes\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ]"
        "  }"
        "}"
        "]";
    FileSyncLogger logger("Find.OnPrimaryTable_Hex.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    static_index::GetRangeRequest req;
    map<Json::Value, static_index::GetRangeResponse> resps;
    const char kRequest0[] = "{"
        "  \"TableName\": \"test\","
        "  \"Limit\": 5000,"
        "  \"Start\": ["
        "    {\"Name\": \"composited\", \"Value\": \"0000000000000001\"},"
        "    {\"Name\": \"pkey\", \"Value\": 1}"
        "  ],"
        "  \"Stop\": ["
        "    {\"Name\": \"composited\", \"Value\": \"0000000000000002\"},"
        "    {\"Name\": \"pkey\", \"Value\": 3}"
        "  ]"
        "}";
    const char kResponse0[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"composited\",\"Value\": \"0000000000000001\"},"
        "        {\"Name\": \"pkey\",\"Value\": 1}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"composited\",\"Value\": \"0000000000000002\"},"
        "        {\"Name\": \"pkey\",\"Value\": 2}"
        "      ]"
        "    }"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, GetRange(_, _))
        .WillRepeatedly(Invoke(bind(&MockGetRange, _1, _2, &logger, &hits, cref(resps))));
    
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));
    vector<Json::Value> result;
    Json::Value condition = Json::objectValue;
    condition["pkey"]["$gte"] = 1;
    condition["pkey"]["$lte"] = 2;
    Json::Value projection = Json::arrayValue;
    Json::Value order = Json::objectValue;
    int r = sind->Find(
        "test",
        projection,
        condition,
        0,
        0,
        order,
        result);
    ASSERT_EQ(r, FICUS_SUCC);
    ASSERT_EQ(result.size(), 2ull);
    EXPECT_EQ(ToString(result[0]), "{\"pkey\":1}\n");
    EXPECT_EQ(ToString(result[1]), "{\"pkey\":2}\n");
    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

TEST(Find, OnPrimaryTable_Concat)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"composited\","
        "        \"Type\": \"Composited\","
        "        \"Definition\": \"(| pkey0 ($Hex pkey1))\""
        "      },"
        "      {"
        "        \"Name\": \"pkey0\","
        "        \"Type\": \"Str\""
        "      },"
        "      {"
        "        \"Name\": \"pkey1\","
        "        \"Type\": \"Int\""
        "      }"
        "    ],"
        "    \"RequiredAttributes\": ["
        "      {"
        "        \"Name\": \"pkey0\","
        "        \"Type\": \"Str\""
        "      },"
        "      {"
        "        \"Name\": \"pkey1\","
        "        \"Type\": \"Int\""
        "      }"
        "    ]"
        "  }"
        "}"
        "]";
    FileSyncLogger logger("Find.OnPrimaryTable_Concat.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    static_index::GetRangeRequest req;
    map<Json::Value, static_index::GetRangeResponse> resps;
    const char kRequest0[] = "{"
        "  \"TableName\": \"test\","
        "  \"Limit\": 5000,"
        "  \"Start\": ["
        "    {\"Name\": \"composited\", \"Value\": \"a|0000000000000001\"},"
        "    {\"Name\": \"pkey0\", \"Value\": \"a\"},"
        "    {\"Name\": \"pkey1\", \"Value\": 1}"
        "  ],"
        "  \"Stop\": ["
        "    {\"Name\": \"composited\", \"Value\": \"c|0000000000000002\"},"
        "    {\"Name\": \"pkey0\", \"Value\": \"c\"},"
        "    {\"Name\": \"pkey1\", \"Value\": 3}"
        "  ]"
        "}";
    const char kResponse0[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"composited\",\"Value\": \"a|0000000000000001\"},"
        "        {\"Name\": \"pkey0\",\"Value\": \"a\"},"
        "        {\"Name\": \"pkey1\",\"Value\": 1}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"composited\",\"Value\": \"0000000000000002\"},"
        "        {\"Name\": \"pkey0\",\"Value\": \"b\"},"
        "        {\"Name\": \"pkey1\",\"Value\": 2}"
        "      ]"
        "    }"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, GetRange(_, _))
        .WillRepeatedly(Invoke(bind(&MockGetRange, _1, _2, &logger, &hits, cref(resps))));
    
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));
    vector<Json::Value> result;
    Json::Value condition = Json::objectValue;
    condition["pkey0"]["$gte"] = "a";
    condition["pkey0"]["$lte"] = "c";
    condition["pkey1"]["$gte"] = 1;
    condition["pkey1"]["$lte"] = 2;
    Json::Value projection = Json::arrayValue;
    Json::Value order = Json::objectValue;
    int r = sind->Find(
        "test",
        projection,
        condition,
        0,
        0,
        order,
        result);
    ASSERT_EQ(r, FICUS_SUCC);
    ASSERT_EQ(result.size(), 2ull);
    EXPECT_EQ(ToString(result[0]), "{\"pkey0\":\"a\",\"pkey1\":1}\n");
    EXPECT_EQ(ToString(result[1]), "{\"pkey0\":\"b\",\"pkey1\":2}\n");
    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

TEST(Find, OnPrimaryTable_ShiftToUint64)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"composited\","
        "        \"Type\": \"Composited\","
        "        \"Definition\": \"($Hex ($ShiftToUint64 pkey))\""
        "      },"
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ],"
        "    \"RequiredAttributes\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ]"
        "  }"
        "}"
        "]";
    FileSyncLogger logger("Find.OnPrimaryTable_Hex.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    static_index::GetRangeRequest req;
    map<Json::Value, static_index::GetRangeResponse> resps;
    const char kRequest0[] = "{"
        "  \"TableName\": \"test\","
        "  \"Limit\": 5000,"
        "  \"Start\": ["
        "    {\"Name\": \"composited\", \"Value\": \"8000000000000001\"},"
        "    {\"Name\": \"pkey\", \"Value\": 1}"
        "  ],"
        "  \"Stop\": ["
        "    {\"Name\": \"composited\", \"Value\": \"8000000000000002\"},"
        "    {\"Name\": \"pkey\", \"Value\": 3}"
        "  ]"
        "}";
    const char kResponse0[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"composited\",\"Value\": \"8000000000000001\"},"
        "        {\"Name\": \"pkey\",\"Value\": 1}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"composited\",\"Value\": \"8000000000000002\"},"
        "        {\"Name\": \"pkey\",\"Value\": 2}"
        "      ]"
        "    }"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, GetRange(_, _))
        .WillRepeatedly(Invoke(bind(&MockGetRange, _1, _2, &logger, &hits, cref(resps))));
    
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));
    vector<Json::Value> result;
    Json::Value condition = Json::objectValue;
    condition["pkey"]["$gte"] = 1;
    condition["pkey"]["$lte"] = 2;
    Json::Value projection = Json::arrayValue;
    Json::Value order = Json::objectValue;
    int r = sind->Find(
        "test",
        projection,
        condition,
        0,
        0,
        order,
        result);
    ASSERT_EQ(r, FICUS_SUCC);
    ASSERT_EQ(result.size(), 2ull);
    EXPECT_EQ(ToString(result[0]), "{\"pkey\":1}\n");
    EXPECT_EQ(ToString(result[1]), "{\"pkey\":2}\n");
    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

TEST(Find, OnPrimaryTable_PointQuery)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ],"
        "    \"RequiredAttributes\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ]"
        "  }"
        "}"
        "]";
    FileSyncLogger logger("Find.OnPrimaryTable_PointQuery.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    static_index::GetRangeRequest req;
    map<Json::Value, static_index::BatchGetRowResponse> resps;
    const char kRequest0[] = "{"
        "  \"GetRows\": ["
        "    {"
        "      \"TableName\": \"test\","
        "      \"PrimaryKey\": [{\"Name\": \"pkey\", \"Value\": 1}]"
        "    }"
        "  ]"
        "}";
    const char kResponse0[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 1}"
        "      ]"
        "    }"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, BatchGetRow(_, _))
        .WillRepeatedly(Invoke(bind(&MockBatchGetRow, _1, _2, &logger, &hits, cref(resps))));
    
    FlagSetter<int64_t> bwl(BulkExecutor::sBatchGetRowLimit, 1);
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));

    vector<Json::Value> result;
    Json::Value condition = Json::objectValue;
    condition["pkey"] = 1;
    Json::Value projection = Json::arrayValue;
    Json::Value order = Json::objectValue;
    int r = sind->Find(
        "test",
        projection,
        condition,
        0,
        0,
        order,
        result);
    ASSERT_EQ(r, FICUS_SUCC);
    ASSERT_EQ(result.size(), 1ull);
    EXPECT_EQ(ToString(result[0]), "{\"pkey\":1}\n");
    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

TEST(Find, OnPrimaryTable_In)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ],"
        "    \"RequiredAttributes\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ]"
        "  }"
        "}"
        "]";
    FileSyncLogger logger("Find.OnPrimaryTable_In.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    static_index::GetRangeRequest req;
    map<Json::Value, static_index::BatchGetRowResponse> resps;
    const char kRequest0[] = "{"
        "  \"GetRows\": ["
        "    {"
        "      \"TableName\": \"test\","
        "      \"PrimaryKey\": [{\"Name\": \"pkey\", \"Value\": 1}]"
        "    }"
        "  ]"
        "}";
    const char kResponse0[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 1}"
        "      ]"
        "    }"
        "  ]"
        "}";
    const char kRequest1[] = "{"
        "  \"GetRows\": ["
        "    {"
        "      \"TableName\": \"test\","
        "      \"PrimaryKey\": [{\"Name\": \"pkey\", \"Value\": 2}]"
        "    }"
        "  ]"
        "}";
    const char kResponse1[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 2}"
        "      ]"
        "    }"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest1)]), FromJsonStr(kResponse1));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, BatchGetRow(_, _))
        .WillRepeatedly(Invoke(bind(&MockBatchGetRow, _1, _2, &logger, &hits, cref(resps))));
    
    FlagSetter<int64_t> bwl(BulkExecutor::sBatchGetRowLimit, 1);
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));

    vector<Json::Value> result;
    Json::Value condition = Json::objectValue;
    condition["pkey"]["$in"] = Json::arrayValue;
    condition["pkey"]["$in"].append(1);
    condition["pkey"]["$in"].append(2);
    Json::Value projection = Json::arrayValue;
    Json::Value order = Json::objectValue;
    order["pkey"] = 1;
    int r = sind->Find(
        "test",
        projection,
        condition,
        0,
        0,
        order,
        result);
    ASSERT_EQ(r, FICUS_SUCC);
    ASSERT_EQ(result.size(), 2ull);
    EXPECT_EQ(ToString(result[0]), "{\"pkey\":1}\n");
    EXPECT_EQ(ToString(result[1]), "{\"pkey\":2}\n");
    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

TEST(Find, OnPrimaryTable_InForTwo)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"hex_pkey\","
        "        \"Type\": \"Composited\","
        "        \"Definition\": \"($Hex pkey)\""
        "      },"
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ],"
        "    \"RequiredAttributes\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ]"
        "  }"
        "}"
        "]";
    FileSyncLogger logger("Find.OnPrimaryTable_InForTwo.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    static_index::GetRangeRequest req;
    map<Json::Value, static_index::BatchGetRowResponse> resps;
    const char kRequest0[] = "{"
        "  \"GetRows\": ["
        "    {"
        "      \"TableName\": \"test\","
        "      \"PrimaryKey\": ["
        "        {\"Name\": \"hex_pkey\", \"Value\": \"0000000000000001\"},"
        "        {\"Name\": \"pkey\", \"Value\": 1}]"
        "    }"
        "  ]"
        "}";
    const char kResponse0[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"hex_pkey\", \"Value\": \"0000000000000001\"},"
        "        {\"Name\": \"pkey\",\"Value\": 1}"
        "      ]"
        "    }"
        "  ]"
        "}";
    const char kRequest1[] = "{"
        "  \"GetRows\": ["
        "    {"
        "      \"TableName\": \"test\","
        "      \"PrimaryKey\": ["
        "        {\"Name\": \"hex_pkey\", \"Value\": \"0000000000000002\"},"
        "        {\"Name\": \"pkey\", \"Value\": 2}]"
        "    }"
        "  ]"
        "}";
    const char kResponse1[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"hex_pkey\", \"Value\": \"0000000000000002\"},"
        "        {\"Name\": \"pkey\",\"Value\": 2}"
        "      ]"
        "    }"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest1)]), FromJsonStr(kResponse1));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, BatchGetRow(_, _))
        .WillRepeatedly(Invoke(bind(&MockBatchGetRow, _1, _2, &logger, &hits, cref(resps))));
    
    FlagSetter<int64_t> bwl(BulkExecutor::sBatchGetRowLimit, 1);
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));

    vector<Json::Value> result;
    Json::Value condition = Json::objectValue;
    condition["pkey"]["$in"] = Json::arrayValue;
    condition["pkey"]["$in"].append(1);
    condition["pkey"]["$in"].append(2);
    Json::Value projection = Json::arrayValue;
    projection.append("pkey");
    Json::Value order = Json::objectValue;
    order["pkey"] = 1;
    int r = sind->Find(
        "test",
        projection,
        condition,
        0,
        0,
        order,
        result);
    ASSERT_EQ(r, FICUS_SUCC);
    ASSERT_EQ(result.size(), 2ull);
    EXPECT_EQ(ToString(result[0]), "{\"pkey\":1}\n");
    EXPECT_EQ(ToString(result[1]), "{\"pkey\":2}\n");
    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

TEST(Find, OnPrimaryTable_Mixed)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"hex_pkey0\","
        "        \"Type\": \"Composited\","
        "        \"Definition\": \"($Hex pkey0)\""
        "      },"
        "      {"
        "        \"Name\": \"pkey0\","
        "        \"Type\": \"Int\""
        "      },"
        "      {"
        "        \"Name\": \"pkey1\","
        "        \"Type\": \"Int\""
        "      }"
        "    ],"
        "    \"RequiredAttributes\": ["
        "      {"
        "        \"Name\": \"pkey0\","
        "        \"Type\": \"Int\""
        "      },"
        "      {"
        "        \"Name\": \"pkey1\","
        "        \"Type\": \"Int\""
        "      }"
        "    ]"
        "  }"
        "}"
        "]";
    FileSyncLogger logger("Find.OnPrimaryTable_Mixed.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    static_index::GetRangeRequest req;
    map<Json::Value, static_index::GetRangeResponse> resps;
    const char kRequest0[] = "{"
        "  \"TableName\": \"test\","
        "  \"Limit\": 5000,"
        "  \"Start\": ["
        "    {\"Name\": \"hex_pkey0\", \"Value\": \"0000000000000000\"},"
        "    {\"Name\": \"pkey0\", \"Value\": 0},"
        "    {\"Name\": \"pkey1\", \"Value\": -1}"
        "  ],"
        "  \"Stop\": ["
        "    {\"Name\": \"hex_pkey0\", \"Value\": \"0000000000000000\"},"
        "    {\"Name\": \"pkey0\", \"Value\": 0},"
        "    {\"Name\": \"pkey1\", \"Value\": 2}"
        "  ]"
        "}";
    const char kResponse0[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"hex_pkey0\", \"Value\": \"0000000000000000\"},"
        "        {\"Name\": \"pkey0\",\"Value\": 0},"
        "        {\"Name\": \"pkey1\", \"Value\": -1}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"hex_pkey0\", \"Value\": \"0000000000000000\"},"
        "        {\"Name\": \"pkey0\",\"Value\": 0},"
        "        {\"Name\": \"pkey1\", \"Value\": 0}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"hex_pkey0\", \"Value\": \"0000000000000000\"},"
        "        {\"Name\": \"pkey0\",\"Value\": 0},"
        "        {\"Name\": \"pkey1\", \"Value\": 1}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"hex_pkey0\", \"Value\": \"0000000000000000\"},"
        "        {\"Name\": \"pkey0\",\"Value\": 0},"
        "        {\"Name\": \"pkey1\", \"Value\": 2}"
        "      ]"
        "    }"
        "  ]"
        "}";
    const char kRequest1[] = "{"
        "  \"TableName\": \"test\","
        "  \"Limit\": 5000,"
        "  \"Start\": ["
        "    {\"Name\": \"hex_pkey0\", \"Value\": \"0000000000000001\"},"
        "    {\"Name\": \"pkey0\", \"Value\": 1},"
        "    {\"Name\": \"pkey1\", \"Value\": -1}"
        "  ],"
        "  \"Stop\": ["
        "    {\"Name\": \"hex_pkey0\", \"Value\": \"0000000000000001\"},"
        "    {\"Name\": \"pkey0\", \"Value\": 1},"
        "    {\"Name\": \"pkey1\", \"Value\": 2}"
        "  ]"
        "}";
    const char kResponse1[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"hex_pkey0\", \"Value\": \"0000000000000001\"},"
        "        {\"Name\": \"pkey0\",\"Value\": 1},"
        "        {\"Name\": \"pkey1\", \"Value\": -1}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"hex_pkey0\", \"Value\": \"0000000000000001\"},"
        "        {\"Name\": \"pkey0\",\"Value\": 1},"
        "        {\"Name\": \"pkey1\", \"Value\": 0}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"hex_pkey0\", \"Value\": \"0000000000000001\"},"
        "        {\"Name\": \"pkey0\",\"Value\": 1},"
        "        {\"Name\": \"pkey1\", \"Value\": 1}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"hex_pkey0\", \"Value\": \"0000000000000001\"},"
        "        {\"Name\": \"pkey0\",\"Value\": 1},"
        "        {\"Name\": \"pkey1\", \"Value\": 2}"
        "      ]"
        "    }"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest1)]), FromJsonStr(kResponse1));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, GetRange(_, _))
        .WillRepeatedly(Invoke(bind(&MockGetRange, _1, _2, &logger, &hits, cref(resps))));
    
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));
    vector<Json::Value> result;
    Json::Value condition = Json::objectValue;
    condition["pkey0"]["$in"] = Json::arrayValue;
    condition["pkey0"]["$in"].append(0);
    condition["pkey0"]["$in"].append(1);
    condition["pkey1"]["$gte"] = -1;
    condition["pkey1"]["$lte"] = 1;
    Json::Value projection = Json::arrayValue;
    Json::Value order = Json::objectValue;
    int r = sind->Find(
        "test",
        projection,
        condition,
        0,
        0,
        order,
        result);
    ASSERT_EQ(r, FICUS_SUCC);
    set<string> expect;
    expect.insert("{\"pkey0\":0,\"pkey1\":-1}\n");
    expect.insert("{\"pkey0\":0,\"pkey1\":0}\n");
    expect.insert("{\"pkey0\":0,\"pkey1\":1}\n");
    expect.insert("{\"pkey0\":1,\"pkey1\":-1}\n");
    expect.insert("{\"pkey0\":1,\"pkey1\":0}\n");
    expect.insert("{\"pkey0\":1,\"pkey1\":1}\n");
    AssertEq(&logger, result, expect);
    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

TEST(Find, OnIndex)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ],"
        "    \"RequiredAttributes\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Int\""
        "      }"
        "    ]"
        "  },"
        "  \"Indexes\": ["
        "    {"
        "      \"TableName\": \"index\","
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"attr\", \"Type\": \"Int\"},"
        "        {\"Name\": \"pkey\", \"Type\": \"Int\"}"
        "      ],"
        "      \"RequiredAttributes\": ["
        "        {\"Name\": \"attr\", \"Type\": \"Int\"},"
        "        {\"Name\": \"pkey\", \"Type\": \"Int\"}"
        "      ]"
        "    }"
        "  ]"
        "}"
        "]";
    FileSyncLogger logger("Find.OnIndex.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    static_index::GetRangeRequest req;
    const char kRequest0[] = "{"
        "  \"TableName\": \"index\","
        "  \"Limit\": 5000,"
        "  \"Start\": ["
        "    {\"Name\": \"attr\", \"Value\": -1},"
        "    {\"Name\": \"pkey\", \"Value\": \"-inf\"}"
        "  ],"
        "  \"Stop\": ["
        "    {\"Name\": \"attr\", \"Value\": 1},"
        "    {\"Name\": \"pkey\", \"Value\": \"+inf\"}"
        "  ]"
        "}";
    const char kResponse0[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"attr\",\"Value\": -1},"
        "        {\"Name\": \"pkey\", \"Value\": -1}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"attr\",\"Value\": 0},"
        "        {\"Name\": \"pkey\", \"Value\": 0}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"attr\",\"Value\": 1},"
        "        {\"Name\": \"pkey\", \"Value\": 1}"
        "      ]"
        "    }"
        "  ]"
        "}";
    const char kRequest1[] = "{"
        "  \"GetRows\": ["
        "    {"
        "      \"TableName\": \"test\","
        "      \"PrimaryKey\": [{\"Name\": \"pkey\", \"Value\": -1}]"
        "    }"
        "  ]"
        "}";
    const char kResponse1[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": -1}"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"attr\", \"Value\": -1}"
        "      ]"
        "    }"
        "  ]"
        "}";
    const char kRequest2[] = "{"
        "  \"GetRows\": ["
        "    {"
        "      \"TableName\": \"test\","
        "      \"PrimaryKey\": [{\"Name\": \"pkey\", \"Value\": 1}]"
        "    }"
        "  ]"
        "}";
    const char kResponse2[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": 1}"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"attr\", \"Value\": 1}"
        "      ]"
        "    }"
        "  ]"
        "}";
    const char kRequest3[] = "{"
        "  \"GetRows\": ["
        "    {"
        "      \"TableName\": \"test\","
        "      \"PrimaryKey\": [{\"Name\": \"pkey\", \"Value\": 0}]"
        "    }"
        "  ]"
        "}";
    const char kResponse3[] = "{"
        "  \"Rows\": ["
        "    {"
        "      \"PrimaryKeys\": [],"
        "      \"Attributes\": []"
        "    }"
        "  ]"
        "}";
    map<Json::Value, static_index::GetRangeResponse> respsGetRange;
    map<Json::Value, static_index::BatchGetRowResponse> respsGetRow;
    Unjsonize(&logger, &(respsGetRange[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    Unjsonize(&logger, &(respsGetRow[FromJsonStr(kRequest1)]), FromJsonStr(kResponse1));
    Unjsonize(&logger, &(respsGetRow[FromJsonStr(kRequest2)]), FromJsonStr(kResponse2));
    Unjsonize(&logger, &(respsGetRow[FromJsonStr(kRequest3)]), FromJsonStr(kResponse3));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, BatchGetRow(_, _))
        .WillRepeatedly(Invoke(bind(&MockBatchGetRow, _1, _2, &logger, &hits, cref(respsGetRow))));
    EXPECT_CALL(client, GetRange(_, _))
        .WillRepeatedly(Invoke(bind(&MockGetRange, _1, _2, &logger, &hits, cref(respsGetRange))));

    FlagSetter<int64_t> bwl(BulkExecutor::sBatchGetRowLimit, 1);
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));

    vector<Json::Value> result;
    Json::Value condition = Json::objectValue;
    condition["attr"]["$gte"] = -1;
    condition["attr"]["$lte"] = 1;
    Json::Value projection = Json::arrayValue;
    Json::Value order = Json::objectValue;
    order["pkey"] = 1;
    int r = sind->Find(
        "test",
        projection,
        condition,
        0,
        0,
        order,
        result);
    OTS_ASSERT(&logger, r == FICUS_SUCC);
    EXPECT_EQ(ToString(result[0]), "{\"attr\":-1,\"pkey\":-1}\n");
    EXPECT_EQ(ToString(result[1]), "{\"attr\":1,\"pkey\":1}\n");
    FOREACH_ITER(i, respsGetRange) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
    FOREACH_ITER(i, respsGetRow) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

