#include "static_index_common.h"

using namespace ::std;
using namespace ::std::tr1;
using namespace ::std::tr1::placeholders;
using namespace ::testing;
using namespace ::aliyun::openservices::ots;
using namespace ::static_index;

TEST(Insert, PrimaryTable)
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
    FileSyncLogger logger("Insert.PrimaryTable.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    map<Json::Value, static_index::BatchWriteResponse> resps;
    const char kRequest0[] =
        "{"
        "   \"PutRows\" : ["
        "      {"
        "         \"Attrs\" : {"
        "            \"attr0\" : true,"
        "            \"attr1\" : 0.20000000000000001,"
        "            \"attr2\" : \"hello\","
        "            \"attr3\" : \"[1,2]\\n\","
        "            \"attr4\" : \"{\\\"greeting\\\":\\\"xixi\\\"}\\n\""
        "         },"
        "         \"Pkey\" : ["
        "            {"
        "               \"pkey\" : 123"
        "            }"
        "         ],"
        "         \"TableName\" : \"test\""
        "      }"
        "   ]"
        "}";
    const char kResponse0[] = "{"
        "  \"PutRows\": ["
        "    {}"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, BatchWrite(_, _))
        .WillRepeatedly(Invoke(bind(&MockBatchWrite, _1, _2, &logger, &hits, cref(resps))));
    FlagSetter<int64_t> bwl(BulkExecutor::sBatchWriteLimit, 1);
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));

    const char data[] = "{"
        "\"pkey\": 123, "
        "\"attr0\": true, "
        "\"attr1\": 0.2, "
        "\"attr2\": \"hello\", "
        "\"attr3\": [1, 2], "
        "\"attr4\": {\"greeting\": \"xixi\"}}";
    int r = sind->Insert("test", FromJsonStr(data));
    OTS_ASSERT(&logger, r == FICUS_SUCC);
    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

TEST(Insert, PrimaryTable_MissingRequired)
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
    FileSyncLogger logger("Insert.PrimaryTable_MissingRequired.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    const char data[] = "{\"attr0\": true}";
    
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));
    int r = sind->Insert("test", FromJsonStr(data));
    ASSERT_NE(r, FICUS_SUCC);
}

TEST(Insert, WrongTable)
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
    FileSyncLogger logger("Insert.WrongTable.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    const char data[] = "{\"pkey\": 123, \"attr0\": true}";
    
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));
    int r = sind->Insert("xxx", FromJsonStr(data));
    ASSERT_NE(r, FICUS_SUCC);
}

TEST(Insert, WrongTypeInPrimaryTable)
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
    FileSyncLogger logger("Insert.WrongTypeInPrimaryTable.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    const char data[] = "{\"pkey\": \"hello\"}";
    
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));
    int r = sind->Insert("test", FromJsonStr(data));
    ASSERT_NE(r, FICUS_SUCC);
}

TEST(Insert, Composited_Crc64Int)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"hash_pkey\","
        "        \"Type\": \"Composited\","
        "        \"Definition\": \"($Crc64Int pkey)\""
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
    FileSyncLogger logger("Insert.Composited_Crc64Int.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    map<Json::Value, static_index::BatchWriteResponse> resps;
    const char kRequest0[] =
        "{\n"
        "   \"PutRows\" : [\n"
        "      {\n"
        "         \"Attrs\" : {\n"
        "            \"pkey\" : 123\n"
        "         },\n"
        "         \"Pkey\" : [\n"
        "            {\n"
        "               \"hash_pkey\" : 109344488003309218\n"
        "            }\n"
        "         ],\n"
        "         \"TableName\" : \"test\"\n"
        "      }\n"
        "   ]\n"
        "}\n";
    const char kResponse0[] = "{"
        "  \"PutRows\": ["
        "    {}"
        "  ]"
        "}";
    Json::Value jv = FromJsonStr(kRequest0);
    jv["PutRows"][0]["Pkey"][0]["hash_pkey"] = Json::Int64(109344488003309218);
    Unjsonize(&logger, &(resps[jv]), FromJsonStr(kResponse0));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, BatchWrite(_, _))
        .WillRepeatedly(Invoke(bind(&MockBatchWrite, _1, _2, &logger, &hits, cref(resps))));
    FlagSetter<int64_t> bwl(BulkExecutor::sBatchWriteLimit, 1);
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));

    const char data[] = "{\"pkey\": 123}";
    int r = sind->Insert("test", FromJsonStr(data));
    OTS_ASSERT(&logger, r == FICUS_SUCC);

    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

TEST(Insert, Composited_Hex)
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
    FileSyncLogger logger("Insert.Composited_Hex.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    map<Json::Value, static_index::BatchWriteResponse> resps;
    const char kRequest0[] =
        "{\n"
        "   \"PutRows\" : [\n"
        "      {\n"
        "         \"Attrs\" : {\n"
        "            \"pkey\" : 123\n"
        "         },\n"
        "         \"Pkey\" : [\n"
        "            {\n"
        "               \"hash_pkey\" : \"000000000000007B\"\n"
        "            }\n"
        "         ],\n"
        "         \"TableName\" : \"test\"\n"
        "      }\n"
        "   ]\n"
        "}\n";
    const char kResponse0[] = "{"
        "  \"PutRows\": ["
        "    {}"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, BatchWrite(_, _))
        .WillRepeatedly(Invoke(bind(&MockBatchWrite, _1, _2, &logger, &hits, cref(resps))));
    FlagSetter<int64_t> bwl(BulkExecutor::sBatchWriteLimit, 1);
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));

    const char data[] = "{\"pkey\": 123}";
    int r = sind->Insert("test", FromJsonStr(data));
    OTS_ASSERT(&logger, r == FICUS_SUCC);
    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

TEST(Insert, Composited)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"hash_pkey\","
        "        \"Type\": \"Composited\","
        "        \"Definition\": \"($Hex ($Crc64Int pkey))\""
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
    FileSyncLogger logger("Insert.Composited.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    map<Json::Value, static_index::BatchWriteResponse> resps;
    const char kRequest0[] =
        "{\n"
        "   \"PutRows\" : [\n"
        "      {\n"
        "         \"Attrs\" : {\n"
        "            \"pkey\" : 123\n"
        "         },\n"
        "         \"Pkey\" : [\n"
        "            {\n"
        "               \"hash_pkey\" : \"0184783B85958EA2\"\n"
        "            }\n"
        "         ],\n"
        "         \"TableName\" : \"test\"\n"
        "      }\n"
        "   ]\n"
        "}\n";
    const char kResponse0[] = "{"
        "  \"PutRows\": ["
        "    {}"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, BatchWrite(_, _))
        .WillRepeatedly(Invoke(bind(&MockBatchWrite, _1, _2, &logger, &hits, cref(resps))));
    FlagSetter<int64_t> bwl(BulkExecutor::sBatchWriteLimit, 1);
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));

    const char data[] = "{\"pkey\": 123}";
    int r = sind->Insert("test", FromJsonStr(data));
    OTS_ASSERT(&logger, r == FICUS_SUCC);
    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

TEST(Insert, Composited_Concatenation)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"hash_pkey\","
        "        \"Type\": \"Composited\","
        "        \"Definition\": \"(| pkey ($Hex($Crc64Str pkey)))\""
        "      }"
        "    ],"
        "    \"RequiredAttributes\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Str\""
        "      }"
        "    ]"
        "  }"
        "}"
        "]";
    FileSyncLogger logger("Insert.Composited_Concatenation.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    map<Json::Value, static_index::BatchWriteResponse> resps;
    const char kRequest0[] =
        "{\n"
        "   \"PutRows\" : [\n"
        "      {\n"
        "         \"Attrs\" : {\n"
        "            \"pkey\" : \"123456789\"\n"
        "         },\n"
        "         \"Pkey\" : [\n"
        "            {\n"
        "               \"hash_pkey\" : \"123456789|E9C6D914C4B8D9CA\"\n"
        "            }\n"
        "         ],\n"
        "         \"TableName\" : \"test\"\n"
        "      }\n"
        "   ]\n"
        "}\n";
    const char kResponse0[] = "{"
        "  \"PutRows\": ["
        "    {}"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, BatchWrite(_, _))
        .WillRepeatedly(Invoke(bind(&MockBatchWrite, _1, _2, &logger, &hits, cref(resps))));
    FlagSetter<int64_t> bwl(BulkExecutor::sBatchWriteLimit, 1);
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));

    const char data[] = "{\"pkey\": \"123456789\"}";
    int r = sind->Insert("test", FromJsonStr(data));
    OTS_ASSERT(&logger, r == FICUS_SUCC);
    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

TEST(Insert, Composited_Crc64Str)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"hash_pkey\","
        "        \"Type\": \"Composited\","
        "        \"Definition\": \"($Hex ($Crc64Str pkey))\""
        "      }"
        "    ],"
        "    \"RequiredAttributes\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Str\""
        "      }"
        "    ]"
        "  }"
        "}"
        "]";
    FileSyncLogger logger("Insert.Composited_Crc64Str.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    map<Json::Value, static_index::BatchWriteResponse> resps;
    const char kRequest0[] =
        "{\n"
        "   \"PutRows\" : [\n"
        "      {\n"
        "         \"Attrs\" : {\n"
        "            \"pkey\" : \"123456789\"\n"
        "         },\n"
        "         \"Pkey\" : [\n"
        "            {\n"
        "               \"hash_pkey\" : \"E9C6D914C4B8D9CA\"\n"
        "            }\n"
        "         ],\n"
        "         \"TableName\" : \"test\"\n"
        "      }\n"
        "   ]\n"
        "}\n";
    const char kResponse0[] = "{"
        "  \"PutRows\": ["
        "    {}"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, BatchWrite(_, _))
        .WillRepeatedly(Invoke(bind(&MockBatchWrite, _1, _2, &logger, &hits, cref(resps))));
    FlagSetter<int64_t> bwl(BulkExecutor::sBatchWriteLimit, 1);
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));

    const char data[] = "{\"pkey\": \"123456789\"}";
    int r = sind->Insert("test", FromJsonStr(data));
    OTS_ASSERT(&logger, r == FICUS_SUCC);
    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

TEST(Insert, Composited_ShiftToUint64)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"hash_pkey\","
        "        \"Type\": \"Composited\","
        "        \"Definition\": \"($ShiftToUint64 pkey)\""
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
    FileSyncLogger logger("Insert.Composited_ShiftToUint64.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    map<Json::Value, static_index::BatchWriteResponse> resps;
    const char kRequest0[] =
        "{\n"
        "   \"PutRows\" : [\n"
        "      {\n"
        "         \"Attrs\" : {\n"
        "            \"pkey\" : 0\n"
        "         },\n"
        "         \"Pkey\" : [\n"
        "            {\n"
        "               \"hash_pkey\" : -9223372036854775808\n"
        "            }\n"
        "         ],\n"
        "         \"TableName\" : \"test\"\n"
        "      }\n"
        "   ]\n"
        "}\n";
    const char kResponse0[] = "{"
        "  \"PutRows\": ["
        "    {}"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, BatchWrite(_, _))
        .WillRepeatedly(Invoke(bind(&MockBatchWrite, _1, _2, &logger, &hits, cref(resps))));
    FlagSetter<int64_t> bwl(BulkExecutor::sBatchWriteLimit, 1);
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));

    const char data[] = "{\"pkey\": 0}";
    int r = sind->Insert("test", FromJsonStr(data));
    OTS_ASSERT(&logger, r == FICUS_SUCC);
    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

TEST(Insert, Index)
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
        "        {"
        "          \"Name\": \"hash_name\","
        "          \"Type\": \"Composited\","
        "          \"Definition\": \"($Hex ($Crc64Str name))\""
        "        }"
        "      ],"
        "      \"RequiredAttributes\": ["
        "        {"
        "          \"Name\": \"name\","
        "          \"Type\": \"Str\""
        "        }"
        "      ],"
        "      \"OptionalAttributes\": ["
        "        {"
        "          \"Name\": \"pkey\","
        "          \"Type\": \"Int\""
        "        }"
        "      ]"
        "    }"
        "  ]"
        "}"
        "]";
    FileSyncLogger logger("Insert.Index.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    map<Json::Value, static_index::BatchWriteResponse> resps;
    const char kRequest0[] =
        "{\n"
        "   \"PutRows\" : [\n"
        "      {\n"
        "         \"Attrs\" : {\n"
        "            \"name\" : \"123456789\",\n"
        "            \"pkey\" : 0\n"
        "         },\n"
        "         \"Pkey\" : [\n"
        "            {\n"
        "               \"hash_name\" : \"E9C6D914C4B8D9CA\"\n"
        "            }\n"
        "         ],\n"
        "         \"TableName\" : \"index\"\n"
        "      }\n"
        "   ]\n"
        "}\n";
    const char kResponse0[] = "{"
        "  \"PutRows\": ["
        "    {}"
        "  ]"
        "}";
    const char kRequest1[] =
        "{\n"
        "   \"PutRows\" : [\n"
        "      {\n"
        "         \"Attrs\" : {\n"
        "            \"greeting\" : \"hello\",\n"
        "            \"name\" : \"123456789\"\n"
        "         },\n"
        "         \"Pkey\" : [\n"
        "            {\n"
        "               \"pkey\" : 0\n"
        "            }\n"
        "         ],\n"
        "         \"TableName\" : \"test\"\n"
        "      }\n"
        "   ]\n"
        "}\n";
    const char kResponse1[] = "{"
        "  \"PutRows\": ["
        "    {}"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest1)]), FromJsonStr(kResponse1));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, BatchWrite(_, _))
        .WillRepeatedly(Invoke(bind(&MockBatchWrite, _1, _2, &logger, &hits, cref(resps))));
    FlagSetter<int64_t> bwl(BulkExecutor::sBatchWriteLimit, 1);
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));

    const char data[] = "{\"pkey\": 0, \"name\": \"123456789\", \"greeting\": \"hello\"}";
    int r = sind->Insert("test", FromJsonStr(data));
    OTS_ASSERT(&logger, r == FICUS_SUCC);
    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

TEST(Insert, Index_MissingRequired)
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
        "        {"
        "          \"Name\": \"hash_name\","
        "          \"Type\": \"Composited\","
        "          \"Definition\": \"($Crc64Str name)\""
        "        }"
        "      ],"
        "      \"RequiredAttributes\": ["
        "        {"
        "          \"Name\": \"name\","
        "          \"Type\": \"Str\""
        "        }"
        "      ]"
        "    }"
        "  ]"
        "}"
        "]";
    FileSyncLogger logger("Insert.Index_MissingRequired.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    map<Json::Value, static_index::BatchWriteResponse> resps;
    const char kRequest0[] =
        "{\n"
        "   \"PutRows\" : [\n"
        "      {\n"
        "         \"Attrs\" : {},\n"
        "         \"Pkey\" : [\n"
        "            {\n"
        "               \"pkey\" : 0\n"
        "            }\n"
        "         ],\n"
        "         \"TableName\" : \"test\"\n"
        "      }\n"
        "   ]\n"
        "}\n";
    const char kResponse0[] = "{"
        "  \"PutRows\": ["
        "    {}"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, BatchWrite(_, _))
        .WillRepeatedly(Invoke(bind(&MockBatchWrite, _1, _2, &logger, &hits, cref(resps))));
    FlagSetter<int64_t> bwl(BulkExecutor::sBatchWriteLimit, 1);
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));

    const char data[] = "{\"pkey\": 0}";
    int r = sind->Insert("test", FromJsonStr(data));
    OTS_ASSERT(&logger, r == FICUS_SUCC);
    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

TEST(Insert, Index_MultiplePkey)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"hash_face_image_id\","
        "        \"Type\": \"Composited\","
        "        \"Definition\": \"($Hex ($Crc64Int face_image_id))\""
        "      },"
        "      {"
        "        \"Name\": \"face_image_id\","
        "        \"Type\": \"Int\""
        "      }"
        "    ],"
        "    \"RequiredAttributes\": ["
        "      {"
        "        \"Name\": \"face_image_id\","
        "        \"Type\": \"Int\""
        "      }"
        "    ]"
        "  },"
        "  \"Indexes\": ["
        "    {"
        "      \"TableName\": \"face_meta_on_name\","
        "      \"PrimaryKeys\": ["
        "        {"
        "          \"Name\": \"hash_name\","
        "          \"Type\": \"Composited\","
        "          \"Definition\": \"($Hex ($Crc64Str name))\""
        "        },"
        "        {"
        "          \"Name\": \"face_image_id\","
        "          \"Type\": \"Int\""
        "        }"
        "      ],"
        "      \"RequiredAttributes\": ["
        "        {"
        "          \"Name\": \"name\","
        "          \"Type\": \"Str\""
        "        },"
        "        {"
        "          \"Name\": \"face_image_id\","
        "          \"Type\": \"Int\""
        "        }"
        "      ]"
        "    }"
        "  ]"
        "}"
        "]";
    FileSyncLogger logger("Insert.Index_MultiplePkey.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    map<Json::Value, static_index::BatchWriteResponse> resps;
    const char kRequest0[] =
        "{\"PutRows\":["
        "  {\"Attrs\":{\"name\":\"name_50\"},"
        "   \"Pkey\":[{\"hash_name\":\"82E15CB012BAE77F\"},{\"face_image_id\":50}],"
        "   \"TableName\":\"face_meta_on_name\"}"
        "]}\n";
    const char kResponse0[] = "{"
        "  \"PutRows\": ["
        "    {}"
        "  ]"
        "}";
    const char kRequest1[] =
        "{\"PutRows\":["
        "  {\"Attrs\":{\"name\":\"name_50\"},"
        "   \"Pkey\":"
        "      [{\"hash_face_image_id\":\"5F2E9A834B9C09E4\"},{\"face_image_id\":50}],"
        "   \"TableName\":\"test\"}"
        "]}\n";
    const char kResponse1[] = "{"
        "  \"PutRows\": ["
        "    {}"
        "  ]"
        "}";
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    Unjsonize(&logger, &(resps[FromJsonStr(kRequest1)]), FromJsonStr(kResponse1));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, BatchWrite(_, _))
        .WillRepeatedly(Invoke(bind(&MockBatchWrite, _1, _2, &logger, &hits, cref(resps))));
    FlagSetter<int64_t> bwl(BulkExecutor::sBatchWriteLimit, 1);
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));

    const char data[] = "{\"face_image_id\": 50, \"name\": \"name_50\"}";
    int r = sind->Insert("test", FromJsonStr(data));
    OTS_ASSERT(&logger, r == FICUS_SUCC);
    FOREACH_ITER(i, resps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}
