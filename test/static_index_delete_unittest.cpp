#include "static_index_common.h"

using namespace ::std;
using namespace ::std::tr1;
using namespace ::std::tr1::placeholders;
using namespace ::testing;
using namespace ::aliyun::openservices::ots;
using namespace ::static_index;

TEST(Delete, Primary)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Str\""
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
    FileSyncLogger logger("Delete.Primary.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    map<Json::Value, static_index::GetRangeResponse> getRangeResps;
    map<Json::Value, static_index::BatchWriteResponse> batchWriteResps;
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
        "        {\"Name\": \"pkey\",\"Value\": \"a\"}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": \"b\"}"
        "      ]"
        "    }"
        "  ]"
        "}";
    const char kRequest1[] = "{"
        "  \"DelRows\": ["
        "    {\"TableName\":\"test\",\"PrimaryKey\": [{\"Name\": \"pkey\", \"Value\": \"a\"}]}"
        "  ]"
        "}";
    const char kResponse1[] = "{"
        "  \"DelRows\": ["
        "    {}"
        "  ]"
        "}";
    const char kRequest2[] = "{"
        "  \"DelRows\": ["
        "    {\"TableName\":\"test\",\"PrimaryKey\": [{\"Name\": \"pkey\", \"Value\": \"b\"}]}"
        "  ]"
        "}";
    const char kResponse2[] = "{"
        "  \"DelRows\": ["
        "    {}"
        "  ]"
        "}";
    Unjsonize(&logger, &(getRangeResps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    Unjsonize(&logger, &(batchWriteResps[FromJsonStr(kRequest1)]), FromJsonStr(kResponse1));
    Unjsonize(&logger, &(batchWriteResps[FromJsonStr(kRequest2)]), FromJsonStr(kResponse2));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, GetRange(_, _))
        .WillRepeatedly(Invoke(bind(&MockGetRange, _1, _2, &logger, &hits, cref(getRangeResps))));
    EXPECT_CALL(client, BatchWrite(_, _))
        .WillRepeatedly(Invoke(bind(&MockBatchWrite, _1, _2, &logger, &hits, cref(batchWriteResps))));
    FlagSetter<int64_t> bwl(BulkExecutor::sBatchWriteLimit, 1);
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));
    
    vector<Json::Value> result;
    Json::Value condition = Json::objectValue;
    Json::Value projection = Json::arrayValue;
    Json::Value order = Json::objectValue;
    int r = sind->Delete("test", condition);
    OTS_ASSERT(&logger, r == FICUS_SUCC);
    FOREACH_ITER(i, getRangeResps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
    FOREACH_ITER(i, batchWriteResps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

TEST(Delete, Index)
{
    const char schema[] = "["
        "{"
        "  \"PrimaryTable\": {"
        "    \"TableName\": \"test\","
        "    \"PrimaryKeys\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Str\""
        "      }"
        "    ],"
        "    \"RequiredAttributes\": ["
        "      {"
        "        \"Name\": \"pkey\","
        "        \"Type\": \"Str\""
        "      }"
        "    ]"
        "  },"
        "  \"Indexes\": ["
        "    {"
        "      \"TableName\": \"index\","
        "      \"PrimaryKeys\": ["
        "        {"
        "          \"Name\": \"attr\","
        "          \"Type\": \"Str\""
        "        }"
        "      ],"
        "      \"RequiredAttributes\": ["
        "        {"
        "          \"Name\": \"attr\","
        "          \"Type\": \"Str\""
        "        }"
        "      ]"
        "    }"
        "  ]"
        "}"
        "]";
    FileSyncLogger logger("Delete.Index.log");
    auto_ptr<ThreadPool> tp(ThreadPool::New(&logger, 10, 1000));
    Settings settings;
    settings.mThreadPool = tp.get();
    settings.mLogger = &logger;
    settings.mSchema = FromJsonStr(schema);
    MockClientDelegate client;
    map<Json::Value, static_index::GetRangeResponse> getRangeResps;
    map<Json::Value, static_index::BatchWriteResponse> batchWriteResps;
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
        "        {\"Name\": \"pkey\",\"Value\": \"a\"}"
        "      ],"
        "      \"Attributes\": ["
        "        {\"Name\": \"attr\",\"Value\":\"A\"}"
        "      ]"
        "    },"
        "    {"
        "      \"PrimaryKeys\": ["
        "        {\"Name\": \"pkey\",\"Value\": \"b\"}"
        "      ]"
        "    }"
        "  ]"
        "}";
    const char kRequest1[] = "{"
        "  \"DelRows\": ["
        "    {\"TableName\":\"test\",\"PrimaryKey\": [{\"Name\": \"pkey\", \"Value\": \"a\"}]}"
        "  ]"
        "}";
    const char kResponse1[] = "{"
        "  \"DelRows\": ["
        "    {}"
        "  ]"
        "}";
    const char kRequest2[] = "{"
        "  \"DelRows\": ["
        "    {\"TableName\":\"test\",\"PrimaryKey\": [{\"Name\": \"pkey\", \"Value\": \"b\"}]}"
        "  ]"
        "}";
    const char kResponse2[] = "{"
        "  \"DelRows\": ["
        "    {}"
        "  ]"
        "}";
    const char kRequest3[] = "{"
        "  \"DelRows\": ["
        "    {\"TableName\":\"index\",\"PrimaryKey\": [{\"Name\": \"attr\", \"Value\": \"A\"}]}"
        "  ]"
        "}";
    const char kResponse3[] = "{"
        "  \"DelRows\": ["
        "    {}"
        "  ]"
        "}";
    Unjsonize(&logger, &(getRangeResps[FromJsonStr(kRequest0)]), FromJsonStr(kResponse0));
    Unjsonize(&logger, &(batchWriteResps[FromJsonStr(kRequest1)]), FromJsonStr(kResponse1));
    Unjsonize(&logger, &(batchWriteResps[FromJsonStr(kRequest2)]), FromJsonStr(kResponse2));
    Unjsonize(&logger, &(batchWriteResps[FromJsonStr(kRequest3)]), FromJsonStr(kResponse3));
    map<Json::Value, int64_t> hits;
    EXPECT_CALL(client, GetRange(_, _))
        .WillRepeatedly(Invoke(bind(&MockGetRange, _1, _2, &logger, &hits, cref(getRangeResps))));
    EXPECT_CALL(client, BatchWrite(_, _))
        .WillRepeatedly(Invoke(bind(&MockBatchWrite, _1, _2, &logger, &hits, cref(batchWriteResps))));
    FlagSetter<int64_t> bwl(BulkExecutor::sBatchWriteLimit, 1);
    auto_ptr<StaticallyIndexed> sind(StaticallyIndexed::New(settings, &client));
    
    vector<Json::Value> result;
    Json::Value condition = Json::objectValue;
    Json::Value projection = Json::arrayValue;
    Json::Value order = Json::objectValue;
    int r = sind->Delete("test", condition);
    OTS_ASSERT(&logger, r == FICUS_SUCC);
    FOREACH_ITER(i, getRangeResps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
    FOREACH_ITER(i, batchWriteResps) {
        const Json::Value& req = i->first;
        OTS_ASSERT(&logger, hits.count(req))(ToString(req));
    }
}

