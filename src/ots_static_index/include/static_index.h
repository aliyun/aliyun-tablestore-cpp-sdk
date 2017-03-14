#ifndef ALIYUN_OTS_STATIC_INDEX_H
#define ALIYUN_OTS_STATIC_INDEX_H

#include "exceptional.h"
#include "logger.h"
#include "thread_pool.h"
#include "client_delegate.h"
#include "jsoncpp/json/value.h"
#include <vector>
#include <stdint.h>

// error code write to here first
#define FICUS_SUCC 0
#define FICUS_OBJECT_STORAGE_CONNECTION_ERROR -1
#define FICUS_OBJECT_STORAGE_OPERATION_ERROR -2

namespace static_index {

class Settings
{
public:
    ::Json::Value mSchema; // 一个json array，每一个元素都是之前定义的表配置。
    ThreadPool* mThreadPool; // 共享的线程池。not own。
    Logger* mLogger; // 用户定义的日志机制。not own。
};

class StaticallyIndexed
{
public:
    virtual ~StaticallyIndexed() {}

    static StaticallyIndexed* New(
        const Settings& /* copy */,
        ClientDelegate* /* not own */);

    /**
     * 插入新行。
     *
     * - 同时写入各索引表，最后写入主表。
     * - 如果该行已存在，会报ServiceException。
     * - 如果insertData里缺少任何一个主表的RequiredAttributes，会报错。
     * - 如果insertData里缺少某一个索引表的RequiredAttributes，则不会写入该索引表，但是会写入主表和其他索引表。
     * - 如果某字段的值为json object或者json list，则将该值的json作为字符串整体写入。
     * - 如果某字段的值为null，则不写入该字段。
     */
    virtual int Insert(
        const ::std::string& collection, const ::Json::Value& insertData) =0;
    
    virtual int Insert(
        const ::std::string& collection,
        const ::std::vector< ::Json::Value >& insertData) =0;

    /**
     * 查询所有满足条件的行。
     *
     * - projection 要求是由json string构成的json array，标明了要返回的列。可以是主表的主键列。不可以是索引表的主键列；若指定，行为未定义。
     * - 最初的 start 个 object 将会被跳过, 最多返回 limit 个结果(limit=0 表示无限制)。
     * - 选择索引表的算法：匹配的前缀数比上required attributes数最大的索引表被选中。如果该比数相同，主表被选中。如果没有索引表或主表被选中，则扫描整个主表。
     * - 前缀匹配的定义：
     *   + 该列之前的所有主键列都被认为匹配。
     *   + 对于hash过的主键列，只有point query认为匹配（{"face_image_id": 5}，或者{"face_image_id": {"$eq", 5}}，或者{"face_image_id": {"$in": [5]}}）。
     *   + 对于非hash过的主键列，比较关系也认为匹配。但$ne不被认为匹配。
     *   + 只有最后一个匹配可以是比较关系。
     * - $in会被拆分成多个请求发往服务端。在多个字段上使用$in，实际的请求个数是各个字段可选值数量的乘积。
     *   故而，请谨慎使用$in。
     * - 若指定order，则总会读回所有满足condition的数据再进行排序。
     */
    virtual int Find(
        const ::std::string& collection,
        const Json::Value& projection,
        const Json::Value& condition,
        int64_t start,
        int64_t limit,
        const Json::Value& order,
        ::std::vector<Json::Value>& out) = 0;

    /**
     * 在指定 collection 中将满足 condition 的所有 object 删除
     *
     * - 索引表的选择算法参见Find接口。
     * - 删除逻辑是：1. 读索引表定位主表；2. 读取主表对应的行；3. 删除主表这一行；4. 同时删除索引表相应行。
     *   注意：这里不保证原子性。
     */
    virtual int Delete(
        const ::std::string& collection,
        const ::Json::Value& condition) =0;


    /**
     * 在指定 collection 中查找满足 condition 的所有 object 的个数, 结果返回在 total 中
     *      最初的 start 个 object 将会被跳过, 最多返回 limit 个结果
     * 参数说明：
     *     参考Find接口
     */
    virtual int Count(
        const ::std::string& collection,
        const ::Json::Value& condition,
        int64_t start,
        int64_t limit,
        uint64_t& total) = 0;

    /**
     * 修改已存在的行。
     *
     * - condition内只能有face_image_id，并且只能对其进行point query。否则报ClientException。
     * - newData 要求是objectValue, 允许使用修饰符$set
     *   当包含$set修饰符时，update会仅修改newData中的域；当不包含$set修饰符时，update会用新记录替换掉原记录
     *     样例：newData = {"$set" : {"person_id" : 1, "name" : "wahaha"}}
     *           newData = {"person_id" : 1, "name" : "wahaha"}
     */
    virtual int Update(
        const ::std::string& collection,
        const ::Json::Value& condition,
        const ::Json::Value& newData) = 0;
    

    /**
     * 插入新行，或者修改已存在的行。
     *
     * - condition内只能有face_image_id，并且只能对其进行point query。否则报ClientException。
     */
    virtual int Upsert(
        const ::std::string& collection,
        const ::Json::Value& condition,
        const ::Json::Value& newData) = 0;
};

} // namespace static_index

#endif /* ALIYUN_OTS_STATIC_INDEX_H */

