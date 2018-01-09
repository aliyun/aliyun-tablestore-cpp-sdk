# C++ SDK for Aliyun TableStore使用手册

## 环境准备与编译

关于环境，简单来讲就这么几条：
* 支持C++98 TR1（别名C++03）。对C++ 11/14的支持还在开发中
* 支持64bit。理论上也支持32bit，但官方没测试过。
* 对windows的支持还在开发中
* 对BSD家的系统（比如MacOS）支持还在开发中
* 对Linux家的系统，支持一部分。官方测试过的包括centos7和debian8。

下面进入瞎叨叨环节。

说到写C/C++库，最难的是什么？
最难的是说明自己支持什么系统、依赖什么库。

Linux主流发行版计有redhat enterprise, fedora, centos, debian, ubuntu, archlinux等等；
BSD家有FreeBSD, NetBSD, Dragonfly，以及大名鼎鼎的MacOS；
Windows得益于M记近乎偏执的向后兼容性，可以认为只有一个版本。
所有这些系统，各有包管理系统，包名也不尽相同。

而且所有这些系统还都有32bit/64bit之分。

好在时代的车轮已经到了10年代。
我们有docker。
我们把官方支持的系统都做了dockerfile，放在`docker`目录下。
拿debian8为例，打开`docker/debian8/Dockerfile`，可以看到这样两行：

```
RUN apt-get install -y scons g++ libboost-all-dev protobuf-compiler libprotobuf-dev uuid-dev libssl-dev
RUN apt-get install -y ca-certificates # for HTTPS support
```

这两行说明SDK依赖
* scons: 编译系统。别问为什么不支持make, cmake, automake, bazar, ... 这么多选择，总得挑一个不是？
* gcc & boost: 不解释
* protobuf: 序列化库
* uuid: 顾名思义
* openssl: 签名，以及支持HTTPS所用
* ca-certificates: 仅为支持HTTPS所用。如果您只用表格存储的HTTP地址，可以不安装这个库。不过我们建议还是走HTTPS较为稳妥。

好了，如果您的系统是debian8，并且安装了上面这些库，您就可以编译SDK。
方法是下载SDK，并到SDK的根目录执行`scons`，像下面这样
```
$ git clone https://github.com/aliyun/aliyun-tablestore-cpp-sdk.git
$ cd aliyun-tablestore-cpp-sdk
$ scons -j4
```

当一切结束，一个tar包就编译好了。
包名通常可以在`scons`最后的输出中找到。
比如debian8系统，包名及所在的路径是
```
build/release/pkg/aliyun-tablestore-cpp98-sdk-4.4.1-debian8.9-x86_64.tar.gz
```
我们可以看到包名里包含这样几个要素：
* C++版本（C++98）
* SDK版本（4.4.1）
* OS版本（debian8.9）
* OS架构（x86_64）

包里有些啥？
让我们瞅一瞅。
```
#tar -tf build/release/pkg/aliyun-tablestore-cpp98-sdk-4.4.1-debian8.9-x86_64.tar.gz
version.ini
lib/libtablestore_core.so
lib/libtablestore_core_static.a
lib/libtablestore_util.so
lib/libtablestore_util_static.a
include/tablestore/util/arithmetic.hpp
include/tablestore/util/assert.hpp
include/tablestore/util/foreach.hpp
include/tablestore/util/iterator.hpp
include/tablestore/util/logger.hpp
include/tablestore/util/logging.hpp
include/tablestore/util/mempiece.hpp
include/tablestore/util/mempool.hpp
include/tablestore/util/metaprogramming.hpp
include/tablestore/util/move.hpp
include/tablestore/util/optional.hpp
include/tablestore/util/prettyprint.hpp
include/tablestore/util/random.hpp
include/tablestore/util/result.hpp
include/tablestore/util/security.hpp
include/tablestore/util/seq_gen.hpp
include/tablestore/util/threading.hpp
include/tablestore/util/timestamp.hpp
include/tablestore/util/try.hpp
include/tablestore/util/assert.ipp
include/tablestore/util/iterator.ipp
include/tablestore/util/logging.ipp
include/tablestore/util/mempiece.ipp
include/tablestore/util/move.ipp
include/tablestore/util/prettyprint.ipp
include/tablestore/core/client.hpp
include/tablestore/core/error.hpp
include/tablestore/core/range_iterator.hpp
include/tablestore/core/retry.hpp
include/tablestore/core/types.hpp
```
里面有：
* 版本文件：`version.ini`
* 库文件：`lib/`下所有文件。其中`libtablestore_core_static.a`依赖`libtablestore_util_static.a`，动态库也类似。
* 头文件：`include/`下所有文件。

用户需要自行将头文件放到自己的代码库里，将库文件放到自己的编译环境里。

然而，以上所说并非C++使用三方库的正确姿势。
你真的能肯定拿一个.so或者.a链接到你的程序里是没问题的吗？
编译参数一致不一致？
编译器兼容不兼容？
运行环境或者编译环境有一天要升级怎么办？

以我几十年C++老司机的套路，我会选择把三方库的源码放在我自己的代码库里，用编译我自己代码库的编译方式去编译三方库的代码。
虽然SDK用了scons，我自己去用的时候未必还用这套sconscript。
* 注：直接复制源码的话会缺少`buildinfo.cpp`。可以先用scons编译一下，然后把`build/release/src/tablestore/core/impl/buildinfo.cpp`复制出来。

所以，别问我为什么用scons，不用make, cmake, bazar……。
也别问我为什么不出rpm/deb，不提供yum源/apt源。

## 最简短样例

```c++
void syncListTable()
{
    Endpoint ep("YourEndpoint", "YourInstance");
    Credential cr("AccessKeyId", "AccessKeySecret");
    ClientOptions opts;
    SyncClient* pclient = NULL;
    {
        Optional<OTSError> res = SyncClient::create(pclient, ep, cr, opts);
        assert(!res.present());
    }
    auto_ptr<SyncClient> client(pclient);
    sleep(30); // wait a while for connection ready
    ListTableRequest req;
    ListTableResponse resp;
    Optional<OTSError> res = client->listTable(resp, req);
    assert(!res.present());
    const IVector<string>& xs = resp.tables();
    for(int64_t i = 0; i < xs.size(); ++i) {
        cout << xs[i] << endl;
    }
}
```

## 配置

### Endpoint

`aliyun::tablestore::core::Endpoint`是一个包含_实例访问地址_和_实例名_的结构。
典型的构造方法是传入实例访问地址和实例名。
例如
```c++
Endpoint ep("https://实例名.地域.ots.aliyuncs.com", "实例名");
```

### Credential

`aliyun::tablestore::core::Credential`是为认证鉴权提供的结构。

其最简单的形式是直接提供主账号或子账号的AccessKey。
我们推荐使用子账号。
```c++
Credential cred("AccessKeyId", "AcceesKeySecret");
```

我们同时也支持Security Token。
```c++
Credential cred("AccessKeyId", "AcceesKeySecret", "SecurityToken");
```

### ClientOptions

`aliyun::tablestore::core::ClientOptions`包含一些SDK运行中需要的其他项目。
默认构造可以适用最常见的场景。

具体来讲有以下这些配置项。
* `mMaxConnections`

  最大连接数，同时也是最大并发请求数。
  SDK和表格存储服务端保持着长连接。
  每次有一个新的请求都会从闲置的连接里随机挑一个来发送请求。
* `mConnectTimeout`

  连接超时时间。
  考虑到DNS解析的时间，连接超时时间不宜短于10秒。
* `mRequestTimeout`

  请求超时时间。
* `mRetryStrategy`

  重试策略。
  默认的重试策略会在10秒内重试失败的幂等请求。
  用户可以[定义自己的重试策略](#重试策略)。
* `mLogger`

  日志记录器。
  默认的日志记录器输出到标准错误上。
  我们建议用户[定义自己的日志记录器](#日志)。
* `mActors`

  线程池。
  用于执行用户回调的线程池。
  默认10根线程。

## 同步客户端使用说明

### 构造

**我们强烈建议一个进程内复用同一个客户端**

SDK构造客户端有两种方法
* 直接构造

  ```c++
  Endpoint ep("YourEndpoint", "YourInstance");
  Credential cr("AccessKeyId", "AccessKeySecret");
  ClientOptions opts;
  SyncClient* client = NULL;
  Optional<OTSError> res = SyncClient::create(client, ep, cr, opts);
  ```
  准备好`Endpoint`, `Credential`, `ClientOptions`和一根空的`SyncClient`指针，调用`SyncClient::create()`。
  需要注意的是，`SyncClient::create()`会检查传入的参数是否合理，因而有可能会返回`OTSError`。
  用户必须检查是否有错误。

  直接构造之后，客户端会在后台与表格存储服务端建立并维护连接。
  于是，一，用刚建好的客户端发送请求有可能会碰到`OTSNoAvailableConnection`；二，客户端的并发数会有一个上升的过程。

* 从`AsyncClient`构造

  ```c++
  AsyncClient& async = ...;
  SyncClient* sync = SyncClient::create(async);
  ```
  此时同步、异步两个客户端共享同一个底层结构（但是各自持有各自的引用计数）。

### 列举表

我们用`SyncClient::listTable()`来列举实例下的所有表。
```c++
SyncClient* client = ...;
ListTableRequest req;
ListTableResponse resp;
Optional<OTSError> res = client->listTable(resp, req);
```
在调用`SyncClient::listTable()`之前事先准备好
* 作为请求内容的`ListTableRequest`对象
* 盛放返回内容的`ListTableResponse`对象

并且在之后判断返回值来判断是否发生[错误](#错误处理)。

我们可以通过`ListTableResponse::tables()`方法来获取一个表名的列表。
比如我们可以输出实例下的所有表
```c++
const IVector<string>& xs = resp.tables();
for(int64_t i = 0; i < xs.size(); ++i) {
    cout << xs[i] << endl;
}
```

### 建表

```c++
CreateTableRequest req;
{
    // immutable configurations of the table
    TableMeta& meta = req.mutableMeta();
    meta.mutableTableName() = "simple_create_delete_table";
    {
        // with exactly one integer primary key column
        Schema& schema = meta.mutableSchema();
        PrimaryKeyColumnSchema& pkColSchema = schema.append();
        pkColSchema.mutableName() = "pkey";
        pkColSchema.mutableType() = kPKT_Integer;
    }
}
CreateTableResponse resp;
Optional<OTSError> res = client.createTable(resp, req);
```

创建表时必须指定表的名字和主键。
主键包含 1~4 个主键列，每一个主键列都有名字和类型。
示例中的表，名为`simple_create_delete_table`；主键只含一个主键列，名为`pkey`，类型为整型(`kPKT_Integer`)。

表格存储的表可以设置自增主键列，请看[这里](#自增主键列)。

#### 可变参数

表上可以设置若干可变参数。
这些参数既可以在建表时设定，也可以通过[更新表参数](#更新表参数)来修改。
以下是一个建表时设定[预留读写吞吐量](https://help.aliyun.com/document_detail/27284.html#h2--)的示例：

```c++
CreateTableRequest req;
{
    // immutable configurations of the table
    TableMeta& meta = req.mutableMeta();
    meta.mutableTableName() = "create_table_with_reserved_throughput";
    {
        // with exactly one integer primary key column
        Schema& schema = meta.mutableSchema();
        PrimaryKeyColumnSchema& pkColSchema = schema.append();
        pkColSchema.mutableName() = "pkey";
        pkColSchema.mutableType() = kPKT_Integer;
    }
}
{
    TableOptions& opts = req.mutableOptions();
    {
        // 0 reserved read capacity-unit, 1 reserved write capacity-unit
        CapacityUnit cu(0, 1);
        opts.mutableReservedThroughput().reset(util::move(cu));
    }
}
CreateTableResponse resp;
Optional<OTSError> res = client.createTable(resp, req);
```

所有可变参数罗列于此：
* [数据生命周期](https://help.aliyun.com/document_detail/52618.html)(`mutableTimeToLive()`)

  默认值为-1(即永不过期)

* [最大版本数](https://help.aliyun.com/document_detail/27281.html)(`mutableMaxVersions()`)

  默认值为1

* [有效版本偏差](https://help.aliyun.com/document_detail/52624.html)(`mutableMaxTimeDeviation()`)

  默认值为86400秒（即一天）

* [预留读写吞吐量](https://help.aliyun.com/document_detail/27284.html#h2--)(`mutableReservedThroughput()`)

  默认读写皆为0（即全部读写按量计费）

### 删表

删除一张表只需指定表名。

```c++
DeleteTableRequest req;
req.mutableTable() = "YourTable";
DeleteTableResponse resp;
Optional<OTSError> res = client.deleteTable(resp, req);
```

### 更新表参数

表上的可变参数可以随需更改。
这些参数的解释请参考[建表](#可变参数)一节。
下面是一个更新预留吞吐量的示例。

```c++
UpdateTableRequest req;
req.mutableTable() = "YourTable";
UpdateTableResponse resp;
{
    TableOptions& opts = req.mutableOptions();
    {
        // 0 reserved read capacity-unit, 1 reserved write capacity-unit
        CapacityUnit cu(0, 1);
        opts.mutableReservedThroughput().reset(util::move(cu));
    }
}
Optional<OTSError> res = client.updateTable(resp, req);
```

### 获取表信息

```c++
DescribeTableRequest req;
req.mutableTable() = "YourTable";
DescribeTableResponse resp;
Optional<OTSError> res = client.describeTable(resp, req);
```

通过`describeTable()`接口我们可以获取：
* 表的状态。包括
  - `kTS_Active`：表可以正常提供读写服务。
  - `kTS_Inactive`：表上不可读写，但表上数据保留。通常这个状态出现在主备表切换时。
  - `kTS_Loading`：正在建表过程中。表上仍然不可读写。
  - `kTS_Unloading`：正在删表过程中。表上不可读写。
  - `kTS_Updating`：正在更新表可变参数中。表上不可读写。
* 表meta。参见[建表](#建表)。
* 表的可变参数。参见[可变参数](#可变参数)。
* 分片之间的分割点。
  表格存储上的一张表被水平切分成若干分片。
  通过这个接口可以拿到各分片间的分割点。
  需要提醒的是，由于表格存储会在后台根据负载进行自动分裂与合并，这个接口取到的分割点保证是曾经出现过的分片情况，但不保证与当前情况完全吻合。

### 单行读

```c++
GetRowRequest req;
{
    PointQueryCriterion& query = req.mutableQueryCriterion();
    query.mutableTable() = “YourTable”;
    {
        PrimaryKey& pkey = query.mutablePrimaryKey();
        pkey.append() = PrimaryKeyColumn(
            "pkey",
            PrimaryKeyValue::toStr("some_key")); // 假设主键只有一列，并且类型为字符串
    }
    query.mutableMaxVersions().reset(1);
}
GetRowResponse resp;
Optional<OTSError> res = client.getRow(resp, req);
```

指定表名和一行的主键，读取的结果可能有两种：
若该行存在，则`GetRowResponse`对象返回该行的各主键列以及属性列；
若该行不存在，则`GetRowResponse`对象不含有行，并不会报错。

读操作的更多参数，请参考[读之进阶](#读之进阶)。

### 批量单行读

```c++
BatchGetRowRequest req;
{
    MultiPointQueryCriterion& criterion = req.mutableCriteria().append();
    IVector<MultiPointQueryCriterion::RowKey>& rowkeys = criterion.mutableRowKeys();
    {
        MultiPointQueryCriterion::RowKey& exist = rowkeys.append();
        exist.mutableGet().append() = PrimaryKeyColumn(
            "pkey",
            PrimaryKeyValue::toStr("some key"));
        exist.mutableUserData() = &userDataForSomeKey;
    }
    {
        MultiPointQueryCriterion::RowKey& exist = rowkeys.append();
        exist.mutableGet().append() = PrimaryKeyColumn(
            "pkey",
            PrimaryKeyValue::toStr("another key"));
        exist.mutableUserData() = &userDataForAnotherKey;
    }
    criterion.mutableTable() = "YourTable";
    criterion.mutableMaxVersions().reset(1);
}
BatchGetRowResponse resp;
Optional<OTSError> res = client.batchGetRow(resp, req);
```

批量单行读可以聚合一批读请求，一起发给表格存储的服务端。
* 一个`BatchGetRowRequest`由一批`MultiPointQueryCriterion`对象组成。
  每个`MultiPointQueryCriterion`对象指定一个表以及其他读取参数。
  所以，一个批量单行读请求可以读取同一个实例下不同的多个表。
* 每个`MultiPointQueryCriterion`指定一批主键。
  指定主键的同时也可以关联自定义的数据结构（但是并不持有该数据结构）。
  这些自定义的数据结构不会发送给表格存储的服务端。
  然而`BatchGetRowResponse`里面的行结果会将请求相关联的数据结构交还。
  这个特性可以帮助聚合不同业务场景的单行读，复用同一个表格存储客户端。
* 批量单行读有两种错误形态：
  1. 请求整体错误，比如网络错误。
     这种情况下，错误存放在`batchGetRow()`的返回值里。
  1. 请求整体没有错误，个别行出错。
     这种情况下，错误存放在相应的行的结果里。

  即，仅仅检查请求整体有无错误并不能保证该批量单行读请求完全没有出错。

读操作的更多参数，请参考[读之进阶](#读之进阶)。

### 范围读

```c++
RangeQueryCriterion query;
query.mutableTable() = "YourTable";
query.mutableMaxVersions().reset(1);
{
    PrimaryKey& start = query.mutableInclusiveStart();
    start.append() = PrimaryKeyColumn(
        "pkey",
        PrimaryKeyValue::toInfMin());
}
{
    PrimaryKey& end = query.mutableExclusiveEnd();
    end.append() = PrimaryKeyColumn(
        "pkey",
        PrimaryKeyValue::toInfMax());
}
auto_ptr<AsyncClient> aclient(AsyncClient::create(client));
RangeIterator iter(*aclient, query);
for(;;) {
    Optional<OTSError> err = iter.moveNext();
    if (err.present()) {
        // do something with err
        abort();
    }
    if (!iter.valid()) {
        break;
    }
    Row& row = iter.get();
    // do something with row
}
```

范围读能够读取指定范围内的所有数据。
虽然客户端暴露了底层的接口，但我们强烈建议使用`RangeIterator`。
`RangeIterator`除了封装了底层接口的复杂之处，更利用异步预取来隐藏延迟提高吞吐。
构造`RangeIterator`需要提供
* 异步客户端。请参考[异步客户端使用说明](#异步客户端使用说明)。
* `RangeQueryCriterion`与之前单行读的`PointQueryCriterion`大体类同，除了`RangeQueryCriterion`需要
  - 设定范围的起始点（包含）和终止点（不包含）。
    除了正常主键键值之外，这里还可以使用“负无穷大”和“正无穷大”两个特殊值。
    故名思义，“负无穷大”严格小于所有正常主键列值，“正无穷大”严格大于所有正常主键列值。
  - 设定正向走（由小及大）还是逆向走（由大及小）。
    默认正向走。
    正向走的话，起始点须小于终止点；逆向走的话，起始点须大于终止点。

`RangeIterator`对象是个`Iterator`，提供三个接口：
* `moveNext()`将`RangeIterator`对象移动到下一行。
  刚构造出来的`RangeIterator`对象必须调用一下`moveNext()`才可以取值。
  如果取数据失败，`moveNext()`会将错误在返回值里带出来。
  `RangeIterator`对象走到了范围的终点，并不是一个错误，这个信息将由`valid()`给出。
* `valid()`给出`RangeIterator`对象是否走到了范围的终点。
* 如果`valid()`为`true`，那么可以通过`get()`取到行对象。
  用户可以将`get()`返回的行对象的内容搬移走以避免内存复制。
  如果这样做，紧接着的`get()`将返回搬移后的内容。

读操作的更多参数，请参考[读之进阶](#读之进阶)。

### 读之进阶

#### 指定列读取

表格存储支持宽度无上限的行。
然而通常无需读取整行。
指定若干列读取即可。
`QueryCriterion`(`PointQueryCriterion`, `MultiPointQueryCriterion`和`RangeQueryCriterion`的基类)提供了`mutableColumnsToGet()`方法来指定需要读取的列，既可以是属性列，也可以是主键列，如果为空则读取整行。

如果指定的列在读取的行上不存在，返回的结果里便缺失这个列。
我们并不提供占位符。

在范围读中，如果指定的列全部是属性列，而范围内某行恰好缺少全部指定的列，那么在结果中并不会出现这一行。
如果确实需要感知到该行，可以将主键列加入到指定列之中。

#### 最大版本数与版本范围

每个属性列可以包含多个版本，每个版本号（时间戳）对应一个列值。
读取的时候可以指定读取多少个版本(`mutableMaxVersions()`)以及读取的版本范围(`mutableTimeRange()`)。
- 如果单单指定版本数，则返回所有版本里从新到老至多指定数量个数据。
- 如果单单指定版本范围，则返回该范围内所有数据。
- 如果同时指定版本数和版本范围，则返回版本范围内从新到老至多指定数量个数据。
- 最大版本数和版本范围，至少指定其中之一。

#### 过滤器

表格存储可以在服务端预先过滤到一些行，以便减少网络上传输的数据量。
下面是一个示例。
该示例从全表里过滤出“主键列`pkey`大于1并且属性列`attr`等于"A"”的行。

```c++
RangeQueryCriterion query;
query.mutableTable() = kTableName;
query.mutableMaxVersions().reset(1);
{
  PrimaryKey& start = query.mutableInclusiveStart();
  start.append() = PrimaryKeyColumn(
      "pkey",
      PrimaryKeyValue::toInfMin());
}
{
  PrimaryKey& end = query.mutableExclusiveEnd();
  end.append() = PrimaryKeyColumn(
      "pkey",
      PrimaryKeyValue::toInfMax());
}
{
  // set filter
  shared_ptr<ColumnCondition> pkeyCond(
      new SingleColumnCondition(
          "pkey",
          SingleColumnCondition::kLarger,
          AttributeValue::toInteger(1)));
  shared_ptr<ColumnCondition> attrCond(
      new SingleColumnCondition(
          "attr",
          SingleColumnCondition::kEqual,
          AttributeValue::toStr("A")));
  shared_ptr<CompositeColumnCondition> top(new CompositeColumnCondition());
  top->mutableOp() = CompositeColumnCondition::kAnd;
  top->mutableChildren().append() = pkeyCond;
  top->mutableChildren().append() = attrCond;
  query.mutableFilter() = top;
}
```

过滤器是一个树形结构，以逻辑运算（`CompositeColumnCondition`）为内节点，以比较判断(`SingleColumnCondition`)为叶节点。
过滤器作用于其他条件筛选的结果上。
所以，如果过滤器中有某一列，而指定列读取中未指定该列，或版本范围内该列没有列值，则过滤器都认为该列缺失。
* `CompositeColumnCondition`支持与、或、非。其中与和或可以挂载2个或更多子树，非只能挂载一棵子树。
* `SingleColumnCondition`支持全部6种比较条件（等于、不等于、大于、小于、大于等于、小于等于）。
* 每个`SingleColumnCondition`对象支持一列（可以是主键列）和一个常量比较。不支持两列相比较，也不支持两个常量相比较。
* `SingleColumnCondition`的`latestVersionOnly`参数控制多个版本的列值如何参与比较，默认为`true`。
  - 若为`true`，则只有版本范围内的最新版本列值参与比较（仅仅是参与比较，如果是过滤器认可该行，其他版本的列值依旧会返回）。
  - 若为`false`，则任意一个列值满足条件，该节点即认为`true`。
* `SingleColumnCondition`的`passIfMissing`参数控制列值缺失时该节点应当视作何值，默认`false`。

### 单行覆写

```c++
PutRowRequest req;
{
    RowPutChange& chg = req.mutableRowChange();
    chg.mutableTable() = "YourTable";
    {
        // set primary key of the row to put
        PrimaryKey& pkey = chg.mutablePrimaryKey();
        pkey.append() = PrimaryKeyColumn(
            "pkey",
            PrimaryKeyValue::toStr("pkey-value"));
    }
    {
        // set attributes of the row to put
        IVector<Attribute>& attrs = chg.mutableAttributes();
        attrs.append() = Attribute(
            "attr",
            AttributeValue::toInteger(123));
    }
}
PutRowResponse resp;
Optional<OTSError> res = client.putRow(resp, req);
```

指定表名以及将写入的行的主键以及属性列，若该行不存在则插入，若该行存在则覆盖（即原行的所有列以及所有版本的列值都删除）。

写操作的更多参数，请参考[写之进阶](#写之进阶)。

### 单行删除

```c++
DeleteRowRequest req;
{
    RowDeleteChange& chg = req.mutableRowChange();
    chg.mutableTable() = "YourTable";
    {
        // set primary key of the row to delete
        PrimaryKey& pkey = chg.mutablePrimaryKey();
        pkey.append() = PrimaryKeyColumn(
            "pkey",
            PrimaryKeyValue::toInteger(1));
    }
}
DeleteRowResponse resp;
Optional<OTSError> res = client.deleteRow(resp, req);
```

指定表名以及将删除的行的主键，无论该行存在与否都不会报错，仅设置`DeleteRowResponse`对象。

写操作的更多参数，请参考[写之进阶](#写之进阶)。

### 单行更新

```c++
UpdateRowRequest req;
{
    RowUpdateChange& chg = req.mutableRowChange();
    chg.mutableTable() = "YourTable";
    {
        // set primary key of the row to put
        PrimaryKey& pkey = chg.mutablePrimaryKey();
        pkey.append() = PrimaryKeyColumn(
            "pkey",
            PrimaryKeyValue::toStr("pkey"));
    }
    {
        // insert a value without specifying version
        RowUpdateChange::Update& up = chg.mutableUpdates().append();
        up.mutableType() = RowUpdateChange::Update::kPut;
        up.mutableAttrName() = "attr0";
        up.mutableAttrValue().reset(AttributeValue::toStr("new value without specifying version"));
    }
    {
        // insert a value with version
        RowUpdateChange::Update& up = chg.mutableUpdates().append();
        up.mutableType() = RowUpdateChange::Update::kPut;
        up.mutableAttrName() = "attr1";
        up.mutableAttrValue().reset(AttributeValue::toStr("new value with version"));
        up.mutableTimestamp().reset(UtcTime::now());
    }
    {
        // delete a value with specific version
        RowUpdateChange::Update& up = chg.mutableUpdates().append();
        up.mutableType() = RowUpdateChange::Update::kDelete;
        up.mutableAttrName() = "attr2";
        up.mutableTimestamp().reset(UtcTime::now());
    }
    {
        // delete all values of a attribute column
        RowUpdateChange::Update& up = chg.mutableUpdates().append();
        up.mutableType() = RowUpdateChange::Update::kDeleteAll;
        up.mutableAttrName() = "attr3";
    }
}
UpdateRowResponse resp;
Optional<OTSError> res = client.updateRow(resp, req);
```

单行更新操作可以更新一行内的属性列。
计有四种情况，示例中一一作了演示。
* 不指定版本写入一个列值，表格存储服务端会自动补上一个版本号，保证此种情况下版本号的递增。
* 指定版本写入一个列值，若该列本无该版本列值，则插入，否则覆盖原值。
* 删除指定版本的列值。
* 删除整个列的所有版本列值。

如果该行本不存在，那么单行更新操作会插入该行。
如果该行存在，那么仅单行更新操作指定的列及其列值受到影响。

写操作的更多参数，请参考[写之进阶](#写之进阶)。

### 批量写

```c++
static const char kPutRow[] = "PutRow";
static const char kUpdateRow[] = "UpdateRow";
static const char kDeleteRow[] = "DeleteRow";

BatchWriteRowRequest req;
{
    // put row
    BatchWriteRowRequest::Put& put = req.mutablePuts().append();
    put.mutableUserData() = kPutRow;
    put.mutableGet().mutableTable() = kTableName;
    PrimaryKey& pkey = put.mutableGet().mutablePrimaryKey();
    pkey.append() = PrimaryKeyColumn(
        "pkey",
        PrimaryKeyValue::toStr("row to put"));
}
{
    // update row
    BatchWriteRowRequest::Update& update = req.mutableUpdates().append();
    update.mutableUserData() = kUpdateRow;
    update.mutableGet().mutableTable() = kTableName;
    PrimaryKey& pkey = update.mutableGet().mutablePrimaryKey();
    pkey.append() = PrimaryKeyColumn(
        "pkey",
        PrimaryKeyValue::toStr("row to update"));
    RowUpdateChange::Update& attr = update.mutableGet().mutableUpdates().append();
    attr.mutableType() = RowUpdateChange::Update::kPut;
    attr.mutableAttrName() = "attr0";
    attr.mutableAttrValue().reset(AttributeValue::toStr("some value"));
}
{
    // delete row
    BatchWriteRowRequest::Delete& del = req.mutableDeletes().append();
    del.mutableUserData() = kDeleteRow;
    del.mutableGet().mutableTable() = kTableName;
    PrimaryKey& pkey = del.mutableGet().mutablePrimaryKey();
    pkey.append() = PrimaryKeyColumn(
        "pkey",
        PrimaryKeyValue::toStr("row to delete"));
}
BatchWriteRowResponse resp;
Optional<OTSError> res = client.batchWriteRow(resp, req);
```

批量写可以用来聚集一批写操作，一次性发往服务端，避免多次网络往返带来的性能损失。
用户可以给每一行关联一个数据结构。
这些关联数据结构并不会发往服务端，但是`BatchWriteRowResponse`中会关联返回结果和相应的数据结构，方便用户将聚集的写操作的结果交付给不同的来源。

与批量单行读操作类似，批量写有两类错误。
一类是请求整体的错误，比如网络超时。
这类错误会在`batchWriteRow()`返回值上给出。
另一类是单行上的错误，比如主键值不合法。
这类错误不会在`batchWriteRow()`返回值上给出，而是在`BatchWriteRowResponse`中的每一行上给出。

写操作的更多参数，请参考[写之进阶](#写之进阶)。

### 写之进阶

#### 返回值包含主键

`RowChange`(`RowPutChange`, `RowUpdateChange`和`RowDeleteChange`的基类)对象可以设置返回值是否带上主键(`mutableReturnType()`)。
默认是不带主键(`RowChange::kRT_None`)。
设置为`RowChange::kRT_PrimaryKey`则会带上主键。

#### 条件写

条件写是指在写一行之前先检查条件，当条件成立才实际写入。
表格存储保证这里的条件检查和写入是一个原子操作。
目前支持行存在性条件和列值条件两种。
* 行存在性条件分为忽略`Condition::kIgnore`（默认值，无论行是否存在都写入）、行存在`Condition::kExpectExist`（行存在则写入）和`Condition::kExpectNotExist`（行不存在则写入）。
  注：删除行配合`Condition::kExpectExist`可以知道是否实际删除了一行，代价是性能略差。
* 列值条件等同于[过滤器](#过滤器)。

### 自增主键列

本节介绍C++ SDK如何使用主键列自增功能。
关于这个功能本身，请参考[我们官网文档](https://help.aliyun.com/document_detail/47745.html)。

#### 建表

```c++
CreateTableRequest req;
{
    // immutable configurations of the table
    TableMeta& meta = req.mutableMeta();
    meta.mutableTableName() = kTableName;
    Schema& schema = meta.mutableSchema();
    {
        PrimaryKeyColumnSchema& pkColSchema = schema.append();
        pkColSchema.mutableName() = "ShardKey";
        pkColSchema.mutableType() = kPKT_String;
    }
    {
        PrimaryKeyColumnSchema& pkColSchema = schema.append();
        pkColSchema.mutableName() = "AutoIncrKey";
        pkColSchema.mutableType() = kPKT_Integer;
        pkColSchema.mutableOption().reset(PrimaryKeyColumnSchema::AutoIncrement);
    }
}
CreateTableResponse resp;
Optional<OTSError> res = client.createTable(resp, req);
```

自增主键列必须是整型，并且需要设置`PrimaryKeyColumnSchema::AutoIncrement`。

#### 写入

以单行覆写举例言之。
其他写入接口类似。

```c++
PutRowRequest req;
{
    RowPutChange& chg = req.mutableRowChange();
    chg.mutableTable() = kTableName;
    chg.mutableReturnType() = RowChange::kRT_PrimaryKey;
    PrimaryKey& pkey = chg.mutablePrimaryKey();
    pkey.append() = PrimaryKeyColumn(
        "ShardKey",
        PrimaryKeyValue::toStr("shard0"));
    pkey.append() = PrimaryKeyColumn(
        "AutoIncrKey",
        PrimaryKeyValue::toAutoIncrement());
}
```

自增主键列的值是由表格存储的服务端填入的。
那么就有两个问题：
* 写入的时候该主键列填什么值？
  这时必须填入一个特殊的占位符，可以由`PrimaryKeyValue::toAutoIncrement()`取得该占位符对象。
* 如果后续需要读取这行，该用什么主键？
  可以设置返回值包含主键。
  这样服务端会将主键中的自增主键列的占位符替换成实际的列值，然后返回回来。
  用户可以记录下该主键用于后续的使用。

## 异步客户端使用说明
### 构造

**我们强烈建议一个进程内复用同一个客户端**

SDK构造客户端有两种方法
* 直接构造

  ```c++
  Endpoint ep("YourEndpoint", "YourInstance");
  Credential cr("AccessKeyId", "AccessKeySecret");
  ClientOptions opts;
  AsyncClient* client = NULL;
  Optional<OTSError> res = AsyncClient::create(client, ep, cr, opts);
  ```
  准备好`Endpoint`, `Credential`, `ClientOptions`和一根空的`AsyncClient`指针，调用`AsyncClient::create()`。
  需要注意的是，`AsyncClient::create()`会检查传入的参数是否合理，因而有可能会返回`OTSError`。
  用户必须检查是否有错误。

  直接构造之后，客户端会在后台与表格存储服务端建立并维护连接。
  于是，一，用刚建好的客户端发送请求有可能会碰到`OTSNoAvailableConnection`；二，客户端的并发数会有一个上升的过程。

* 从`SyncClient`构造

  ```c++
  SyncClient& sync = ...;
  AsyncClient* async = AsyncClient::create(sync);
  ```
  此时同步、异步两个客户端共享同一个底层结构（但是各自持有各自的引用计数）。

### 调用与回调

下面我们以列举表操作为例来说明异步接口的使用。

```c++
void listTableCallback(
    ListTableRequest&,
    Optional<OTSError>& err,
    ListTableResponse& resp)
{
    if (err.present()) {
        // 处理错误
    } else {
        const IVector<string>& xs = resp.tables();
        for(int64_t i = 0; i < xs.size(); ++i) {
            cout << xs[i] << endl;
        }
    }
}

void listTable(AsyncClient& client)
{
    ListTableRequest req;
    client.listTable(req, listTableCallback);
}
```

* 首先，与同步接口类似，我们需要准备请求对象。

  值得指出的是，`listTable`的函数签名是

  ```c++
  void listTable(
      ListTableRequest&,
      const std::tr1::function<void(
          ListTableRequest&, util::Optional<OTSError>&, ListTableResponse&)>&);
  ```

  其中第一个参数是可变引用，这与同步接口不同（同步接口是不可变引用）。
  在`listTable()`返回之后（这时整个列举表操作并没有完成），传入的`ListTableRequest`对象一不小心会被改变或者析构，这样就会引入一些难以调查的微妙错误。
  为了避免这类问题，异步客户端会将传入的请求对象里的内容转移到内部保存起来。
  但是，这里是转移，并不是复制，因为复制往往很费时。
  所以，调用了`listTable()`之后，传入的请求对象有可能被改变。

* 其次，我们需要准备一个回调函数。

  回调函数不需要（也不可以）返回任何值，接收三个参数
  - 请求对象。
    其内容即用户调用`listTable()`时传入的请求对象。
    因为回调之后异步客户端也不再需要请求对象，于是以可变引用的方式还给用户的回调函数。
    这样用户可以将请求对象的内容转移出来。
  - 包装在`Optional`内的错误对象。
    如果没有错误，则该对象的`present()`方法返回`false`。
  - 响应对象。
    与请求对象类似，响应对象也是以可变引用的方式交给回调函数。
    如果有错误，响应对象一定是一个合法的对象（可以正常析构），但是其内容是未定义的。

* 异步客户端保证每个请求的回调一定会被调用不多不少正好一次。
* 理论上，回调函数有可能在`listTable()`返回之前被调用。

## 杂项
### 版本号说明

客户端版本号由三段组成，分别是主、次、末版本号。
* 主版本号。
  标识和服务端的通讯协议兼容性。
  如果和服务端的通讯协议有不兼容的改动，则必须提升主版本号。
* 次版本号。
  标识API兼容性。
  如果暴露给用户的接口有不兼容的改动，则必须提升次版本号。
* 末位版本号。
  标识代码。
  如果对代码有任何改动，则必须提升末位版本号。

我们 **不保证二进制兼容性**。
所以也就没有必要在版本号中体现。

### 编译参数

在编译客户端代码的时候，有些编译器的行为是必须保证的，即，某些编译器参数是必须的。
下面我们针对gcc编译器给出这些编译参数以及其解释。

以下这些是必须的：
* `--std=gnu++03`

  支持的语言版本是C++98 TR1，带gcc扩展。
  这里的gcc扩展是指`typeof`。

* `-pthread`

  支持多线程编程所必须。
  这个参数无论编译还是链接都需要加上。

* `-fwrapv`

  整型数据溢出则回转，即，无符号整型向上溢出则成为0，有符号整型向上溢出则成为最小的负数。
  客户端基于这个行为做了一些溢出检查。

以下这些我们建议带上：

* `-O2`

  优化级别。
  我们一般不建议更高的优化级别。

* `-fsanitize=address`和`-fvar-tracking-assignments`

  gcc-4.9之后支持libasan，可以快速而轻量地检测各种内存使用上的错误。
  我们建议在需要我们开发人员调查错误之前一定要带上这两个编译参数复现错误。
  毕竟C++是很容易写坏内存的，而我们的客户端有可能是受害者，而非加害者。
  若干带着这两个参数编译，那么在链接的时候也需要带上前一个参数。
  * 注：libasan和valgrind不兼容。

### 线程使用

客户端使用线程池来服务用户的请求和响应。
这个线程池的大小可以[定制](#ClientOptions)。
除此以外，客户端还另外使用2个线程
* 1个线程用于事件循环
* 1个线程用于调度请求和连接

在linux平台上，因为没有异步的域名解析接口，所以会有额外的1个线程用于域名解析。
这个线程一般在客户端刚构建的时候存在，一旦域名解析完成，这个线程会自行退出。

使用默认日志器的话，有额外的一个线程用于记录日志。

### 错误处理

可能发生错误的接口都会返回`Optional<OTSError>`对象。
* `Optional<T>`是定义在`tablestore/util/optional.hpp`的一个模板类。
  可以把它看作一个只能存放至多一个`T`对象的箱子。
  箱子里要么有一个`T`对象（这时`Optional<T>::present()`返回`true`），可以取出这个`T`对象来用；
  要么箱子里没有`T`对象（这时`Optional<T>::present()`返回`false`）。
  我们借用这个能力来表示错误是否发生。
  如果`Optional<OTSError>::present()`为`true`，则有错误发生。
* `OTSError`对象表示一个具体的错误。
  它有5个字段：
  - `httpStatus`和`errorCode`，HTTP返回码和错误码。
    除了表格存储[官网](https://help.aliyun.com/document_detail/27300.html)上定义的之外，客户端另外定义了若干仅有客户端会发生的错误。

    | HTTP返回码 | 错误码 | 语义 |
    | --- | --- | --- |
    | 6 | OTSCouldntResolveHost | 无法解析域名。通常因为实例访问地址有错，或者网络不通。 |
    | 7 | OTSCouldntConnect | 无法连接服务端。通常因为本地host文件配置错误。 |
    | 28 | OTSRequestTimeout | 请求超时。 |
    | 35 | OTSSslHandshakeFail | HTTPS握手失败。通常因为没有安装本地的证书。 |
    | 55 | OTSWriteRequestFail | 网络发送失败。通常因为网络中断。 |
    | 56 | OTSCorruptedResponse | 响应不完整。 |
    | 89 | OTSNoAvailableConnection | 没有可用的连接。通常发生在客户端刚刚构造时（此时正在网络连接正在逐步建立过程中），或者因为并发的请求数超过了网络连接数。 |
  - `message`，错误的更详细的说明。
  - `requestId`和`traceId`，详见[request ID](#request-id)和[trace ID](#trace-id)。

### 日志

客户端默认会提供一个日志记录器。
然而用户的应用往往有自己的日志记录器。
为了便于管理，应用可以用自己的日志记录器替换。

与之相关的概念有四个，都定义在`tablestore/util/logger.hpp`。
* `Logger`接口

  `Logger`负责将日志的内容组装成`Record`对象，并转交给`Sinker`去写出。
  同时客户端将`Logger`组织成树形的结构，其根是用户在`ClientOptions`中定义的日志记录器，负责请求的逻辑和负责网络的逻辑使用从这个根日志记录器派生出的不同的子日志记录器。

  ```c++
  class Logger
  {
  public:
      enum LogLevel
      {
          kDebug,
          kInfo,
          kError,
      };

      virtual ~Logger() {}
      virtual LogLevel level() const =0;
      virtual void record(LogLevel, const std::string&) =0;
      virtual Logger* spawn(const std::string& key) =0;
      virtual Logger* spawn(const std::string& key, LogLevel) =0;
    };
  ```

  - `level()`返回`Logger`接受的日志等级，低于该等级的日志直接丢弃，不会被传递给`Sinker`。
  - `record()`接受一条日志及其等级，组长成`Record`对象后交给`Logger`对应的`Sinker`。
  - `spawn()`派生一个子日志记录器。

* `Record`接口

  `Record`对象供`Logger`向`Sinker`传递日志内容用。

  `Record`接口本身没有任何方法。
  具体的`Record`类提供怎样的方法由`Logger`与`Sinker`约定。

* `Sinker`接口

  `Sinker`负责将`Record`对象写出。

  ```c++
  class Sinker
  {
  public:
      virtual ~Sinker() {}
      virtual void sink(Record*) =0;
      virtual void flush() =0;
  };
  ```

  - `sink()`，写出一条`Record`。可以只是写到某个缓存中。
  - `flush()`，刷缓存，确保每条日志都落地。

* `SinkerCenter`是一个单例对象。

  `SinkerCenter`持有所有的`Sinker`对象，并将它们和一些键关联起来。

  ```c++
  class SinkerCenter
  {
  public:
      virtual ~SinkerCenter() {}
      static std::tr1::shared_ptr<SinkerCenter> singleton();

      virtual Sinker* registerSinker(const std::string& key, Sinker*) =0;
      virtual void flushAll() =0;
  };
  ```

  - `singleton()`，获取`SinkerCenter`单例对象。
  - `registerSinker()`，在`SinkerCenter`中注册一个`Sinker`。
  - `flushAll()`，将`SinkerCenter`中的所有`Sinker`都刷一遍。

### 重试策略

客户端內建了三种重试策略，分别是：
* 截止期重试策略

  在用户指定的截止期之前，不断采用随机指数退避的方式进行重试。

  默认的重试策略即为本策略（截止期10s）。

* 计数重试策略

  按用户指定的间隔重试，最多重试用户指定的次数。

* 不重试

通常使用內建的重试策略已经足够了。
不过总有一些情况，用户希望自定义重试策略。
于是，用户需要实现`RetryStrategy`接口。

```c++
class RetryStrategy
{
public:
    virtual ~RetryStrategy() {}

    virtual RetryStrategy* clone() const =0;
    virtual int64_t retries() const throw() =0;
    virtual bool shouldRetry(Action, const OTSError&) const =0;
    virtual util::Duration nextPause() =0;
};
```

* `clone()`，复制一个新的对象。必须和当前对象相同类型，并且重试次数等内部状态也完全一样。
* `retries()`，已重试次数。
* `shouldRetry()`，给定操作和错误，判断是否应该重试。

  为了方便用户，我们提供了两个工具函数
  ```c++
  enum RetryCategory
  {
      UNRETRIABLE,
      RETRIABLE,
      DEPENDS,
  };

  static RetryCategory retriable(const OTSError&);
  static bool retriable(Action, const OTSError&);
  ```

  - 第一个`retriable`将错误分成三类。
    一类是重试绝对无害的，比如`OTSTableNotReady`。
    一类是重试有害，或无意义的，比如各种参数错误。
    最后一类是仅凭错误无法判断的，比如`OTSRequestTimeout`。
  - 第二个`retriable`根据操作的幂等原则，结合操作和错误判断可否重试。
    也就是说，`RETRIABLE`类的错误，和读操作的`DEPENDS`类错误，都判为可重试。

* `nextPause()`，如果可以重试，那么距离下次重试的间隔时间。

### request ID与trace ID

每个发送到服务端的请求都会由服务端分配一个编号。
表格存储的服务支持人员会要求用户告知这个编号。
如果响应正常返回，响应对象里会有`requestId`。
如果服务端判断请求出错，那么错误对象中会带有`requestId`。
如果在请求发送之前，或者网络链路上出错，那么错误对象里不会有`requestId`。

每个API调用都会由客户端分配`traceId`。
不同的API调用分配的`traceId`不同。
同一个API调用涉及多次重试的，`traceId`相同，但是`requestId`可能不同。

未来`traceId`还会发送给服务端，以方便将同一次API调用发生的事件都串起来。
所以也请将`traceId`一并告知表格存储的服务支持人员。
