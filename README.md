

## Redis流复制工具Redisx

[![996.icu](https://img.shields.io/badge/link-996.icu-red.svg)](https://996.icu)

### 作者

大花团队（详见github提交者），redis相关材料请关注[material项目](https://github.com/970263611/redisx-material)

### 名词解释

From：数据来源的Redis集群对应Redisx中的节点。

To：需要被同步数据的Redis集群对应Redisx中的节点。



### 启动环境

环境需求为Jdk1.8+，启动指令为java -jar redisx.jar

### 建设思路

![](images/redis-x.png)

#### 组件对比

| 组件           | redisx         | REDIS shake          |
| -------------- |----------------|----------------------------|
| 支持版本       | 2.8及以上         | 2.8及以上                     |
| 高可用         | 集群部署，纵向扩展      | 单机部署                       |
| 初始化同步方式 | 全量同步rdb，增量同步   | 全量同步rdb和aof，增量同步           |
| 支持续传       | 支持             | 不支持                        |
| 数据类型       | 五种基本类型 + stream | 五种基本类型 + stream + 3种module |
| 其他功能       | 数据查询，集群节点宕机重连  | 数据筛选                       |

### REDISX优势

#### 部署简洁

上手简单，通过简单的配置信息即可快速完成部署，没有复杂的操作流程，降低学习成本;

服务轻量，无需其它中间件辅助，可单独完成功能，降低组件的使用成本。

配置示例

```
redisx:
  from:
    redis:
      version: 6.0.9  #redis版本
    password: 1a.2b*  #Redis密码
    mode: cluster     #Redis模式，单机：single 集群：cluster 哨兵:sentinel
    address:          #from数据来源地址，如模式是进群或哨兵，配置单一节点即可
      - 127.0.0.1:6379
  to:
    password: 1a.2b*  #redis密码
    mode: cluster     #Redis模式，单机：single 集群：cluster 哨兵:sentinel
    address:          #to数据来源地址，如模式是进群或哨兵，配置单一节点即可
      - 127.0.0.1:6380
```

#### 支持高可用

REDISX可进行多节点部署，指向相同Redis To节点的REDISX服务会自动形成主备模式，当REDISX主节点发生异常时，备节点会自动完成主备切换，继续进行数据同步工作，并支持断点续传，保证数据的完整性和连续性。

#### 纵向扩展

REDISX支持对同一集群的不同节点进行单独的同步工作，可以通过配置使REDISX服务于from集群的单一节点，分担数据同步产生的压力，大幅度提高同步效率。

#### 自动修复

Redis节点的状态不会影响到REDISX服务的运行。当Redis节点出现服务down机或者主节点漂移等异常现象时，REDISX可以自动判断选择正常的节点，并开始或中止数据同步工作，而无需关心Redix节点的状态会对REDISX造成影响。

同时通过简单的配置，可以改变REDISX同步工作启停的条件，例如要求Redis进群节点必须完整才进行同步，从而保证数据同步的完整性；或是仅同步Redis正常节点的数据，从而使数据同步不会停滞。

#### 服务监控(完善中)

通过REDISX页面监控功能可以实时展示REDIS以及REDISX节点的工作状态，REDISX的数据同步速度，数据堆积以及REDISX的配置信息。同时现正在逐渐完善REDISX的告警功能，从而使REDISX运行更加稳定。

#### 其它扩展

通过配置可以开启REDIS的双向的数据查询功能，通过页面可以查询From和To服务中的实时数据。

### 性能测试

CPU为13600KF、内存为DDR5 64G（32G双通道）的电脑上搭建3主3从两套redis集群，发压工具（30并发）和redisx同时运行，在redisx没有特殊指定启动内存大小、没有-server启动、jdk为1.8的形况下，测试结果如下：

![](images/redisx5w.jpg)

### 模式支持

#### Reids双端模式：

源：集群 - 入：集群

源：单机/哨兵 - 入：集群

源：集群 - 入：单机/哨兵

源：单机/哨兵 - 入：单机/哨兵

#### Reids-x模式

##### 默认模式

默认采用一秒一次的频率提交偏移量，100条/100毫秒频率提交数据至To端集群，此模式吞吐量较高，和强一致模式互斥。

##### 强一致模式

强一致模式每条数据写入To集群后都会强制同步一次偏移量，此模式稳定性较高，和默认模式互斥。

##### 强制全量同步数据模式

强制全量同步数据模式为每次启动都会强制全量同步主/从所有数据信息，大数据量下初始会存在延迟，但是可以保证数据幂等性，此模式不与其他模式互斥。

### 配置介绍

项目启动如无指定配置文件时会自动扫描resources目录下的redisx.yml，如果需要指定配置文件需要在启动指令后添加配置文件地址。如：

```shell
java -jar redisx.jar redisx.yml
```

配置信息描述：

```yaml
redisx:
  from:
  	redis:
  	  #(必填项)from端redis版本，建议该版本等于或低于to端版本，以防止redis指令不兼容导致的同步问题
      version: 6.0.9
    #from端redis密码，支持enc加密
    #哨兵模式下，redis node节点和哨兵节点密码应保持一致
    password: 1a.2b*
    #(必填项)from端redis模式，单机：single 集群：cluster 哨兵:sentinel
    mode: cluster
    #(redis.from.mode为sentinel时必填)哨兵模式下主节点的mastername
    masterName: myMaster
    #(必填项)from端redis节点地址，
    #集群模式下需配置单个或多个节点地址，建议配置完整的节点地址
    #哨兵模式下需配置单个或多个哨兵地址，建议配置完整的哨兵节点
    address:
      - 127.0.0.1:16001
      - 127.0.0.1:16002
      - 127.0.0.1:16003
    #纵向扩展，为true时，
    verticalScaling: false
    connectMaster: true
  to:
    #to端redis密码，支持enc加密
    password: 2b*1a.
    #(必填项)from端redis模式，单机：single 集群：cluster 哨兵:sentinel
    mode: cluster
    #(redis.to.mode为sentinel时必填)哨兵模式下主节点的mastername
    masterName: myMaster
    #(必填项)from端redis节点地址，
    #集群模式下需配置单个或多个节点地址，建议配置完整的节点地址
    #哨兵模式下需配置单个或多个哨兵地址，建议配置完整的哨兵节点
    address:
      - 127.0.0.2:16101
      - 127.0.0.2:16102
      - 127.0.0.2:16103
    #是否在启动时清空to端数据，默认为false
    flushDb: false
    #to端写入数据阈值，N条数据的write进行一次flush
    flushSize: 20
    #当to端集群不完整，是否需要槽信息连续才同步数据，默认为false（仅当to为集群配置生效）
    syncWithCheckSlot: false
  console:
    #是否启用控制台，控制台主要用于双向查询数据
    enable: false
    #控制台发布端口
    port: 9999
    #控制台查询时间
    timeout: 5000
  #强一致模式，该模式下，单条数据完成io才会进行偏移量更新
  #开启后可以降低服务异常导致的数据不一致问题，但会大幅度降低同步效率
  immediate:
    #是否开启强一致模式
    enable: false
    #强一致模式下写入失败重试次数
    resendTimes: 3
  #全局是否强制全量同步数据模式
  #开启后每次重新开始同步均会进行全量同步，而不进行续传同步
  #开启后，syncRdb配置强制为true
  alwaysFullSync: false
  #redisx主从切换标志,redis-x高可用部署、断点续传会以该标识作为key在to节点中写入实时同步状态信息
  switchFlag: REDISX-AUTHOR:DAHUA&CHANGDONGLIANG&ZHANGHUIHAO&ZHANGSHUHAN
  #是否同步存量数据，为false时之同步增量数据
  syncRdb: true
#配置文件支持enc加密，加密的配置需要使用'ENC(配置内容)'包裹
jasypt:
  encryptor:
    password: KU1aBcAit9x
    algorithm: PBEWithMD5AndDES
    ivGeneratorClassName: org.jasypt.iv.NoIvGenerator
logging:
  level:
    #全局日志级别
    global: info
```

### 高可用

Redisx组件天然支持多节点启动，多节点间会自动选举一个主节点，当主节点宕机10秒后，从节点们会自动选举一个新的主节点。

#### 纵向扩展

纵向扩展场景一般存在于一端为集群模式时，Redisx组件可自动剔除非配置节点hash槽数据，多节点可通过多组并行模式进行纵向扩展。

![](images/highuse.png)

### 控制台使用

**注**：不建议生产启动，不建议端口开放访问

```
http://${ip}:${port}/console?command=${command}&type=from/to   
#command为具体指令,type:为from查询from端redis数据，为to查询to端redis数据
如：
http://localhost:9999/console?command=get testKey&type=from
http://localhost:9999/console?command=get testKey&type=to
```



[![LICENSE](https://img.shields.io/badge/license-Anti%20996-blue.svg)](https://github.com/996icu/996.ICU/blob/master/LICENSE)
