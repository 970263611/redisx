

## Redis流复制工具Redisx

[![996.icu](https://img.shields.io/badge/link-996.icu-red.svg)](https://996.icu)

### 作者

大花团队（详见github提交者），redis相关材料请关注[material项目](https://github.com/970263611/redisx-material)

### 名词解释

From：数据来源Redis节点的统称。

To：数据存入Redis节点的同城。

Redisx:  流复制工具的名称

### 启动环境

环境需求为Jdk1.8+

### 启动指令

配置文件在jar内部

​	java -jar redisx.jar

配置文件在jar外，优先级高于jar内部配置文件

​	java -jar redisx.jar /home/redisx/redisx.yml (后跟配置文件全路径)

启动时命令行或环境变量传参,优先级最高

​	java  -Dredisx.from.password=xxxx -Dredisx.to.password=xxxx  -jar redisx.jar 

### 建设思路

![](images/redis-x.png)

#### 组件对比

| 组件           | redisx         | redis shake     |
| -------------- |----------------|----------------------------|
| 支持版本       | 2.8及以上         | 2.8及以上                     |
| 高可用         | 集群部署，纵向扩展      | 单机部署                       |
| 初始化同步方式 | 全量同步rdb，增量同步   | 全量同步rdb和aof，增量同步           |
| 支持续传       | 支持             | 不支持                        |
| 数据类型       | 五种基本类型 + stream | 五种基本类型 + stream + 3种module |
| 其他功能       | 数据查询，双端集群节点宕机重连 | 数据筛选                       |

### Redisx优势

#### 部署简洁

上手简单，通过简单的配置信息即可快速完成部署，没有复杂的操作流程，降低学习成本;

服务轻量，无需其它中间件辅助，可单独完成功能，降低组件的使用成本。

配置示例

```yaml
redisx:
  from:
    redis:
      version: 6.0.9  #redis版本
    password: 1a.2b*  #Redis密码
    mode: cluster     #Redis模式，单机：single 哨兵:sentinel 集群：cluster
    address:          #from数据来源地址，如模式是集群或哨兵，配置单一节点即可
      - 127.0.0.1:6379
  to:
    password: 1a.2b*  #redis密码
    mode: cluster     #Redis模式，单机：single 哨兵:sentinel 集群：cluster
    address:          #to数据来源地址，如模式是集群或哨兵，配置单一节点即可
      - 127.0.0.1:6380
```

#### 支持高可用

Redisx可进行多节点部署，指向相同Redis To节点的Redisx服务会自动形成主备模式，当Redisx主节点发生异常时，备节点会自动完成主备切换，继续进行数据同步工作，并支持断点续传，保证数据的完整性和连续性。

#### 垂直扩展

Redisx支持对同一集群的不同节点进行单独的同步工作，可以通过配置使Redisx服务于from集群的单一节点，分担数据同步产生的压力，大幅度提高同步效率。

#### 自动修复

Redis节点的状态不会影响到Redisx服务的运行。当Redis节点出现服务down机或者主节点漂移等异常现象时，Redisx可以自动判断选择正常的节点，并开始或中止数据同步工作，而无需关心Redisx节点的状态会对Redisx造成影响。

同时通过简单的配置，可以改变Redisx同步工作启停的条件，例如要求Redis进群节点必须完整才进行同步，从而保证数据同步的完整性；或是仅同步Redis正常节点的数据，从而使数据同步不会停滞。

#### 服务监控

通过Redisx页面监控功能可以实时展示Redis以及Redisx节点的工作状态，Redisx的数据同步速度，数据堆积以及Redisx的配置信息。同时现正在逐渐完善Redisx的告警功能，从而使Redisx运行更加稳定。

#### 定时退出功能

支持设置一定时间后同步服务自动退出，满足一次性同步的场景。

#### 其它扩展

通过配置可以开启Redis的双向的数据查询功能，通过页面可以查询From和To服务中的实时数据。

### 性能测试

CPU为13600KF、内存为DDR5 64G（32G双通道）的电脑上搭建3主3从两套redis集群，发压工具（30并发）和Redisx同时运行，在redisx没有特殊指定启动内存大小、没有-server启动、jdk为1.8的形况下，测试结果如下：

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
      #(必填项)from端redis版本，建议该版本不高于to端版本，防止因redis指令不兼容导致的同步问题
      version: 6.0.9
      #from端redis密码。哨兵模式下数据节点和哨兵节点密码应保持一致
      password: 1a.2b*
      #(必填项)from端redis模式，单机：single 集群：cluster 哨兵:sentinel
      mode: cluster
      #(redis.from.mode为sentinel时必填)哨兵模式下主节点的mastername
      masterName: myMaster
      #(必填项)from端redis节点地址，可配置单个或多个节点地址
      address:
        - 127.0.0.1:16001
      #纵向扩展，为true时，
      verticalScaling: false
      #是否强制连接主节点
      connectMaster: true
  to:
    #to端redis密码
    password: 2b*1a.
    #(必填项)to端redis模式，单机：single 集群：cluster 哨兵:sentinel
    mode: cluster
    #(redis.to.mode为sentinel时必填)哨兵模式下主节点的mastername
    masterName: myMaster
    #(必填项)to端redis节点地址，可配置单个或多个节点地址
    address:
      - 127.0.0.2:16101
    #是否在启动时清空to端数据
    flushDb: false
    #to端单次写入数据阈值
    flushSize: 50
  console:
    #是否启用控制台
    enable: true
    #是否开启控制台双向查询数据功能
    search: false
    #控制台响应超时时间
    timeout: 5000
    #控制台发布端口
    port: 15967
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
  #redisx主从切换标志
  switchFlag: REDISX-AUTHOR:DAHUA&CHANGDONGLIANG&ZHANGHUIHAO&ZHANGSHUHAN&ZHANGYING&CHENYU&MAMING
  #是否同步存量数据
  syncRdb: true
  #定时退出
  timedExit:
    #是否开启定时退出
    enable: false
    #是否执行关闭钩子函数
    force: false
    #定时时长，单位：秒，小于0则定时退出功能失效
    duration: -1
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

### 场景介绍

#### 常规模式

适用场景

高效数据同步场景

配置信息

```yaml
redisx:
  from:
    redis:
      version: x.x.x
    mode: cluster
    password: 1a.2b*
    masterName:
    address:
      - xxx.xxx.xxx.xxx:port
      ...
  to:
    mode: cluster
    password: 1a.2b*
    masterName:
    address:
      - xxx.xxx.xxx.xxx:port
      ...
```

功能描述

1、高效同步，批量提交，单个To节点‘每50条数据或间隔最大100毫秒’进行一次数据提交和偏移量刷新

2、From端优先选从节点连接，无从选主

3、Redis服务异常时中断同步，Redis服务正常时自动开始同步

4、支持存量RDB数据同步，支持中断续传

数据差异(仅异常情况下才可能产生)

1、From节点故障或Redisx正常关闭(kill)，无数据差异

2、单次To节点故障，可能出现不大于 ‘To数量 * flushDb’数据差异

3、Redisx通过强制中断(kill -9)，可能出现不大于 ‘To数量 * flushDb’数据差异

#### 强一致模式

适用场景

数据量较小，但对一致性要求较高

配置信息

常规模式配置中，加入如下配置

```yaml
 redisx:
   immediate:
     enable: true
     resendTimes: 1
```

功能描述

1、效率较低，单条提交，单个To节点‘每1条数据’进行一次数据提交及I/O操作结果确认，偏移量刷新及结果确认

2、From端优先选从节点连接，无从选主

3、Redis服务异常时中断同步，Redis服务正常时自动开始同步

4、支持存量RDB数据同步，支持中断续传

数据差异(仅异常情况下才可能产生)

1、From节点故障或Redisx正常关闭(kill)，无数据差异

2、单次To节点故障，可能出现不大于 ‘To数量’数据差异

3、Redisx通过强制中断(kill -9)，可能出现不大于 ‘To数量’数据差异

#### 垂直拆分模式

适用场景

集群模式，数据量极大，对同步效率要求极高

配置信息

Redisx1：

```yaml
redisx:
  from:
    redis:
      version: x.x.x
    mode: cluster
    password: 1a.2b*
    masterName:
    address:
      - From节点1
      ...
    verticalScaling: true
  to:
    mode: cluster
    password: 1a.2b*
    masterName:
    address:
      - xxx.xxx.xxx.xxx:port
      ...
  switchFlag: REDISX-...每组Redisx服务必须不一致，同组主备Redisx服务须一致
```

Redisx2:

```yaml
redisx:
  from:
    redis:
      version: x.x.x
    mode: cluster
    password: 1a.2b*
    masterName:
    address:
      - From节点2
      ...
    verticalScaling: true
  to:
    mode: cluster
    password: 1a.2b*
    masterName:
    address:
      - xxx.xxx.xxx.xxx:port
      ...
  switchFlag: REDISX-...每组Redisx服务必须不一致，同组主备Redisx服务须一致
```

功能描述

1、仅对配置中的From节点数据进行同步，不扩展至整个From集群数据

2、From端仅选择配置节点

3、Redis服务异常时中断同步，Redis服务正常时自动开始同步

4、支持存量RDB数据同步，支持中断续传

数据差异(仅异常情况下才可能产生)

1、From节点故障或Redisx正常关闭(kill)，无数据差异

2、单次To节点故障，可能出现不大于 ‘To数量’数据差异

3、Redisx通过强制中断(kill -9)，可能出现不大于 ‘To数量’数据差异

#### 定时中断模式

适用场景

一次性同步，短期同步

配置信息

常规模式或强一致模式配置中，加入如下配置

```yaml
redisx:
  timedExit:
    enable: true
    force: true  #是否开启中断补偿，及中断时队列中的堆积数据是否完全同步；默认false
    duration: 1000   #多久关闭，单位秒
```

功能描述

1、同步效率同常规模式或强一致模式，运行设定时长后，自动关闭

2、From端优先选从节点连接，无从选主

3、Redis服务异常时中断同步，Redis服务正常时自动开始同步

4、支持存量RDB数据同步，支持中断续传

数据差异(仅异常情况下才可能产生)

同常规模式或强一致模式

#### 仅主节点模式

适用场景

主节点性能较好，数据实时性要求较高

配置信息

常规模式或强一致模式配置中，加入如下配置(垂直扩展下该配置无效)

```yaml
redisx:
  connectMaster: true
```

功能描述

1、同步效率同常规模式或强一致模式

2、仅连接From主节点

3、Redis服务异常时中断同步，Redis服务正常时自动开始同步

4、支持存量RDB数据同步，支持中断续传

数据差异(仅异常情况下才可能产生)

同常规模式或强一致模式

#### 其它功能

##### 1、配置加密

功能描述

密码加密，支持jasypt密码加解密操作，加密配置放入ENC(...)括号中

配置信息

```yaml
redisx:
  from:
    password: ENC(...)
jasypt:
  encryptor:
    password: U8eT6mld1
    algorithm: PBEWithMD5AndDES
    ivGeneratorClassName: org.jasypt.iv.NoIvGenerator
```

##### 2、To端数据清理

功能描述

全局首次启动时清理To端数据，适用于To端脏数据清理

配置信息

```yaml
redisx:
  to:
    flushDb: true #默认false
```

##### 3、仅全量同步

功能描述

不再中断续传，每次均进行全量同步。数据完整性强，适用于数据量小，要求数据完整的场景

配置信息

```yaml
redisx:
  alwaysFullSync: true #默认false
```

##### 4、仅增量同步

功能描述

不再同步存量RDB数据，仅同步Redisx服务启动后产生的增量数据；若开启仅全量同步，该配置失效。

配置信息

```yaml
redisx:
  syncRdb: false #默认true
```

[![LICENSE](https://img.shields.io/badge/license-Anti%20996-blue.svg)](https://github.com/996icu/996.ICU/blob/master/LICENSE)