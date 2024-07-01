## Redis流复制工具Redis-x

### 作者

大花团队（详见github提交者）

### 名词解释

From：数据来源的Redis集群对应Redis-x中的节点。

To：需要被同步数据的Redis集群对应Redis-x中的节点。 	

### 启动环境

环境需求为Jdk1.8+，启动指令为java -jar redis-x.jar

### 建设思路

![](images/redis-x.png)

### 性能测试

CPU为13600KF、内存为DDR5 64G（32G双通道）的电脑上搭建3主3从两套redis集群，发压工具（30并发）和redis-x同时运行，在redis-x没有特殊指定启动内存大小、没有-server启动、jdk为1.8的形况下，测试结果如下：

![](images/sendtest.png)

![](images/recvtest.png)

### 准确性测试

### 准确性测试

在默认模式下（笔记本中测试，因为反复测试切换场景，所以数据传递落后于发布数量）：

Redis：2.8.0（双端单机，此版本Redis不支持集群。Redis-x组件2节点）

![](images/2.8.0.png)

Redis：3.0.3（双端集群，3主3从。Redis-x组件2节点）

![](images/3.0.0.png)

Redis：4.0.11（双端集群，3主3从。Redis-x组件2节点）

![](images/4.0.11.png)

Redis：5.0.0（双端集群，3主3从。Redis-x组件2节点）

![](images/5.0.0.png)

Redis：6.0.9（双端集群，3主3从。Redis-x组件2节点）

![](images/6.0.9.png)

Redis：7.2.4（双端集群，3主3从。Redis-x组件2节点）

![](images/7.2.4.png)

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

配置文件中参数：

```yaml
redisx:         
  from:
  	redis:
      version: 6.0.9          #From端redis版本，From和To可以不一致（必要参数）
    password: 1a.2b*          #From端redis密码
    isCluster: true           #From端redis是否为集群
    address:
      - 127.0.0.1:16001       #From端redis集群地址（必要参数）
      - 127.0.0.1:16002
      - 127.0.0.1:16003
  to:
    password: 2b*1a.          #To端redis密码
    isCluster: true           #To端redis是否为集群
    address:
      - 127.0.0.2:16101       #To端redis集群地址（必要参数）
      - 127.0.0.2:16102
      - 127.0.0.2:16103
  console:
    enable: false             #是否启用控制台，控制台主要用于双向查询数据
    port: 9999                #控制台发布端口
    timeout: 5000             #控制台查询时间
  immediate:                  
    enable: false             #是否开启强一致模式
    resendTimes: 3            #强一致模式下写入失败重试次数
  alwaysFullSync: false       #全局是否强制全量同步数据模式
  switchFlag: REDIS-X-AUTHOR:DAHUA&CHANGDONGLIANG&ZHANGHUIHAO&ZHANGSHUHAN      #redis-x主从切换标志，在纵向扩展时需要配置
```

### 高可用

Redis-x组件支持横向和纵向扩展。

#### 横向扩展

Redis-x组件天然支持多节点启动，多节点间会自动选举一个主节点，当主节点宕机10秒后，从节点们会自动选举一个新的主节点。

#### 纵向扩展

纵向扩展场景一般存在于一端为集群模式时，Redis-x组件可自动剔除非配置节点hash槽数据，多节点可通过多组并行模式进行纵向扩展。
