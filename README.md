# 系统功能

本项目为基于波动度的自适应采样技术（Fluctuation-based Adaptive Samlping Technique, FAST）的系统实现，主要提供了REST API服务来对已有的时序数据源进行查询与采样。目前支持的功能包含元数据查询、原始数据查询、分批采样查询、数据订阅查询。

## 元数据查询

### 数据库查询
请求格式: host:port/database
满足需求：查询已有的所有数据库/存储组名称

| 参数名称 | 参数说明 | 参数类型 | 是否必须 | 缺省值 |
| ------------ | ------------ | ------------ | ------------ | ------------ |
| url | Jdbc地址 | String | 否 | “127.0.0.1” |
| username | Jdbc用户名 | String | 否 | “root” |
| password | Jdbc密码 | String | 否 | “root” |
| ip | Jdbc网址，如非空将覆盖jdbcurl | String | 否 | - |
| port | Jdbc端口，如非空将覆盖jdbcurl | String | 否 | - |
| dbtype | 数据库类型,支持iotdb， | influxdb与postgresql(timescaledb) | String | 否 | “iotdb” |

返回结果：IoTDB的所有存储组名称，或者Influxdb/TimescaleDB的所有数据库名

### 时间序列查询

满足需求：查询已有的所有时间序列/measurement/column
    请求格式: host:port/timeseries
    
| 参数名称 | 参数说明 | 参数类型 | 是否必须 | 缺省值 |
| ------------ | ------------ | ------------ | ------------ | ------------ |
| url | Jdbc地址 | String | 是 | “127.0.0.1” |
| username | Jdbc用户名 | String | 是 | “root” |
| password | Jdbc密码 | String | 是 | “root” |
| ip | Jdbc网址，如非空将覆盖jdbcurl | String | 否 | - |
| port | Jdbc端口，如非空将覆盖jdbcurl | String | 否 | - |
| database | 数据库名称 | String | 是 | “root” |
| dbtype | 数据库类型,支持iotdb， | influxdb与postgresql(timescaledb) | String | 是 | “iotdb” |

返回结果：给定数据库内的所有时间序列名称


### 数据列查询

满足需求：查询给定时间序列的已有的所有列
    请求格式: host:port/columns

| 参数名称 | 参数说明 | 参数类型 | 是否必须 | 缺省值 |
| ------------ | ------------ | ------------ | ------------ | ------------ |
| url | Jdbc地址 | String | 是 | “127.0.0.1” |
| username | Jdbc用户名 | String | 是 | “root” |
| password | Jdbc密码 | String | 是 | “root” |
| ip | Jdbc网址，如非空将覆盖jdbcurl | String | 否 | - |
| port | Jdbc端口，如非空将覆盖jdbcurl | String | 否 | - | |
| database | 数据库名称 | String | 是 | “root” |
| timeseries | 时间序列名 | String | 是 | “” |
| dbtype | 数据库类型,支持iotdb， | influxdb与postgresql(timescaledb) | String | 是 | “iotdb” |

返回结果：给定时间序列内所有列的名称


## 原始数据查询

满足需求：查询一组可以用于可视化展示的原始时序数据

请求格式: host:port/data

| 参数名称 | 参数说明 | 参数类型 | 是否必须 | 缺省值 |
| ------------ | ------------ | ------------ | ------------ | ------------ |
| url | Jdbc地址 | String | 是 | “127.0.0.1” |
| username | Jdbc用户名 | String | 是 | “root” |
| password | Jdbc密码 | String | 是 | “root” |
| database | 数据库名称 | String | 是 | “root” |
| timeseries | 时间序列名 | String | 是 | “” |
| columns | 数据列名 | String | 是 | “” |
| timeColumn | 时间列名 | String | 否 | “time” |
| startTime | 查询起始时间，格式“yyyy-mm-dd | hh-MM-ss(.SSS)” | String | 否 | - |
| endTime | 查询终止时间，格式“yyyy-mm-dd | hh-MM-ss(.SSS)” | String | 否 | - |
| condition | 查询条件 | String | 否 | - |
| sql | 查询语句，如非空将覆盖上述查询条件 | String | 否 |  |
| format | 结果返回形式，支持map与obj两种形式 | String | 否 | “map” |
| ip | Jdbc网址，如非空将覆盖jdbcurl | String | 否 |  |
| port | Jdbc端口，如非空将覆盖jdbcurl | String | 否 |  |
| dbtype | 数据库类型,支持iotdb， | influxdb,postgresql(timescaledb)与kafka等 | String | 是 | “iotdb” |

返回结果：给定时间序列某一列的数据点集合。如果format为map，将以键值对的方式返回；如果format为obj，将以{time:123,column:a, value:1}的形式返回。


## 分批采样查询

满足需求：查询一组可以用于可视化展示的时间序列数据的采样结果
    请求格式: host:port/sample

 | 参数名称 | 参数说明 | 参数类型 | 是否必须 | 缺省值 |
| ------------ | ------------ | ------------ | ------------ | ------------ |
 | url | Jdbc地址 | String | 是 | “127.0.0.1” |
 | username | Jdbc用户名 | String | 是 | “root” |
 | password | Jdbc密码 | String | 是 | “root” |
 | database | 数据库名称 | String | 是 | “root” |
 | timeseries | 时间序列名 | String | 是 | “” |
 | columns | 数据列名 | String | 是 | “” |
 | timeColumn | 时间列名 | String | 否 | “time” |
 | startTime | 查询起始时间，格式“yyyy-mm-dd | hh-MM-ss(.SSS)” | String | 否 | - |
 | endTime | 查询终止时间，格式“yyyy-mm-dd | hh-MM-ss(.SSS)” | String | 否 | - |
 | condition | 查询条件 | String | 否 | - |
 | sample | 采样算子参数，支持“m4”“aggregation”“random”“outlier” | 算子 | String | 是 | - |
 | timeLimit | 横轴离群值阈值 | Double | 是 | - |
 | valueLimit | 纵轴离群值阈值 | Double | 是 | - |
 | sql | 查询语句，如非空将覆盖上述查询条件 | String | 否 |  |
 | ip | Jdbc网址，如非空将覆盖jdbcurl | String | 否 |  |
 | port | Jdbc端口，如非空将覆盖jdbcurl | String | 否 |  |
 | amount | 采样结果数目 | Integer | 否 | 2000 |
 | format | 结果返回形式，支持map与obj两种形式 | String | 否 | “map” |
 | dbtype | 数据库类型,支持iotdb， | influxdb,postgresql(timescaledb)与kafka等 | String | 是 | “iotdb” |

返回结果：给定时间序列某一列的数据点集合


## 数据订阅查询

### 采样订阅
满足需求：向时序数据采样模块发送一组时序数据样本订阅请求 
    请求格式: host:port/sub

| 参数名称 | 参数说明 | 参数类型 | 是否必须 | 缺省值 |
| ------------ | ------------ | ------------ | ------------ | ------------ |
| url | Jdbc地址 | String | 是 | “127.0.0.1:6667” |
| username | Jdbc用户名 | String | 是 | “root” |
| password | Jdbc密码 | String | 是 | “root” |
| database | 数据库名称 | String | 是 | - |
| timeseries | 时间序列名 | String | 是 | - |
| columns | 数据列名 | String | 是 | “” |
| timeColumn | 时间列名 | String | 否 | “time” |
| startTime | 订阅起始时间，格式“yyyy-mm-dd | hh-MM-ss(.SSS)” | String | 否 | - |
| conditon | 查询条件 | String | 否 | - |
| timeLimit | 横轴阈值 | Double | 否 | - |
| valueLimit | 纵轴阈值 | Double | 否 | - |
| ratio | 采样比 | Integer | 否 | - |
| sample | 采样算子参数，支持“m4”“aggregation”“random”“outlier” | 算子 | String | 是 | - |
| ip | Jdbc网址，如非空将覆盖jdbcurl | String | 否 |  |
| port | Jdbc端口，如非空将覆盖jdbcurl | String | 否 |  |
| format | 数据返回形式 | String | 否 | - |
| dbtype | 数据库类型,支持iotdb， | influxdb,postgresql(timescaledb)与kafka等 | String | 否 | - |

返回结果：时序数据订阅id

### 数目查询
满足需求：查询对于给定数目内的样本数据 
请求格式: host:port/query

| 参数名称 | 参数说明 | 参数类型 | 是否必须 | 缺省值 |
| ------------ | ------------ | ------------ | ------------ | ------------ |
| url | Jdbc地址 | String | 是 | “127.0.0.1:6667” |
| username | Jdbc用户名 | String | 是 | “root” |
| password | Jdbc密码 | String | 是 | “root” |
| database | 数据库 | String | 是 |  |
| timeseries | 时间序列名称 | String | 是 | - |
| columns | 数据列名 | String | 是 | “” |
| timeColumn | 时间列名 | String | 否 | “time” |
| startTime | 查询起始时间，格式“yyyy-mm-dd | hh-MM-ss(.SSS)” | String | 否 | - |
| endTime | 查询终止时间，格式“yyyy-mm-dd | hh-MM-ss(.SSS)” | String | 否 | - |
| amount | 样本数目 | Long | 是 |  |
| format | 结果返回形式，支持map与obj两种形式 | String | 否 | “map” |
| dbtype | 数据库类型,支持iotdb， | influxdb,postgresql(timescaledb)与kafka等 | String | 是 | “iotdb” |

返回结果：给定时间序列某一列的数据点集合

### 误差查询
满足需求：查询对于给定误差内的样本数据
    请求格式: host:port/errorquery

 | 参数名称 | 参数说明 | 参数类型 | 是否必须 | 缺省值 |
| ------------ | ------------ | ------------ | ------------ | ------------ |
 | url | Jdbc地址 | String | 是 | “127.0.0.1:6667” |
 | username | Jdbc用户名 | String | 是 | “root” |
 | password | Jdbc密码 | String | 是 | “root” |
 | database | 数据库 | String | 是 |  |
 | timeseries | 时间序列名称 | String | 是 | - |
 | columns | 数据列名 | String | 是 | “” |
 | timeColumn | 时间列名 | String | 否 | “time” |
 | startTime | 查询起始时间格式“yyyy-mm-dd | hh-MM-ss(.SSS)” | String | 否 | - |
 | endTime | 查询终止时间格式“yyyy-mm-dd | hh-MM-ss(.SSS)” | String | 否 | - |
 | error | 样本误差百分比 | Double | 是 |  |
 | format | 结果返回形式，支持map与obj两种形式 | String | 否 | “map” |
 | dbtype | 数据库类型,支持iotdb， | influxdb,postgresql(timescaledb)与kafka等 | String | 是 | “iotdb” | | 

返回结果：给定时间序列某一列的数据点集合


# 定量测试

## 测试环境

硬件环境：
- CPU Intel（R）Xeon（R）E5-2680v4@2.40GHz
- RAM 16GB

软件环境：
- Ubuntu 18.04
- Java 1.8
- IoTDB 0.9.3
- Postgresql 12
- Influxdb 2.0
- Kafka 2.4.0

## 测试指标

- MS-SSIM
- PAE
- PMSE

## 测试数据集

- 地铁数据
- Intel数据
- 空气质量数据
- 公交车数据
- KPI数据

## 测试结果

- MS-SSIM：相同采样数目，采样误差平均降低32.7%
- PAE：相同采样数目，可读性平均提高11.8%
- PMSE：相同采样数目，采样误差平均降低58.0%

# 性能测试

## 测试环境

硬件环境：
- CPU Intel（R）Xeon（R）E5-2680v4@2.40GHz
- RAM 16GB

软件环境：
- Ubuntu 18.04
- Java 1.8
- IoTDB 0.9.3
- Postgresql 12
- Influxdb 2.0
- Kafka 2.4.0

## 测试指标

- 实时吞吐量
- 采样延迟

## 测试结果

实验测试流程
（1）数据写入：每0.1秒钟写入一批实时数据，每批数据规模分别为[1000，2000，3000，4000，5000]，分别对应每秒钟写入1万点~每秒钟写入5万点。每个数据点时间戳等间隔（即每秒写入100点时间隔为10毫秒，每秒写入1000点时间隔为1毫秒，以此类推）。
（2）订阅采样：针对写入的数据执行订阅采样功能，在日志中输出实时采样吞吐量。
（3）样本查询：查询时间范围为当前时间点前后1秒钟范围内数据（确保包含最新数据）。分别执行分批采样查询与订阅采样查询。查询完成时刻记为t。分批采样查询结果中的最新时间记为t1，订阅采样中的最新时间记为t2。其实际意义为，在当前时刻t得到的查询结果中能够看到的最新样本分别为t1与t2时刻的样本，则t-t1为分批采样的延迟，t-t2为订阅采样的查询延迟。统计十次延迟取平均值。


### IoTDB

| 数据写入速率（点每秒） | 10000 | 20000 | 30000 | 40000 | 50000 |
| ------------ | ------------ | ------------ | ------------ | ------------ | ------------ |
| 吞吐量（点每秒） |10000 | 20000 | 30000 | 40000 | ~45000 |
| 分批采样延迟t-t1（毫秒） | 174 | 601 | 832 | 1293 | - |
| 订阅采样延迟t-t2（毫秒） | 245 | 857 | 1058 | 1159 | - |

 | 数据规模 | 数据控制器 | 权重控制器 | 分桶控制器 | 采样控制器 |
| ------------ | ------------ | ------------ | ------------ | ------------ |
 | 10000 | 197 | 20 | 20 | 4 |
 | 50000 | 500 | 50 | 40 | 8 |
 | 100000 | 750 | 65 | 100 | 10 |
 | 150000 | 1150 | 100 | 160 | 10 |
 | 200000 | 1374 | 129 | 204 | 13 |
 | 250000 | 1732 | 165 | 254 | 19 |
 | 300000 | 1963 | 171 | 307 | 19 |

InfluxDB

| 目标数据写入速率（点每秒） | 10000 | 20000 | 30000 |
| ------------ | ------------ | ------------ | ------------ |
| 吞吐量（点每秒） | 10000 | 20000 | ~25000 |
| 分批采样延迟t-t1（毫秒） | 248 | 435 |  |
| 订阅采样延迟t-t2（毫秒） | 446 | 876 |  |

 |数据规模 | 数据控制器 | 权重控制器 | 分桶控制器 | 采样控制器 |
| ------------ | ------------ | ------------ | ------------ | ------------ |
 |50000 | 457 | 77 | 100 | 14 |
 |100000 | 834 | 88 | 64 | 23 |
 |150000 | 1198 | 124 | 161 | 11 |
 |200000 | 1817 | 104 | 163 | 11 |
 |250000 | 2517 | 183 | 227 | 20 |
 |300000 | 2620 | 208 | 298 | 27 |

TimescaleDB


| 目标数据写入速率（点每秒） | 10000 | 20000 | 30000 | 40000 |
| ------------ | ------------ | ------------ | ------------ | ------------ |
| 吞吐量（点每秒） | 10000 | 15000 | 15000 | 15000 |
| 分批采样延迟t-t1（毫秒） | 176 | 254 | 273 | 336 |
| 订阅采样延迟t-t2（毫秒） | 204 | 364 | 222 | 419 |

| 数据规模 | 数据控制器 | 权重控制器 | 分桶控制器 | 采样控制器 |
| ------------ | ------------ | ------------ | ------------ | ------------ |
| 40000 | 278 | 60 | 90 | 17 |
| 88000 | 550 | 90 | 120 | 13 |
| 150000 | 680 | 120 | 160 | 14 |
| 200000 | 974 | 233 | 238 | 20 |
| 250000 | 1166 | 174 | 270 | 19 |
| 300000 | 1406 | 278 | 338 | 19 |

### Kafka

| 目标数据写入速率（点每秒） | 10000 | 20000 | 30000 | 40000 | 50000 | |
| ------------ | ------------ | ------------ | ------------ | ------------ |
| 吞吐量（点每秒） | 10000 | 20000 | 30000 | 40000 | 50000 |
| 订阅采样延迟t-t2（毫秒） | 355 | - |  | |

| 数据规模 | 数据控制器 | 权重控制器 | 分桶控制器 | 采样控制器 |
| ------------ | ------------ | ------------ | ------------ | ------------ |
| 10000 | 179 | 33 | 21 | 3 |

# 启动服务

首先安装maven环境，然后进入complete目录（pom.xml所在目录）执行打包程序

```shell
    mvn package
```
生成的jar包文件gs-rest-service-0.1.0.jar位于target目录中，可以拷贝到任意目录中启动服务

```shell
java -jar gs-rest-service-0.1.0.jar
```

接下来就可以通过GET请求访问restapi服务，例如

    http://localhost:8080/database?url=localhost:6667



# 测试接口

项目内实现采样延迟的自动测试接口*/latencytest*，具体测试方法为

0. 准备完成数据源（例如IoTDB，Influxdb，TimescaleDB及Kafka），并将数据源信息以JSON格式保存于jar包同级目录中的test.config文件中
1. 发送测试请求，如
http://192.168.10.172:9090/latencytest?database="root.abc"&batch=1000&batchSize=10&dbtype=iotdb
启动写入iotdb数据库root.abc，batch为每秒写入1000次，batchSize为每次写入十条数据，
2. 等待网页返回测试结果。sublantency为订阅采样延迟，samplelatency为分批采样延迟
3. 在日志中分析对应吞吐量，如 cat result.txt | grep throughput
4. 重启服务 ./restart.sh，中止订阅相关线程，释放数据库链接。
5. 删除测试数据，如IoTDB为：delete from root.abc.1701.ZT31 where time <= 2030-01-01 00:00:00;
6. 删除TimescaleDB的中间层数据库，如drop database root_abc;
7. 进行下轮测试

示例test.json:
```json
{
    "dataURL":"jdbc:iotdb://192.168.10.172:6667/",
    "dataUsername":"root",
    "dataPassword":"root",
    "dataDatabase":"root.group_9",
    "dataTimeseries":"1701",
    "dataColumns":"ZT31",
    "dataStartTime":"2019-08-15 00:00:00",
    "dataEndTime":"2019-08-20 00:00:00",
    "dataConditions":" and ZT31 > 0 ",
    "dataDbtype":"iotdb",

    "IoTDBURL":"jdbc:iotdb://192.168.10.172:6667/",
    "IoTDBUsername":"root",
    "IoTDBPassword":"root",

    "InfluxDBURL":"http://192.168.10.172:8086",
    "InfluxDBUsername":"root",
    "InfluxDBPassword":"root",

    "TimescaleDBURL":"jdbc:postgresql://192.168.10.172:5432/",
    "TimescaleDBUsername":"postgres",
    "TimescaleDBPassword":"1111aaaa",

    "KafkaURL":"192.168.10.172:9092",
}
```