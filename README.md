# Wheels
## 简介
Wheels主要对大数据主流框架及常用算法库进行统一封装，优化及实现，对外提供简洁且高效的API。
## 快速入门

```
import com.wheels.spark._

val sql = Core().support_sql //创建sql对象

//使用sql进行数据处理
sql ==> (
  """
    select
    country,count(1) country_count
    from emp
    group by country
  """, "tmp_country_agg")

sql ==> (
  """
    select
    org_id,count(1) org_count
    from emp
    group by org_id
  """, "tmp_org_agg")

sql ==> (
  """
    select
    e.*,c.country_count,o.org_count
    from emp e,tmp_country_agg c,tmp_org_agg o
    where
    e.country = c.country and
    o.org_id = e.org_id and
    e.height > 156
  """, "emp_res")

sql <== "emp_res" //保存结果数据到默认存储(例如hive)

//写入其他存储

val database: DB = sql.support_database

//写hbase
val hbase = database.hbase("127.0.0.1")
hbase <== "your_view"

//写kafka
val kafka = database.kafka(servers = "yourhost0:port0,yourhost1:port1,yourhost2:port2", "your-topic")
kafka <== "your_view"


//读关系型数据库
val mysql = database.jdbc("com.mysql.cj.jdbc.Driver", "jdbc:mysql://localhost/yourdb", "username")
mysql ==> "your_mysql_table"

//写关系型数据库
mysql <== "your_view"

//写es
val es = database.es("your-index/your-type")
es <== "your_view"

//写redis（集群模式）
val redis = database.redis_cluster(
     Seq(("127.0.0.1", 6379), ("127.0.0.1", 6381), ("127.0.0.1", 6382)),//redis集群地址及端口
     life_seconds = 100 * 60//写入的数据保留100分钟
   )
redis <== "your_view"


```
## 更多内容
+ [安装](doc/install.md)
+ [使用手册](doc/manual.md)
+ [API](doc/api.zip)
+ [配置](doc/conf.md)
