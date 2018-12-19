# Wheels
## 简介
Wheels主要对大数据主流框架及常用算法库进行统一封装，优化及实现，对外提供简洁且高效的API。
## 快速入门

```scala
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

sql <== "emp_res" //保存结果数据到默认存储
```
## 更多内容
+ [安装](doc/install.md)
+ [使用手册](doc/manual.md)
+ [API](doc/api.zip)
+ [配置](doc/conf.md)

## Todo List
- [X] spark-core 集成
- [X] spark-sql 集成
- [X] 实现自动处理小文件的spark dataframe -> hive table存储
- [X] dataframe/view -> redis
- [X] dataframe/view -> hbase
- [X] dataframe/view -> kafka
- [X] super join实现（自动发现并解决含有数据倾斜的join操作）
- [ ] dataframe/view -> es
- [ ] dataframe <-> jdbc
- [ ] 常用特征工程工具实现 
- [ ] ai.h2o 集成
