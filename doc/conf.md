# 配置

[设置方式及基础配置](http://spark.apache.org/docs/latest/configuration.html)

| 配置 | 默认值 | 说明 |
| -------- | -------- | -------- |
| wheel.spark.sql.hive.save.mode | overwrite | 数据入库模式(overwrite:覆盖，append：追加，ignore：若存在则跳过写入，error：若存在则报错) |
| wheel.spark.sql.hive.save.format | parquet | 写入数据格式(parquet,orc,csv,json) |
| wheel.spark.sql.hive.save.file.lines.limit | 1000000 | 写入文件最大行数限制，用于预防小文件产生 |
| wheel.spark.sql.hive.save.refresh.view | false | 数据写入后是否刷新视图 |
| spark.sql.broadcastTimeout | 3000 | 广播变量超时时间(单位：秒) |