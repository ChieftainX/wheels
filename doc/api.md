# API

- ***[wheels-spark](/#wheels-spark)***
  - [创建核心功能对象](/#com.zhjy.wheel.spark.Core.apply)
  - [是否支持sql模块功能](/#com.zhjy.wheel.spark.Core.support_sql)
  - [获取 catalog 对象](/#com.zhjy.wheel.spark.Core.catalog)
  - [释放资源](/#com.zhjy.wheel.spark.Core.stop)
  - [使用sql进行数据处理](/#com.zhjy.wheel.spark.SQL.==>)
  - [视图写入hive](/#com.zhjy.wheel.spark.SQL.<==)
  - [dataframe对象注册到视图](/#com.zhjy.wheel.spark.SQL.register)
  - [读取表，并注册为视图](/#com.zhjy.wheel.spark.SQL.read)
  - [获取视图](/#com.zhjy.wheel.spark.SQL.view)
  - [获取表/视图的行数](/#com.zhjy.wheel.spark.SQL.count)
  - [预览表的数据](/#com.zhjy.wheel.spark.SQL.show)
  - [将视图写入hive](/#com.zhjy.wheel.spark.SQL.save)
  - [缓存dataframe](/#com.zhjy.wheel.spark.SQL.cache-d)
  - [缓存视图](/#com.zhjy.wheel.spark.SQL.cache-v)  
  - [释放dataframe缓存](/#com.zhjy.wheel.spark.SQL.uncache-d)  
  - [释放视图缓存](/#com.zhjy.wheel.spark.SQL.uncache-v)  
  - [释放全部缓存](/#com.zhjy.wheel.spark.SQL.uncache_all)  
  - [分区配置](/#com.zhjy.wheel.spark.SQL.partition)
  - [设置表为初始化](/#com.zhjy.wheel.spark.SQL.partition.table_init)
  - [获取待分区的列的值](/#com.zhjy.wheel.spark.SQL.partition.values)
  - [添加分区的值](/#com.zhjy.wheel.spark.SQL.partition.+)
  - [批量添加分区的值](/#com.zhjy.wheel.spark.SQL.partition.++)
  
## <a name='wheels-spark'>wheels-spark</a>

### <a name='com.zhjy.wheel.spark.Core.apply'>创建核心功能对象</a>
```
com.zhjy.wheel.spark.Core.apply
  /**
    * 创建核心功能对象
    *
    * @param name app名称
    * @param conf runtime 配置信息
    * @param hive_support 是否开启hive支持
    * @param database database名称
    * @param log_less 是否需要少量的日志输出
    * @return 核心功能对象
    */
  def apply(name: String = s"run spark @ ${Time.now}",
            conf: Map[String, Any] = Map(),
            hive_support: Boolean = true,
            database: String = null,
            log_less: Boolean = true
           ): Core
```
### <a name='com.zhjy.wheel.spark.Core.support_sql'>是否支持sql模块功能</a>
```
com.zhjy.wheel.spark.Core.support_sql
  /**
    * 是否支持sql模块功能
    *
    * @return sql对象
    */
  def support_sql: SQL
```
### <a name='com.zhjy.wheel.spark.Core.catalog'>获取 catalog 对象</a>
```
com.zhjy.wheel.spark.Core.catalog
  /**
    * 获取 catalog 对象
    *
    * @return catalog
    */
  def catalog: Catalog
```
### <a name='com.zhjy.wheel.spark.Core.stop'>释放资源</a>
```
com.zhjy.wheel.spark.Core.stop
  /**
    * 释放资源
    */
  def stop(): Unit
```
### <a name='com.zhjy.wheel.spark.SQL.==>'>使用sql进行数据处理</a>
```
com.zhjy.wheel.spark.SQL.==>
   /**
    * 使用sql进行数据处理
    *
    * @param sql 待执行的sql字符串
    * @param view 执行结果的视图名称，若未填入则不注册视图
    * @param cache 是否写入缓存
    * @param level 写入缓存的级别
    * @return dataframe对象
    */
  def ==>(sql: String, view: String = null,
          cache: Boolean = false,
          level: StorageLevel = StorageLevel.MEMORY_AND_DISK): DataFrame
```
### <a name='com.zhjy.wheel.spark.SQL.<=='>视图写入hive</a>
```
com.zhjy.wheel.spark.SQL.<==
/**
    * 视图写入hive
    *
    * @param view 视图名称
    * @param table 待写入hive表名称，默认为视图名称
    * @param p 分区表配置对象，默认为写入非分区表
    * @param save_mode 数据入库模式(overwrite:覆盖，append：追加，ignore：若存在则跳过写入，error：若存在则报错)
    * @param format_source 写入数据格式(parquet,orc,csv,json)
    * @param coalesce_limit 写入文件最大行数限制，用于预防小文件产生
    * @param refresh_view 数据写入后是否刷新视图
    * @return 写入数据的行数
    */
  def <==(view: String, table: String = null,
          p: partition = null,
          save_mode: SaveMode = save_mode,
          format_source: String = format_source,
          coalesce_limit: Long = coalesce_limit,
          refresh_view: Boolean = refresh_view): Long
```
### <a name='com.zhjy.wheel.spark.SQL.register'>dataframe对象注册到视图</a>
```
com.zhjy.wheel.spark.SQL.register
  /**
    * dataframe对象注册到视图
    *
    * @param df 待注册dataframe
    * @param view 视图名称
    * @param cache 是否写入缓存
    * @param level 写入缓存的级别
    * @return dataframe对象
    */
  def register(df: DataFrame, view: String,
               cache: Boolean = false,
               level: StorageLevel = StorageLevel.MEMORY_AND_DISK): DataFrame 
```
### <a name='com.zhjy.wheel.spark.SQL.read'>读取表，并注册为视图</a>
```
com.zhjy.wheel.spark.SQL.read
  /**
    * 读取表，并注册为视图
    *
    * @param table 待读取表的名称
    * @param reality 是否读取真实表
    * @param cache 是否写入缓存
    * @param level 写入缓存的级别
    * @return dataframe对象
    */
  def read(table: String,
           reality: Boolean = true,
           cache: Boolean = false,
           level: StorageLevel = StorageLevel.MEMORY_AND_DISK): DataFrame 

```
### <a name='com.zhjy.wheel.spark.SQL.view'>获取视图</a>
```
com.zhjy.wheel.spark.SQL.view
  /**
    * 获取视图
    *
    * @param view 视图名称
    * @return dataframe对象
    */
  def view(view: String): DataFrame 
```
### <a name='com.zhjy.wheel.spark.SQL.count'>获取表/视图的行数</a>
```
com.zhjy.wheel.spark.SQL.count
  /**
    * 获取表/视图的行数
    *
    * @param table   待读取表的名称
    * @param reality 是否读取真实表
    * @return 表/视图的行数
    */
  def count(table: String, reality: Boolean = false): Long
```
### <a name='com.zhjy.wheel.spark.SQL.show'>预览表的数据</a>
```
com.zhjy.wheel.spark.SQL.show
  /**
    * 预览表的数据
    *
    * @param view     视图名称
    * @param limit    预览的行数
    * @param truncate 是否简化输出结果
    * @param reality  是否读取真实表
    */
  def show(view: String, limit: Int = 20, truncate: Boolean = false,
           reality: Boolean = false): Unit
```
### <a name='com.zhjy.wheel.spark.SQL.save'>将视图写入hive</a>
```
com.zhjy.wheel.spark.SQL.save
  /**
    * 将视图写入hive
    *
    * @param df             待保存dataframe
    * @param table          待写入hive表名称，默认为视图名称
    * @param p              分区表配置对象，默认为写入非分区表
    * @param save_mode      数据入库模式(overwrite:覆盖，append：追加，ignore：若存在则跳过写入，error：若存在则报错)
    * @param format_source  写入数据格式(parquet,orc,csv,json)
    * @param coalesce_limit 写入文件最大行数限制，用于预防小文件产生
    * @param refresh_view   数据写入后是否刷新视图
    * @return 写入数据的行数
    */
  def save(df: DataFrame, table: String,
           p: partition = null,
           save_mode: SaveMode = save_mode,
           format_source: String = format_source,
           coalesce_limit: Long = coalesce_limit,
           refresh_view: Boolean = refresh_view): Long
```
### <a name='com.zhjy.wheel.spark.SQL.cache-d'>缓存dataframe</a>
```
com.zhjy.wheel.spark.SQL.cache
  /**
    * 缓存dataframe
    *
    * @param df  待缓存dataframe
    * @param dfs 支持批量缓存
    */
  def cache(df: DataFrame, dfs: DataFrame*): Unit
```
### <a name='com.zhjy.wheel.spark.SQL.cache-v'>缓存视图</a>
```
com.zhjy.wheel.spark.SQL.cache
  /**
    * 缓存视图
    *
    * @param view 视图名称
    */
  def cache(view: String*): Unit
```
### <a name='com.zhjy.wheel.spark.SQL.uncache-d'>释放dataframe缓存</a>
```
com.zhjy.wheel.spark.SQL.uncache
  /**
    * 释放dataframe缓存
    *
    * @param df dataframe
    * @param dfs 支持批量释放
    */
  def uncache(df: DataFrame, dfs: DataFrame*): Unit
```
### <a name='com.zhjy.wheel.spark.SQL.uncache-v'>释放视图缓存</a>
```
com.zhjy.wheel.spark.SQL.uncache
  /**
    * 释放视图缓存
    *
    * @param view 视图名称
    */
  def uncache(view: String*): Unit
```
### <a name='com.zhjy.wheel.spark.SQL.uncache_all'>释放全部缓存</a>
```
com.zhjy.wheel.spark.SQL.uncache_all
  /**
    * 释放全部缓存
    */
  def uncache_all(): Unit
```
### <a name='com.zhjy.wheel.spark.SQL.partition'>分区配置</a>
```
com.zhjy.wheel.spark.SQL.partition
  /**
    * 分区配置
    *
    * @param col 列名
    */
  case class partition(col: String*)
```
### <a name='com.zhjy.wheel.spark.SQL.partition.table_init'>设置表为初始化</a>
```
com.zhjy.wheel.spark.SQL.partition.table_init
    /**
      * 设置表为初始化
      *
      * @return this
      */
```
### <a name='com.zhjy.wheel.spark.SQL.partition.values'>获取待分区的列的值</a>
```
com.zhjy.wheel.spark.SQL.partition.values
    /**
      * 获取待分区的列的值
      *
      * @return Seq((分区值1，分区值2，分区值3...分区值n))
      */
```
### <a name='com.zhjy.wheel.spark.SQL.partition.+'>添加分区的值</a>
```
com.zhjy.wheel.spark.SQL.partition.+
    /**
      * 添加分区的值
      *
      * @param value (分区值1，分区值2，分区值3...分区值n)
      * @return this
      */
```
### <a name='com.zhjy.wheel.spark.SQL.partition.++'>批量添加分区的值</a>
```
com.zhjy.wheel.spark.SQL.partition.++
    /**
      * 批量添加分区的值
      *
      * @param values Seq((分区值1，分区值2，分区值3...分区值n))
      * @return this
      */
```