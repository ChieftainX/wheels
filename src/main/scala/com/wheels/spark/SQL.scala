package com.wheels.spark

import java.util.Locale

import com.wheels.common.Log
import com.wheels.exception.{IllegalConfException, IllegalParamException, RealityTableNotFoundException}
import com.wheels.spark.database.DB
import com.wheels.spark.ml.ML
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

/**
  * Created by zzy on 2018/10/25.
  */
class SQL(spark: SparkSession) extends Core(spark) {

  import SQL._

  def support_ml: ML = new ML(this)

  def support_database: DB = new DB(this)

  private def save_mode: SaveMode = {
    val mode = spark.conf.get("wheel.spark.sql.hive.save.mode")
    mode.toLowerCase(Locale.ROOT) match {
      case "overwrite" => SaveMode.Overwrite
      case "append" => SaveMode.Append
      case "ignore" => SaveMode.Ignore
      case "error" | "default" => SaveMode.ErrorIfExists
      case _ => throw IllegalConfException(s"unknown save mode: $mode." +
        s"accepted save modes are 'overwrite', 'append', 'ignore', 'error'.")
    }
  }

  private def format_source: String = spark.conf.get("wheel.spark.sql.hive.save.format")

  private def coalesce_limit: Long = spark.conf.get("wheel.spark.sql.hive.save.file.lines.limit").toLong

  private def refresh_view: Boolean = spark.conf.get("wheel.spark.sql.hive.save.refresh.view").toBoolean

  /**
    * 使用sql进行数据处理
    *
    * @param sql   待执行的sql字符串
    * @param view  执行结果的视图名称，若未填入则不注册视图
    * @param cache 是否写入缓存
    * @param level 写入缓存的级别
    * @return dataframe对象
    */
  def ==>(sql: String, view: String = null,
          cache: Boolean = false,
          level: StorageLevel = StorageLevel.MEMORY_AND_DISK): DataFrame = {
    log.info(s"register ${
      if (view eq null) "no view" else s"view[$view]"
    } sql:$sql")
    val df = spark.sql(sql)
    if (cache) df.persist(level)
    if (view ne null) df.createOrReplaceTempView(view)
    df
  }

  /**
    * 视图写入hive
    *
    * @param view           视图名称
    * @param table          待写入hive表名称，默认为视图名称
    * @param p              分区表配置对象，默认为写入非分区表
    * @param save_mode      数据入库模式(overwrite:覆盖，append：追加，ignore：若存在则跳过写入，error：若存在则报错)
    * @param format_source  写入数据格式(parquet,orc,csv,json)
    * @param coalesce_limit 写入文件最大行数限制，用于预防小文件产生
    * @param refresh_view   数据写入后是否刷新视图
    * @return 写入数据的行数
    */
  def <==(view: String, table: String = null,
          p: partition = null,
          save_mode: SaveMode = save_mode,
          format_source: String = format_source,
          coalesce_limit: Long = coalesce_limit,
          refresh_view: Boolean = refresh_view): Long = {
    val df = this view view
    val tb = if (table ne null) table else view
    save(df, tb, p, save_mode, format_source, coalesce_limit, refresh_view)
  }

  /**
    * dataframe对象注册到视图
    *
    * @param df    待注册dataframe
    * @param view  视图名称
    * @param cache 是否写入缓存
    * @param level 写入缓存的级别
    * @return dataframe对象
    */
  def register(df: DataFrame, view: String,
               cache: Boolean = false,
               level: StorageLevel = StorageLevel.MEMORY_AND_DISK): DataFrame = {
    if (cache) df.persist(level)
    df.createOrReplaceTempView(view)
    log.info(s"dataframe register view[$view]")
    df
  }

  /**
    * 读取表，并注册为视图
    *
    * @param table   待读取表的名称
    * @param reality 是否读取真实表
    * @param cache   是否写入缓存
    * @param level   写入缓存的级别
    * @return dataframe对象
    */
  def read(table: String,
           reality: Boolean = true,
           cache: Boolean = false,
           level: StorageLevel = StorageLevel.MEMORY_AND_DISK): DataFrame = {
    if (reality) catalog.dropTempView(table)
    val df = {
      var df: DataFrame = null
      try {
        df = spark.table(table)
      } catch {
        case _: org.apache.spark.sql.AnalysisException =>
          throw RealityTableNotFoundException(s"reality table not found: $table")
      }
      df
    }
    if (cache) df.persist(level)
    df.createOrReplaceTempView(table)
    df
  }

  /**
    * 获取视图
    *
    * @param view 视图名称
    * @return dataframe对象
    */
  def view(view: String): DataFrame = spark.table(view)

  /**
    * 获取表/视图的行数
    *
    * @param table   待读取表的名称
    * @param reality 是否读取真实表
    * @return 表/视图的行数
    */
  def count(table: String, reality: Boolean = false): Long = {
    if (reality) this.read(table).count
    else spark.table(table).count
  }

  /**
    * 预览表的数据
    *
    * @param view     视图名称
    * @param limit    预览的行数
    * @param truncate 是否简化输出结果
    * @param reality  是否读取真实表
    */
  def show(view: String, limit: Int = 20, truncate: Boolean = false,
           reality: Boolean = false): Unit = {
    val df = if (reality) this.read(view) else spark.table(view)
    df.show(limit, truncate)
  }

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
           refresh_view: Boolean = refresh_view): Long = {
    catalog.dropTempView(table)
    log.info(s"$table[save mode:$save_mode,format source:$format_source] will be save")
    log.info(s"schema is:${df.schema}")
    if (df.storageLevel eq StorageLevel.NONE) df.cache
    val ct = df.count
    ct match {
      case 0l =>
        log.warn(s"$table is empty,skip save")
      case _ =>
        log.info(s"$table length is $ct,begin save")
        if (p eq null) {
          val coalesce_num = (1 + ct / coalesce_limit).toInt
          val writer = df.coalesce(coalesce_num).write
          save_mode match {
            case SaveMode.Append =>
              writer.insertInto(table)
            case _ =>
              writer
                .mode(save_mode).format(format_source)
                .saveAsTable(table)
          }
          log.info(s"$table[$coalesce_num flies] is saved")
        } else {
          import org.apache.spark.sql.functions.col
          if (p.values.isEmpty) {
            p ++ df.select(p.col.map(col): _*).distinct.collect
              .map(r => p.col.map(r.getAs[String]))
          }
          val cols = (df.columns.filterNot(p.col.contains) ++ p.col).map(col)
          val pdf = df.select(cols: _*)
          var is_init = p.is_init
          log.info(s"$table is partition table[init:$is_init],will run ${p.values.length} batch")
          p.values.map(v => v.map(s => s"'$s'")).map(v => v.zip(p.col)
            .map(s => s"${s._2}=${s._1}")).foreach(ps => {
            val pdf_ = pdf.where(ps.mkString(" and ")).cache
            val ct_ = pdf_.count
            val coalesce_num = (1 + ct_ / coalesce_limit).toInt
            val writer = pdf_.coalesce(coalesce_num).write
            if (is_init) {
              writer.mode(save_mode)
                .format(format_source)
                .partitionBy(p.col: _*)
                .saveAsTable(table)
              is_init = false
            }
            else {
              spark.sql(s"alter table $table drop if exists partition (${ps.mkString(",")})")
              writer.insertInto(table)
            }
            log.info(s"$table's partition[$ps] is saved,count:$ct_,file number:$coalesce_num")
            pdf_.unpersist
          })
        }
    }
    df.unpersist
    if (refresh_view && ct > 0l) this read table
    this register(df, table)
    ct
  }

  /**
    * 缓存dataframe
    *
    * @param df  待缓存dataframe
    * @param dfs 支持批量缓存
    */
  def cache(df: DataFrame, dfs: DataFrame*): Unit = {
    log.info("1 dataframe will be cache")
    df.cache
    if (dfs.nonEmpty) {
      log.info(s"${dfs.length} dataframe will be cache")
      dfs.foreach(_.cache)
    }

  }

  /**
    * 缓存视图
    *
    * @param view 视图名称
    */
  def cache(view: String*): Unit = {
    view.foreach(v => {
      log.info(s"$view will be cache")
      catalog.cacheTable(v)
    })
  }

  /**
    * 释放dataframe缓存
    *
    * @param df  dataframe
    * @param dfs 支持批量释放
    */
  def uncache(df: DataFrame, dfs: DataFrame*): Unit = {
    log.info("1 dataframe will be cleared")
    df.unpersist
    log.info("1 dataframe is cleared")
    log.info(s"${dfs.length} dataframe will be cleared")
    dfs.foreach(_.unpersist)
    log.info(s"${dfs.length} dataframe is cleared")
  }

  /**
    * 释放视图缓存
    *
    * @param view 视图名称
    */
  def uncache(view: String*): Unit = {
    view.foreach(v => {
      log.info(s"$view will be cleared")
      catalog.uncacheTable(v)
      log.info(s"$view is cleared")
    })
  }

  /**
    * 释放全部缓存
    */
  def uncache_all(): Unit = {
    log.info("all cache will be cleared")
    catalog.clearCache
    log.info("all cache is cleared")
  }

}

object SQL {

  lazy val log: Logger = Log.get("wheel>spark>sql")

  /**
    * 分区配置
    *
    * @param col 列名
    */
  case class partition(col: String*) {

    private var vals = new ListBuffer[Seq[String]]

    // 是否初始化表
    var is_init = false

    /**
      * 设置表为初始化
      *
      * @return this
      */
    def table_init: this.type = {
      is_init = true
      this
    }

    /**
      * 获取待分区的列的值
      *
      * @return Seq((分区值1，分区值2，分区值3...分区值n))
      */
    def values: Seq[Seq[String]] = {
      val cl = col.length
      vals.result().map(v => {
        param_verify(cl, v.length)
        v
      }).distinct
    }

    /**
      * 添加分区的值
      *
      * @param value (分区值1，分区值2，分区值3...分区值n)
      * @return this
      */
    def +(value: String*): this.type = {
      vals += value
      this
    }

    /**
      * 批量添加分区的值
      *
      * @param values Seq((分区值1，分区值2，分区值3...分区值n))
      * @return this
      */
    def ++(values: Seq[Seq[String]]): this.type = {
      vals ++= values
      this
    }

    private def param_verify(cl: Int, vl: Int): Unit = {
      if (cl != vl) throw IllegalParamException("partition column length not equal value length")
    }
  }

}
