package com.wheels.spark

import java.util.Locale

import com.wheels.common.{Log, Time}
import com.wheels.exception.IllegalConfException
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql.catalog.Catalog


/**
  * Created by zzy on 2018/10/25.
  */
class Core(@transient val spark: SparkSession) extends Serializable {

  import Core.log

  /**
    * 是否支持sql模块功能
    *
    * @return sql对象
    */
  def support_sql: SQL = {
    log.info("this wheel[spark] support sql")
    SQL(spark)
  }

  /**
    * 获取 catalog 对象
    *
    * @return catalog
    */
  def catalog: Catalog = spark.catalog

  /**
    * 释放资源
    */
  def stop(): Unit = {
    log.info("spark will stop")
    spark.stop
    log.info("spark is stop")
    println(
      """
        |
        |         ┌─┐       ┌─┐
        |      ┌──┘ ┴───────┘ ┴──┐
        |      │                 │
        |      │        ━        │
        |      │    >       <    │
        |      │                 │
        |      │    ... ⌒ ...    │
        |      │                 │
        |      └───┐         ┌───┘
        |          │         │
        |          │         │ Code is far away from bug with the animal protecting!
        |          │         │
        |          │         └──────────────┐
        |          │                        │
        |          │                        ├─┐
        |          │                        ┌─┘
        |          │                        │
        |          └─┐  ┐  ┌───────┬──┐  ┌──┘
        |            │ ─┤ ─┤       │ ─┤ ─┤
        |            └──┴──┘       └──┴──┘
      """.stripMargin)
  }

  lazy val DOP: Int = {
    val cf = spark.sparkContext.getConf
    val cores_num: Int = cf.get("spark.executor.cores", "2").toInt
    val instances_num: Int = cf.get("spark.executor.instances", "5").toInt
    cores_num * instances_num
  }

  def get_save_mode(key: String): SaveMode = {
    val mode = spark.conf.get(key)
    mode.toLowerCase(Locale.ROOT) match {
      case "overwrite" => SaveMode.Overwrite
      case "append" => SaveMode.Append
      case "ignore" => SaveMode.Ignore
      case "error" | "default" => SaveMode.ErrorIfExists
      case _ => throw IllegalConfException(s"unknown save mode: $mode." +
        s"accepted save modes are 'overwrite', 'append', 'ignore', 'error'.")
    }
  }


}

object Core {

  @transient
  lazy val log: Logger = Log.get("wheel>spark>core")

  /**
    * 创建核心功能对象
    *
    * @param name         app名称
    * @param conf         runtime 配置信息
    * @param hive_support 是否开启hive支持
    * @param database     database名称
    * @param log_less     是否需要少量的日志输出
    * @return 核心功能对象
    */
  def apply(name: String = s"run spark @ ${Time.now}",
            conf: Map[String, Any] = Map(),
            hive_support: Boolean = true,
            database: String = null,
            log_less: Boolean = true
           ): Core = {
    if (log_less) {
      Log.log_setter(Map(
        "org.apache.hadoop" -> Level.ERROR,
        "org" -> Level.WARN
      ))
    }
    val spark: SparkSession = {
      val builder = SparkSession.builder()
      if (hive_support) {
        builder.enableHiveSupport
          .config("hive.exec.dynamic.partition", "true")
          .config("hive.exec.dynamic.partition.mode", "nonstrict")
          .config("hive.exec.max.dynamic.partitions", "36500")
          .config("hive.exec.max.dynamic.partitions.pernode", "3650")
      }
      this.default_conf(builder)
      conf.foreach {
        case (k, v) => builder.config(k, v.toString)
      }
      builder.appName(name).getOrCreate()
    }

    val core = new Core(spark)
    if (database ne null) spark.sql(s"use $database")
    println(
      """
        |
        |         ┌─┐       ┌─┐
        |      ┌──┘ ┴───────┘ ┴──┐
        |      │                 │
        |      │       ───       │
        |      │  ─┬┘       └┬─  │
        |      │                 │
        |      │       ─┴─       │
        |      │                 │
        |      └───┐         ┌───┘
        |          │         │
        |          │         │   神兽保佑
        |          │         │   代码无BUG!
        |          │         └──────────────┐
        |          │                        │
        |          │                        ├─┐
        |          │                        ┌─┘
        |          │                        │
        |          └─┐  ┐  ┌───────┬──┐  ┌──┘
        |            │ ─┤ ─┤       │ ─┤ ─┤
        |            └──┴──┘       └──┴──┘
      """.stripMargin)
    core
  }

  private def default_conf(builder: Builder): Unit = {
    import com.wheels.common.Conf._
    builder
      .config("spark.debug.maxToStringFields", "10086")
      .config("spark.sql.broadcastTimeout", "3000")
      .config(WHEEL_SPARK_SQL_HIVE_SAVE_MODE, "overwrite")
      .config(WHEEL_SPARK_SQL_JDBC_SAVE_MODE, "append")
      .config(WHEEL_SPARK_SQL_HIVE_SAVE_FORMAT, "parquet")
      .config(WHEEL_SPARK_SQL_HIVE_SAVE_FILE_LINES_LIMIT, "1000000")
      .config(WHEEL_SPARK_SQL_HIVE_SAVE_REFRESH_VIEW, "false")
  }

}
