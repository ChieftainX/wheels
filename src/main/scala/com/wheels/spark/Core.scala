package com.wheels.spark

import com.wheels.common.{Log, Time}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql.catalog.Catalog


/**
  * Created by zzy on 2018/10/25.
  */
class Core(val spark: SparkSession) {

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

}

object Core {

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
    core
  }

  private def default_conf(builder: Builder): Unit = {
    builder
      .config("spark.debug.maxToStringFields", "10086")
      .config("spark.sql.broadcastTimeout", "3000")
      .config("wheel.spark.sql.hive.save.mode", "overwrite")
      .config("wheel.spark.sql.hive.save.format", "parquet")
      .config("wheel.spark.sql.hive.save.file.lines.limit", "1000000")
      .config("wheel.spark.sql.hive.save.refresh.view", "false")
  }

}
