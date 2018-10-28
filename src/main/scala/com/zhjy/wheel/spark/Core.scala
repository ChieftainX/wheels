package com.zhjy.wheel.spark

import org.apache.spark.sql.SparkSession
import com.zhjy.wheel.common._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalog.Catalog


/**
  * Created by zzy on 2018/10/25.
  */
class Core(val spark: SparkSession) {

  def support_sql = new SQL(spark)

  val catalog: Catalog = spark.catalog

  def stop(): Unit = spark.stop

}

object Core {

  lazy val log: Logger = Log.get("wheel>spark>core")

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
        .config("spark.sql.broadcastTimeout", "3000")
      if (hive_support) {
        builder.enableHiveSupport
          .config("hive.exec.dynamic.partition", "true")
          .config("hive.exec.dynamic.partition.mode", "nonstrict")
          .config("hive.exec.max.dynamic.partitions.pernode", "36500")
      }
      conf.foreach {
        case (k, v) => builder.config(k, v.toString)
      }
      builder.appName(name).getOrCreate()
    }

    val core = new Core(spark)
    if (database ne null) spark.sql(s"use $database")
    core
  }

}