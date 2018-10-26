package com.zhjy.wheel.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.zhjy.wheel.common._
import org.apache.log4j.{Level, Logger}

/**
  * Created by zzy on 2018/10/25.
  */
class Core(val spark: SparkSession) {

  def support_sql = SQL(this)

  def cache_df(df: DataFrame*): Unit = df.foreach(_.cache)

  def cache_view(view: String*): Unit = view.foreach(spark.catalog.cacheTable)

  def uncache_df(df: DataFrame*): Unit = df.foreach(_.unpersist)

  def uncache_view(view: String*): Unit = view.foreach(spark.catalog.uncacheTable)

  def uncache_all(): Unit = spark.catalog.clearCache

  def stop(): Unit = spark.stop()

}

object Core {

  def log_less(levels: Map[String, Level] = Map(
    "org.apache.hadoop.util.NativeCodeLoader" -> Level.ERROR,
    "org.apache.spark" -> Level.WARN
  )): this.type = {
    levels.foreach {
      case (k, v) => Logger.getLogger(k).setLevel(v)
    }
    this
  }

  def apply(name: String = s"run spark @ ${Time.now}",
            conf: Map[String, Any] = Map(),
            hive_support: Boolean = true,
            database: String = null
           ): Core = {
    val spark: SparkSession = {
      val builder = SparkSession.builder()
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
