package com.zhjy.wheel.spark

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.zhjy.wheel.common._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel

/**
  * Created by zzy on 2018/10/25.
  */
class Core(val spark: SparkSession) {

  import Core.LOGGER_STR

  def support_sql = SQL(this)

  def cache_dfs(df: DataFrame*): Unit = {
    Log.get(LOGGER_STR + "cache_dfs").info(s"${df.length}'s df will be cache")
    df.foreach(_.cache)
  }

  def cache(df: DataFrame): DataFrame = {
    Log.get(LOGGER_STR + "cache").info("1's df will be cache")
    df.cache
  }

  def cache_view(view: String*): Unit = {
    val log = Log.get(LOGGER_STR + "cache_view")
    view.foreach(v => {
      log.info(s"$view will be cache")
      spark.catalog.cacheTable(v)
    })
  }

  def uncache(df: DataFrame*): Unit = {
    val log = Log.get(LOGGER_STR + "uncache")
    log.info(s"${df.length}'s df will be cleared")
    df.foreach(_.unpersist)
    log.info(s"${df.length}'s df is cleared")
  }

  def uncache_view(view: String*): Unit = {
    val log = Log.get(LOGGER_STR + "uncache_view")
    view.foreach(v => {
      log.info(s"$view will be cleared")
      spark.catalog.uncacheTable(v)
      log.info(s"$view is cleared")
    })
  }

  def uncache_all(): Unit = {
    val log = Log.get(LOGGER_STR + "uncache_all")
    log.info("all cache will be cleared")
    spark.catalog.clearCache
    log.info("all cache is cleared")
  }

  def save_view(view: String,
                save_mode: SaveMode = SaveMode.Overwrite,
                format_source: String = "parquet"): Long = {
    val df = spark.read.table(view)
    save_df(df, view, save_mode, format_source)
  }

  def save_df(df: DataFrame, table: String,
              save_mode: SaveMode = SaveMode.Overwrite,
              format_source: String = "parquet"): Long = {
    val log: Logger = Log.get(LOGGER_STR + "save_df")
    val is_cached = df.storageLevel ne StorageLevel.NONE
    log.info(s"$table[cached:$is_cached,save mode:$save_mode,format source:$format_source] will be save")
    log.info(s"schema is\n${df.schema.treeString}")
    val ct = {
      if (is_cached) df
      else this.cache(df)
    }.count
    ct match {
      case 0l =>
        log.warn(s"$table is empty,skip save")
      case _ =>
        log.info(s"$table length is $ct,begin save")
        df.write
          .mode(save_mode)
          .format(format_source)
          .saveAsTable(table)
        log.info(s"$table is saved")
    }
    if (!is_cached) this.uncache(df)
    ct
  }


  def stop(): Unit = spark.stop

}

object Core {

  lazy val LOGGER_STR: String = "wheel~>spark~>core->"

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
