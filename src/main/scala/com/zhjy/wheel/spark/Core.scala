package com.zhjy.wheel.spark

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.zhjy.wheel.common._
import com.zhjy.wheel.exception.IllegalParamException
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

/**
  * Created by zzy on 2018/10/25.
  */
class Core(val spark: SparkSession) {

  import Core._

  def support_sql = new SQL(spark)

  val catalog: Catalog = spark.catalog

  def cache_many(df: DataFrame*): Unit = {
    log.info(s"${df.length} dataframe will be cache")
    df.foreach(_.cache)
  }

  def cache(df: DataFrame): DataFrame = {
    log.info("1 dataframe will be cache")
    df.cache
  }

  def cache_view(view: String*): Unit = {
    view.foreach(v => {
      log.info(s"$view will be cache")
      catalog.cacheTable(v)
    })
  }

  def uncache(df: DataFrame*): Unit = {
    log.info(s"${df.length} dataframe will be cleared")
    df.foreach(_.unpersist)
    log.info(s"${df.length} dataframe is cleared")
  }

  def uncache_view(view: String*): Unit = {
    view.foreach(v => {
      log.info(s"$view will be cleared")
      catalog.uncacheTable(v)
      log.info(s"$view is cleared")
    })
  }

  def uncache_all(): Unit = {
    log.info("all cache will be cleared")
    catalog.clearCache
    log.info("all cache is cleared")
  }

  object hive {

    object default {
      val save_mode: SaveMode = SaveMode.Overwrite
      val format_source: String = "parquet"
      val coalesce_limit: Long = 100 * 10000
    }

    def <==(view: String, table: String = null,
            p: partition = null,
            save_mode: SaveMode = default.save_mode,
            format_source: String = default.format_source,
            coalesce_limit: Long = default.coalesce_limit): Long = {
      val df = spark.read.table(view)
      val tb = if (table ne null) table else view
      save(df, tb, p, save_mode, format_source, coalesce_limit)
    }

    def save(df: DataFrame, table: String,
             p: partition = null,
             save_mode: SaveMode = default.save_mode,
             format_source: String = default.format_source,
             coalesce_limit: Long = default.coalesce_limit): Long = {
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
              println(p.values)
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
      if (ct > 0) spark.read.table(table).createOrReplaceTempView(table)
      ct
    }
  }

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

  case class partition(col: String*) {

    private var vals = new ListBuffer[Seq[String]]

    var is_init = false

    def table_init: this.type = {
      is_init = true
      this
    }

    def values: Seq[Seq[String]] = {
      val cl = col.length
      vals.result().map(v => {
        param_verify(cl, v.length)
        v
      }).distinct
    }

    def +(value: String*): this.type = {
      vals += value
      this
    }

    def ++(values: Seq[Seq[String]]): this.type = {
      vals ++= values
      this
    }

    private def param_verify(cl: Int, vl: Int): Unit = {
      if (cl != vl) throw IllegalParamException("partition column length not equal value length")
    }
  }

}