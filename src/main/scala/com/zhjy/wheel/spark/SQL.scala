package com.zhjy.wheel.spark

import com.zhjy.wheel.common.Log
import com.zhjy.wheel.exception.RealityTableNotFoundException
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
  * Created by zzy on 2018/10/25.
  */
class SQL(spark: SparkSession) extends Core(spark) {

  import SQL._

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

  def register(df: DataFrame, view: String,
               cache: Boolean = false,
               level: StorageLevel = StorageLevel.MEMORY_AND_DISK): DataFrame = {
    if (cache) df.persist(level)
    df.createOrReplaceTempView(view)
    log.info(s"dataframe register view[$view]")
    df
  }

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

  def view(view: String): DataFrame = spark.table(view)

  def count(table: String, reality: Boolean = false): Long = {
    if (reality) this.read(table).count
    else spark.table(table).count
  }

  def show(view: String, limit: Int = 20, truncate: Boolean = false,
           reality: Boolean = false): Unit = {
    val df = if (reality) this.read(view) else spark.table(view)
    df.show(limit, truncate)
  }

}

object SQL {
  lazy val log: Logger = Log.get("wheel>spark>sql")
}
