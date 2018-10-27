package com.zhjy.wheel.spark

import com.zhjy.wheel.common.Log
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
    if (view ne null) {
      df.createOrReplaceTempView(view)
    }
    df
  }

  def view(df: DataFrame, view: String,
           cache: Boolean = false,
           level: StorageLevel = StorageLevel.MEMORY_AND_DISK): DataFrame = {
    if (cache) df.persist(level)
    df.createOrReplaceTempView(view)
    log.info(s"dataframe register view[$view]")
    df
  }

  def read(table: String,
           cache: Boolean = false,
           level: StorageLevel = StorageLevel.MEMORY_AND_DISK): DataFrame = {
    val df = spark.read.table(table)
    if (cache) df.persist(level)
    df.createOrReplaceTempView(table)
    df
  }

  def show(view: String, num: Int = 20, truncate: Boolean = false): Unit = {
    spark.read.table(view).show(num, truncate)
  }

}

object SQL {
  lazy val log: Logger = Log.get("wheel>spark>sql")
}
