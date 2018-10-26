package com.zhjy.wheel.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
  * Created by zzy on 2018/10/25.
  */
class SQL(val core: Core) {

  val spark: SparkSession = core.spark

  def exe(sql: String, view: String = null,
          cache: Boolean = false,
          level: StorageLevel = StorageLevel.MEMORY_AND_DISK): DataFrame = {
    val df = spark.sql(sql)
    if (cache) df.persist(level)
    if (view ne null) df.createOrReplaceTempView(view)
    df
  }

  def view(df: DataFrame, view: String,
           cache: Boolean = false,
           level: StorageLevel = StorageLevel.MEMORY_AND_DISK): DataFrame = {
    if (cache) df.persist(level)
    df.createOrReplaceTempView(view)
    df
  }

  def stop(): Unit = core.stop()

}

object SQL {
  def apply(core: Core): SQL = new SQL(core)
}
