package com.wheels.spark.ml

import com.wheels.spark.SQL
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType, DoubleType}

class ML(sql: SQL) {

  def spark: SparkSession = sql.spark

  def weighing(input: String,
               weight_info: Map[String, Double],
               type_col: String = "type",
               keys: Seq[String] = Seq("user_id", "item_id"),
               degree_col: String = "degree",
               udf: Seq[Double] => Double = (dgs: Seq[Double]) => dgs.sum,
               output: String = null): DataFrame = {
    val df = weighing_df(sql view input, weight_info, type_col, keys, degree_col, udf)
    sql register(df, output)
  }

  def weighing_df(input: DataFrame,
                   weight_info: Map[String, Double],
                   type_col: String = "type",
                   keys: Seq[String] = Seq("user_id", "item_id"),
                   degree_col: String = "degree",
                   udf: Seq[Double] => Double = (dgs: Seq[Double]) => dgs.sum): DataFrame = {
    spark.createDataFrame(
      input.withColumn(degree_col, {
        var flag_ = when(lit(1) === 0, 0.0)
        weight_info.map(m => (lit(m._1), lit(m._2)))
          .foreach(kv => flag_ = flag_.when(col(type_col) === kv._1, col(degree_col) * kv._2))
        flag_.otherwise(col(degree_col))
      }).rdd.map(r => (keys.map(r.getAs[String]), r.getAs[Double](degree_col)))
        .groupByKey.map(r => Row.merge(Row.fromSeq(r._1), Row(udf(r._2.toSeq)))),
      StructType(keys.map(StructField(_, StringType, nullable = true))
        ++ Seq(StructField(degree_col, DoubleType, nullable = true))
      ))
  }


}
