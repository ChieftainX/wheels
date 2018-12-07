package com.wheels.spark.ml

import com.wheels.spark.SQL
import com.wheels.spark.ml.lib.{Features, Recommendation}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

class ML(sql: SQL) extends Serializable {

  val spark: SparkSession = sql.spark

  def recommendation: Recommendation = new Recommendation(this)

  def features: Features = new Features(this)

  /**
    * 联合加权配置项
    *
    * @param weight_info 类型与权重的对应关系
    * @param type_col    类型列名，默认为type
    * @param keys        唯一标示，默认为 Seq("user_id", "item_id")
    * @param degree_col  评分列名，默认为degree
    * @param udf         自定义聚合函数，默认为求和
    * @param output      输出视图名称，默认无输出视图
    */
  case class union_weighing(
                             weight_info: Map[String, Double],
                             type_col: String = "type",
                             keys: Seq[String] = Seq("user_id", "item_id"),
                             degree_col: String = "degree",
                             udf: Seq[Double] => Double = (dgs: Seq[Double]) => dgs.sum,
                             output: String = null) {

    def ==>(input: String): DataFrame = dataframe(sql view input)

    def dataframe(input: DataFrame): DataFrame = {
      val keys_ = keys
      val degree_col_ = degree_col
      val udf_ = udf
      val df = spark.createDataFrame(
        input.withColumn(degree_col, {
          var flag_ = when(lit(1) === 0, 0.0)
          weight_info.map(m => (lit(m._1), lit(m._2)))
            .foreach(kv => flag_ = flag_.when(col(type_col) === kv._1, col(degree_col) * kv._2))
          flag_.otherwise(col(degree_col))
        }).rdd.map(r => (keys_.map(k => r.get(r.fieldIndex(k)).toString), r.getAs[Double](degree_col_)))
          .groupByKey.map(r => Row.merge(Row.fromSeq(r._1), Row(udf_(r._2.toSeq)))),
        StructType(keys.map(StructField(_, StringType, nullable = true))
          ++ Seq(StructField(degree_col, DoubleType, nullable = true))
        ))
      sql register(df, output)
    }
  }


}
