package com.wheels.spark.ml.lib

import com.wheels.spark.ml.ML
import org.apache.spark.ml.feature._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.wheels.spark.SQL._


class Features(ml: ML) {

  val spark: SparkSession = ml.spark

  case class indexer() {

    def s2i(view: String, input_col: String,
            output_col: String = null, output_view: String = null,
            handle_invalid: String = "error"): StringIndexerModel = {
      val indexer = new StringIndexer()
        .setInputCol(input_col)
        .setOutputCol(if (output_col ne null) output_col else input_col + "_index")
        .setHandleInvalid(handle_invalid)
      val df = spark.table(view)
      val model = indexer.fit(df)
      val indexed = model.transform(df)
      indexed.createOrReplaceTempView(if (output_view ne null) output_view else view)
      model
    }

    def i2s(view: String, input_col: String,
            output_col: String = null, output_view: String = null,
            labels: Array[String] = null): DataFrame = {
      val converter = new IndexToString()
        .setInputCol(input_col)
        .setOutputCol(if (output_col ne null) output_col else input_col + "_str")
      if (labels ne null) converter.setLabels(labels)
      val df = spark.table(view)
      val res = converter.transform(df)
      res.createOrReplaceTempView(if (output_view ne null) output_view else view)
      res
    }

  }

  case class scaler(view: String, cols: Seq[String],
                    output_view: String = null, drop: Boolean = true, replace: Boolean = true) {

    def mix_max: DataFrame = {

      val df = spark.sql(s"select *,${to_vector(cols, WHEELS_INPUT_COL)} from $view")
      val scaler = new MinMaxScaler()
        .setInputCol(WHEELS_INPUT_COL)
        .setOutputCol(WHEELS_OUTPUT_COL)
      aftercure(scaler.fit(df).transform(df))
    }

    def z_score: DataFrame = {
      val df = spark.sql(s"select *,${to_vector(cols, WHEELS_INPUT_COL)} from $view")
      val scaler = new MaxAbsScaler()
        .setInputCol(WHEELS_INPUT_COL)
        .setOutputCol(WHEELS_OUTPUT_COL)
      aftercure(scaler.fit(df).transform(df))
    }

    private def aftercure(scaled: DataFrame): DataFrame = {
      var scaled_ = scaled.withColumn(WHEELS_TMP_COL, expr(vector2array(WHEELS_OUTPUT_COL)))
      var i = 0
      cols.foreach(c => {
        val c_ = if (replace) c else c + "_scaled"
        scaled_ = scaled_.withColumn(c_, expr(s"$WHEELS_TMP_COL[$i]"))
        i += 1
      })
      scaled_ = scaled_.drop(WHEELS_TMP_COL)
      val r = if (drop) scaled_.drop(WHEELS_INPUT_COL, WHEELS_OUTPUT_COL) else scaled_
      r.createOrReplaceTempView(if (output_view == null) view else output_view)
      r
    }
  }

}