package com.wheels.spark.ml.lib

import com.wheels.spark.ml.ML
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, StringIndexerModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

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

}
