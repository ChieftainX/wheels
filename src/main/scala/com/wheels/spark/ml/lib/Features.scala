package com.wheels.spark.ml.lib

import com.wheels.spark.ml.ML
import org.apache.spark.ml.feature._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.wheels.spark.SQL._
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.types.DoubleType


class Features(ml: ML) extends Serializable {

  val spark: SparkSession = ml.spark

  case class indexer() {

    /**
      * 字符列->索引列
      *
      * @param view        视图名称
      * @param input_col   输入列
      * @param output_col  输出列，默认为input_col + "_index"
      * @param output_view 输出视图
      * @return 转化模型
      */
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

    /**
      * 索引列->字符列
      *
      * @param view        视图名称
      * @param input_col   输入列
      * @param output_col  输出列，默认为input_col + "_str"
      * @param output_view 输出视图
      * @param labels      索引标签向量
      * @return 转换后的dataframe
      */
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

  /**
    * 标准化
    *
    * @param view        视图
    * @param cols        待标注化待列
    * @param output_view 输出视图
    * @param drop        是否删除向量列
    * @param replace     是否替换原始列
    */
  case class scaler(view: String, cols: Seq[String],
                    output_view: String = null, drop: Boolean = true, replace: Boolean = true) {

    private val df = spark.sql(s"select *,${to_vector(cols, WHEELS_INPUT_COL)} from $view")

    var min: Seq[Double] = _
    var max: Seq[Double] = _
    var maxabs: Seq[Double] = _
    var mean: Seq[Double] = _
    var std: Seq[Double] = _

    /**
      * MinMax
      *
      * @return 处理后的dataframe
      */
    def mix_max: DataFrame = {
      val scaler = new MinMaxScaler()
        .setInputCol(WHEELS_INPUT_COL)
        .setOutputCol(WHEELS_OUTPUT_COL)
      val model = scaler.fit(df)
      model.originalMax.toArray.toSeq
      min = model.originalMin.toArray.toSeq
      max = model.originalMax.toArray.toSeq
      aftercure(model.transform(df))
    }

    /**
      * z-score
      *
      * @return 处理后的dataframe
      */
    def z_score: DataFrame = z_score(with_std = true, with_mean = false)

    /**
      * z-score
      *
      * @param with_std  是否将数据缩放到单位标准差
      * @param with_mean 缩放前是否以均值为中心
      * @return 处理后的dataframe
      */
    def z_score(with_std: Boolean, with_mean: Boolean): DataFrame = {
      val scaler = new StandardScaler()
        .setInputCol(WHEELS_INPUT_COL)
        .setOutputCol(WHEELS_OUTPUT_COL)
        .setWithStd(with_std)
        .setWithMean(with_mean)
      val model = scaler.fit(df)
      mean = model.mean.toArray.toSeq
      std = model.std.toArray.toSeq
      aftercure(model.transform(df))
    }

    /**
      * MaxAbs
      *
      * @return 处理后的dataframe
      */
    def max_abs: DataFrame = {
      val scaler = new MaxAbsScaler()
        .setInputCol(WHEELS_INPUT_COL)
        .setOutputCol(WHEELS_OUTPUT_COL)
      val model = scaler.fit(df)
      maxabs = model.maxAbs.toArray.toSeq
      aftercure(model.transform(df))
    }

    private def aftercure(scaled: DataFrame): DataFrame = {
      var schema = scaled.schema
      val suffix = "_scaled"
      cols.foreach(c => schema = schema.add(s"$c$suffix", DoubleType))
      val scaled_ = spark.createDataFrame(scaled.rdd.map(r =>
        Row.merge(r, Row.fromSeq(r.getAs[DenseVector](WHEELS_OUTPUT_COL).toArray))
      ), schema).drop(WHEELS_TMP_COL)
      val handle_drop = if (drop) scaled_.drop(WHEELS_INPUT_COL, WHEELS_OUTPUT_COL) else scaled_
      val handle_replace = if (replace) {
        var tmp = handle_drop.drop(cols: _*)
        cols.foreach(c => tmp = tmp.withColumnRenamed(s"$c$suffix", c))
        tmp
      } else handle_drop
      handle_replace.createOrReplaceTempView(if (output_view == null) view else output_view)
      handle_replace
    }
  }

}