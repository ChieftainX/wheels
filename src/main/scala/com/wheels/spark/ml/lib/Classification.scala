package com.wheels.spark.ml.lib

import com.wheels.spark.SQL
import com.wheels.spark.ml.ML
import org.apache.spark.ml.linalg.{Matrix, Vector}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Classification(ml: ML) extends Serializable {

  val spark: SparkSession = ml.spark
  val sql: SQL = ml.sql

  /**
    * LogisticRegression 分类器
    *
    * @param features_col                 特征的列名称（类型需为org.apache.spark.ml.linalg.Vectors）
    * @param label_col                    标签的列名称（需要为Double类型，类别必须为0.0, 1.0, 2.0 ... 顺序递增顺延）
    * @param max_iter                     训练最大的迭代次数
    * @param reg                          正则化系数
    * @param family                       分类性质（二分类：binomial，多分类：multinomkial，根据训练数据自动判断auto）
    * @param elastic_net                  正则化组合方式（0.0：L2正则化，1.0：L1正则化，0～1：两种正则化方式按比例组合）
    * @param fit_intercept                是否训练截距
    * @param standardization              是否标准化
    * @param tolerance                    迭代收敛公差阈值
    * @param lower_bounds_on_coefficients 系数下界
    * @param lower_bounds_on_intercepts   截距下界
    * @param upper_bounds_on_coefficients 系数上界
    * @param upper_bounds_on_intercepts   截距上界
    * @param threshold                    二分类阈值
    * @param thresholds                   多分类阈值
    * @param weight_col                   记录权重多列名称
    * @param agg_depth                    聚合深度
    * @param see_prediction               预测结果是否需要预测详细值
    * @param see_probability              预测结果是否需要概率详细值
    */
  case class lr(features_col: String = "features",
                label_col: String = "label",
                max_iter: Int = 100,
                reg: Double = 0.0,
                family: String = "auto",
                elastic_net: Double = 0.0,
                fit_intercept: Boolean = true,
                standardization: Boolean = true,
                tolerance: Double = 1E-6,
                lower_bounds_on_coefficients: Matrix = null,
                lower_bounds_on_intercepts: Vector = null,
                upper_bounds_on_coefficients: Matrix = null,
                upper_bounds_on_intercepts: Vector = null,
                threshold: Double = -1,
                thresholds: Array[Double] = null,
                weight_col: String = null,
                agg_depth: Int = 2,
                see_prediction: Boolean = false,
                see_probability: Boolean = false) {

    import org.apache.spark.ml.classification.LogisticRegression
    import org.apache.spark.ml.classification.LogisticRegressionModel
    import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

    // 分类模型
    var model: LogisticRegressionModel = _

    /**
      * 模型训练
      *
      * @param view 训练视图名称
      */
    def ==>(view: String): LogisticRegressionModel = fit(sql view view)

    /**
      * 模型训练
      *
      * @param data 训练数据集
      */
    def fit(data: DataFrame): LogisticRegressionModel = {
      val obj: LogisticRegression = new LogisticRegression()
        .setFeaturesCol(features_col)
        .setLabelCol(label_col)
        .setMaxIter(max_iter)
        .setRegParam(reg)
        .setElasticNetParam(elastic_net)
        .setAggregationDepth(agg_depth)
        .setFamily(family)
        .setFitIntercept(fit_intercept)
        .setStandardization(standardization)
        .setTol(tolerance)

      if (lower_bounds_on_coefficients ne null) obj.setLowerBoundsOnCoefficients(lower_bounds_on_coefficients)
      if (lower_bounds_on_intercepts ne null) obj.setLowerBoundsOnIntercepts(lower_bounds_on_intercepts)
      if (upper_bounds_on_coefficients ne null) obj.setUpperBoundsOnCoefficients(upper_bounds_on_coefficients)
      if (upper_bounds_on_intercepts ne null) obj.setUpperBoundsOnIntercepts(upper_bounds_on_intercepts)
      if (threshold >= 0.0) obj.setThreshold(threshold)
      if (thresholds ne null) obj.setThresholds(thresholds)
      if (weight_col ne null) obj.setWeightCol(weight_col)

      val m = obj.fit(data)
      model = m
      m
    }

    /**
      * 保存模型
      *
      * @param path 路径
      */
    def save(path: String): Unit = model.write.overwrite().save(path)

    /**
      * 加载模型
      *
      * @param path 路径
      */
    def load(path: String): LogisticRegressionModel = {
      val m = LogisticRegressionModel.load(path)
      model = m
      m
    }

    /**
      * 分类预测
      *
      * @param view 待预测视图名称
      */
    def <==(view: String): DataFrame = {
      val prediction = transform(sql view view)
      sql register(prediction, view)
    }

    /**
      * 分类预测
      *
      * @param data 待预测数据集
      */
    def transform(data: DataFrame): DataFrame = {
      val drop_cols = Array(if (!see_prediction) "rawPrediction" else null,
        if (!see_probability) "probability" else null)
        .filter(_ ne null)
      val prediction = model.transform(data)
      if (drop_cols.isEmpty) prediction else prediction.drop(drop_cols: _*)
    }

    import com.wheels.common.types.Metric

    /**
      * 模型评估
      *
      * @param view           待评估视图名称
      * @param label_col      label列名称
      * @param prediction_col 预测label列名称
      * @param metric         评估指标（可选值ACC PRECISION RECALL F1）
      */
    def <--(view: String,
            label_col: String = label_col,
            prediction_col: String = "prediction",
            metric: Metric = Metric.ACC): Double = evaluation(sql view view, label_col, prediction_col, metric)

    /**
      * 模型评估
      *
      * @param data           待评估数据集
      * @param label_col      label列名称
      * @param prediction_col 预测label列名称
      * @param metric         评估指标（可选值ACC PRECISION RECALL F1）
      */
    def evaluation(data: DataFrame,
                   label_col: String = label_col,
                   prediction_col: String = "prediction",
                   metric: Metric = Metric.ACC): Double =
      new MulticlassClassificationEvaluator()
        .setLabelCol(label_col)
        .setPredictionCol(prediction_col)
        .setMetricName(metric.get)
        .evaluate(data)

  }

}
