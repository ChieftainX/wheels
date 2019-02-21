package com.wheels.spark.ml.lib

import com.wheels.spark.SQL
import com.wheels.spark.ml.ML
import org.apache.spark.ml.linalg.{Matrix, Vector}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Classification(ml: ML) extends Serializable {

  val spark: SparkSession = ml.spark
  val sql: SQL = ml.sql

  case class lr(features_col: String = "features",
                label_col: String = "label",
                max_iter: Int = 100,
                reg: Double = 0.0,
                //binomial multinomkial auto
                family: String = "auto",
                elastic_net: Double = 0.0,
                fit_intercept: Boolean = true,
                standardization: Boolean = true,
                tolerance: Double = 1E-6,
                lower_bounds_on_coefficients: Matrix = null,
                lower_bounds_on_intercepts: Vector = null,
                upper_bounds_on_coefficients: Matrix = null,
                upper_bounds_on_intercepts: Vector = null,
                threshold: Double = null,
                thresholds: Array[Double] = null,
                weight_col: String = null,
                agg_depth: Int = 2) {

    import org.apache.spark.ml.classification.LogisticRegression
    import org.apache.spark.ml.classification.LogisticRegressionModel
    import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

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
    if (threshold ne null) obj.setThreshold(threshold)
    if (thresholds ne null) obj.setThresholds(thresholds)
    if (weight_col ne null) obj.setWeightCol(weight_col)

    def ==>(view: String): LogisticRegressionModel = model(sql view view)

    def model(data: DataFrame): LogisticRegressionModel = obj.fit(data)

    import com.wheels.common.types.Metric

    def evaluation(data: DataFrame,
                   label_col: String = label_col,
                   prediction_col: String = "prediction",
                   metric: Metric = Metric.apply.ACC
                  ): Double = {
      val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol(label_col)
        .setPredictionCol(prediction_col)
        .setMetricName(metric.get)
      evaluator.evaluate(data)
    }

  }

}
