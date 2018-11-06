package com.wheels.spark.ml.lib

import com.wheels.spark.ml.ML
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, SparkSession}


/** *
  * 用于推荐算法实现
  */
class Recommendation(ml: ML) {
  val spark: SparkSession = ml.spark

  case class lfm(rank: Int = 10, max_iter: Int = 10, reg_param: Double = 0.1,
                 user_blocks: Int = 10, item_blocks: Int = 10,
                 implicit_prefs: Boolean = false, alpha: Double = 1.0,
                 user_col: String = "user_id", item_col: String = "item_id", rating_col: String = "rating",
                 nonnegative: Boolean = false, checkpoint_interval: Int = 10,
                 intermediate_storage_level: String = "MEMORY_AND_DISK",
                 final_storage_level: String = "MEMORY_AND_DISK",
                 cold_start_strategy: String = "drop",
                 is_prediction: Boolean = false,
                 training_proportion: Double = 0.8
                ) {

    def ==>(view: String): (ALSModel, Double) = {
      dataframe(spark.table(view))
    }

    def dataframe(input: DataFrame): (ALSModel, Double) = {

      val Array(training, test) = input.randomSplit(Array(training_proportion, 1 - training_proportion))

      val als = new ALS()
        .setRank(rank)
        .setMaxIter(max_iter)
        .setRegParam(reg_param)
        .setNumUserBlocks(user_blocks)
        .setNumItemBlocks(item_blocks)
        .setImplicitPrefs(implicit_prefs)
        .setAlpha(alpha)
        .setUserCol(user_col)
        .setItemCol(item_col)
        .setRatingCol(rating_col)
        .setNonnegative(nonnegative)
        .setCheckpointInterval(checkpoint_interval)
        .setIntermediateStorageLevel(intermediate_storage_level)
        .setFinalStorageLevel(final_storage_level)
        .setColdStartStrategy(cold_start_strategy)

      val model = als.fit(if (is_prediction) training else input)

      val rmse = {
        if (is_prediction) {
          val predictions = model.transform(test)
          val evaluator = new RegressionEvaluator()
            .setMetricName("rmse")
            .setLabelCol(rating_col)
            .setPredictionCol("prediction")
          evaluator.evaluate(predictions)
        } else -1.0
      }
      (model, rmse)
    }
  }

}
