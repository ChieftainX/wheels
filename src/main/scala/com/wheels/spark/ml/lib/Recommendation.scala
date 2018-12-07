package com.wheels.spark.ml.lib

import com.wheels.common.Log
import com.wheels.spark.ml.ML
import org.apache.log4j.Logger
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


/** *
  * 用于推荐算法实现
  */
class Recommendation(ml: ML) extends Serializable {

  import Recommendation._

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

    var model: ALSModel = _

    def ==>(view: String): ALSModel = {
      dataframe(spark.table(view))
    }

    def dataframe(input: DataFrame): ALSModel = {

      log.info(this)

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

      model = als.fit(if (is_prediction) training else input)
      if (is_prediction) {
        val predictions = model.transform(test)
        val evaluator = new RegressionEvaluator()
          .setMetricName("rmse")
          .setLabelCol(rating_col)
          .setPredictionCol("prediction")
        val rmse = evaluator.evaluate(predictions)
        log.info("rmse[lfm]: " + rmse)
      }
      model
    }

    def recommend4users(num: Int = 50, output_view: String = null, normalize_flat: Boolean = false): DataFrame = {
      val df = model.recommendForAllUsers(num)
      val rs = if (normalize_flat) nf(df).toDF(user_col, item_col, rating_col) else df
      if (output_view ne null) rs.createOrReplaceTempView(output_view)
      rs
    }

    def recommend4items(num: Int = 50, output_view: String = null, normalize_flat: Boolean = false): DataFrame = {
      val df = model.recommendForAllItems(num)
      val rs = if (normalize_flat) nf(df).toDF(item_col, user_col, rating_col) else df
      if (output_view ne null) rs.createOrReplaceTempView(output_view)
      rs
    }

    private def nf(df: DataFrame): Dataset[(Int, Int, Double)] = {
      import spark.implicits._
      df.flatMap(r => {
        val res = r.getAs[Seq[Row]](1)
        val dgs = res.map(_.getAs[Float](1))
        val dg_max = dgs.max
        val dg_min = dgs.min
        val dg_mai = dg_max - dg_min
        res.map(re => (r.getAs[Int](0), re.getAs[Int](0),
          if (dg_mai == 0) 0.0 else (re.getAs[Float](1) - dg_min) / dg_mai))
      })
    }

  }

}

object Recommendation {
  lazy val log: Logger = Log.get("wheel>spark>ml>lib>recommendation")
}
