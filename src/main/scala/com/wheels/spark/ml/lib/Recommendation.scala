package com.wheels.spark.ml.lib

import com.wheels.spark.ml.ML
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{DataFrame, SparkSession}


/** *
  * 用于推荐算法实现
  */
class Recommendation(ml: ML) {
  val spark: SparkSession = ml.spark

  case class lfm() {

    def ==>(view: String): Unit = {
      dataframe(spark.table(view))
    }

    def dataframe(input: DataFrame): Unit = {
      val Array(training, test) = input.randomSplit(Array(0.8, 0.2))
      val als = new ALS()
        .setUserCol("user_id")
        .setItemCol("item_id")
        .setRatingCol("rating")

      als.params.toSeq.foreach(p=>{
        println("==================")
        println(p.doc)
        println(p.->())
        println(p.name)
        println("------------------")
      })
      val model = als.fit(training)
      model.setColdStartStrategy("drop")
      val predictions = model.transform(test)
      val evaluator = new RegressionEvaluator()
        .setMetricName("rmse")
        .setLabelCol("rating")
        .setPredictionCol("prediction")
      val rmse = evaluator.evaluate(predictions)
      println(s"Root-mean-square error = $rmse")
//      val userRecs = model.recommendForAllUsers(10)
//      val movieRecs = model.recommendForAllItems(10)
    }
  }

}
