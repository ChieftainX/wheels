package com.zzy.ts.spark.ml

import com.wheels.spark.ml.ML
import com.wheels.spark.{Core, SQL}
import com.zzy.ts.spark.DBS
import org.junit.jupiter.api._
import org.junit.jupiter.api.TestInstance.Lifecycle


@TestInstance(Lifecycle.PER_CLASS)
@DisplayName("测试Spark-Ml模块")
class TS {

  var sql: SQL = _
  var ml: ML = _

  @BeforeAll
  def init_all(): Unit = {
    val conf = Map(
      "spark.master" -> "local[*]",
      "zzy.param" -> "fk"
    )

    sql = Core(
      conf = conf,
      hive_support = false
    ).support_sql

    ml = sql.support_ml
  }

  @BeforeEach
  def init(): Unit = {}


  @Test
  @DisplayName("测试联合加权功能")
  def ts_weighing_rank(): Unit = {

    DBS.recommend_res(sql)

    println("原始数据：")

    sql show "recommend_res"

    val uw = ml.union_weighing(
      Map(
        "t1" -> 0.33,
        "t2" -> 0.22,
        "t3" -> 0.45)
      , output = "recommend_weighing_res"
    )

    uw ==> "recommend_res"

    println("加权（默认：累加）后：")

    sql show "recommend_weighing_res"

    val df = sql view "recommend_res"

    ml.union_weighing(Map(
      "t1" -> 0.33,
      "t2" -> 0.22,
      "t3" -> 0.45
    ),
      udf = (degrees: Seq[Double]) => {
        val ct = degrees.length
        degrees.sum / ct
      },
      output = "recommend_weighing_res") dataframe df

    println("加权后(自定义：取平均)：")

    sql show "recommend_weighing_res"
  }

  @Test
  @DisplayName("测试lfm")
  def ts_re_lfm(): Unit = {
    DBS.movielens_ratings(sql)

    sql show "movielens_ratings"

    val recommendation = ml.recommendation

    val lfm = recommendation.lfm()

    lfm ==> "movielens_ratings"

  }

  @AfterEach
  def after(): Unit = {}

  @AfterAll
  def after_all(): Unit = {
    sql.uncache_all()
    sql.stop()
  }
}