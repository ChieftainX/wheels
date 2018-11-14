package com.zzy.ts.spark.ml

import com.wheels.common.Log
import com.wheels.spark.ml.ML
import com.wheels.spark.{Core, SQL}
import com.zzy.ts.spark.DBS
import org.apache.log4j.Level
import org.apache.spark.sql.Row
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
      "spark.master" -> "local[*]"
    )

    Log.log_setter(Map(
      "com.github.fommil" -> Level.ERROR
    ))

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
  @DisplayName("测试indexer")
  def ts_f_indexer(): Unit = {
    DBS.movielens_ratings(sql)

    sql ==> (
      """
        |select
        |concat('u-',user_id) user_id,
        |concat('i-',item_id) item_id,
        |rating,ts
        |from movielens_ratings limit 10
      """.stripMargin, "movielens_ratings")

    sql show "movielens_ratings"

    val features = ml.features

    val indexer = features.indexer()

    indexer s2i("movielens_ratings", "user_id")
    indexer s2i("movielens_ratings", "item_id")


    sql ==> (
      """
        |select
        |user_id_index user_id,
        |item_id_index item_id,
        |rating,ts
        |from movielens_ratings limit 10
      """.stripMargin, "movielens_ratings")

    sql show "movielens_ratings"

    indexer i2s("movielens_ratings", "user_id")
    indexer i2s("movielens_ratings", "item_id")

    sql desc "movielens_ratings"

    sql show "movielens_ratings"

  }

  @Test
  @DisplayName("测试lfm")
  def ts_re_lfm(): Unit = {
    DBS.movielens_ratings(sql)

    sql show "movielens_ratings"

    val recommendation = ml.recommendation

    val lfm = recommendation.lfm(
      is_prediction = true
    )

    lfm ==> "movielens_ratings"

    lfm recommend4users(5, "re4users")

    sql desc "re4users"

    sql show "re4users"

    val model = lfm.model

    model.userFactors.show(false)
    model.itemFactors.show(false)

  }

  @Test
  @DisplayName("测试lfm [normalize flat]")
  def ts_re_lfm_nf(): Unit = {
    DBS.movielens_ratings(sql)

    sql show "movielens_ratings"

    val recommendation = ml.recommendation

    val lfm = recommendation.lfm(
      is_prediction = true,
      implicit_prefs = true
    )

    lfm ==> "movielens_ratings"

    lfm recommend4users(5, "re4users", normalize_flat = true)
    lfm recommend4items(5, "re4items", normalize_flat = true)

    sql desc "re4users"

    sql show "re4users"
    sql show "re4items"

    val model = lfm.model

    model.userFactors.show(false)
    model.itemFactors.show(false)

  }

  @Test
  @DisplayName("测试需要做indexer处理的lfm")
  def ts_re_lfm_indexer(): Unit = {
    DBS.movielens_ratings(sql)

    sql ==> (
      """
        |select
        |concat('u-',user_id) user_id,
        |concat('i-',item_id) item_id,
        |rating,ts
        |from movielens_ratings
      """.stripMargin, "movielens_ratings")

    sql show "movielens_ratings"

    val indexer = ml.features.indexer()

    val lus = indexer.s2i("movielens_ratings", "user_id").labels
    val lis = indexer.s2i("movielens_ratings", "item_id").labels

    sql show "movielens_ratings"

    val recommendation = ml.recommendation

    val lfm = recommendation.lfm(
      is_prediction = true,
      user_col = "user_id_index",
      item_col = "item_id_index"
    )

    lfm ==> "movielens_ratings"

    val spark = ml.spark

    import spark.implicits._

    val df = lfm.recommend4users(5).flatMap(r => {
      r.getAs[Seq[Row]](1).map(re => (r.getAs[Int](0), re.getAs[Int](0), re.getAs[Float](1)))
    }).toDF("user_id_index", "item_id_index", "degree")

    sql register(df, "re4users")

    indexer i2s("re4users", "user_id_index", "user_id", labels = lus)
    indexer i2s("re4users", "item_id_index", "item_id", labels = lis)

    sql col_drop("re4users", "user_id_index", "item_id_index")

    sql col_select("re4users", "user_id user", "item_id item", "degree")

    sql show "re4users"

  }

  @Test
  @DisplayName("测试mix_max")
  def ts_scaler_mix_max(): Unit = {

    val features = ml.features

    DBS.movielens_ratings(sql)
    val df = sql ==> ("select concat('u-',user_id) uid,rand() rd," +
      "user_id f1,item_id f2,rating-2 f3,ts f4 from movielens_ratings")
    sql register(df.orderBy("rd").limit(10), "ia_tb")

    sql show "ia_tb"
    features.scaler("ia_tb", Seq("f1", "f2", "f3")).mix_max
    sql show "ia_tb"
    features.scaler("ia_tb", Seq("f4", "rd"), drop = false, replace = false).mix_max
    sql show "ia_tb"

  }

  @Test
  @DisplayName("测试z_score")
  def ts_scaler_z_score(): Unit = {

    val features = ml.features

    DBS.movielens_ratings(sql)
    val df = sql ==> ("select concat('u-',user_id) uid,rand() rd," +
      "user_id f1,item_id f2,rating-2 f3,ts*rand() f4 from movielens_ratings")
    sql register(df.orderBy("rd").limit(10), "ia_tb")

    sql show "ia_tb"
    features.scaler("ia_tb", Seq("f1", "f2", "f3")).z_score(with_std = true,with_mean = true)
    sql show "ia_tb"
    features.scaler("ia_tb", Seq("f4", "rd"), drop = false, replace = false).z_score
    sql show "ia_tb"

  }

  @Test
  @DisplayName("测试z_score")
  def ts_scaler_max_abs(): Unit = {

    val features = ml.features

    DBS.movielens_ratings(sql)
    val df = sql ==> ("select concat('u-',user_id) uid,rand() rd," +
      "user_id f1,item_id f2,rating-2 f3,ts*rand() f4 from movielens_ratings")
    sql register(df.orderBy("rd").limit(10), "ia_tb")

    sql show "ia_tb"
    features.scaler("ia_tb", Seq("f1", "f2", "f3")).max_abs
    sql show "ia_tb"
    features.scaler("ia_tb", Seq("f4", "rd"), drop = false, replace = false).max_abs
    sql show "ia_tb"

  }

  @AfterEach
  def after(): Unit = {}

  @AfterAll
  def after_all(): Unit = {
    sql.uncache_all()
    sql.stop()
  }
}