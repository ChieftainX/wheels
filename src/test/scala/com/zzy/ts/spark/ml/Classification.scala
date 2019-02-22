package com.zzy.ts.spark.ml

import com.wheels.common.Log
import com.wheels.spark.ml.ML
import com.wheels.spark.{Core, SQL}
import org.apache.log4j.Level
import com.zzy.ts.spark.DBS
import org.junit.jupiter.api._
import org.junit.jupiter.api.TestInstance.Lifecycle


@TestInstance(Lifecycle.PER_CLASS)
@DisplayName("测试Spark-Ml-Classification模块")
class Classification {

  var sql: SQL = _
  var ml: ML = _
  var classification: com.wheels.spark.ml.lib.Classification = _

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
    classification = ml.classification
  }

  @BeforeEach
  def init(): Unit = {}


  @Test
  @DisplayName("测试Logistic Regression(binomial)")
  def ts_lr_binomial(): Unit = {
    DBS.sample_libsvm_data(sql)

    sql.random_split("sample_libsvm_data", Array(0.7, 0.3), Array("train", "test"))

    val lr = classification.lr()

    lr ==> "train"
    lr <== "test"

    sql print_schema "test"

    println(s"ACC is ${lr <-- "test"}")

  }

  @Test
  @DisplayName("测试Logistic Regression(classification)")
  def ts_lr_classification(): Unit = {
    DBS.sample_multiclass_classification_data(sql)

    sql.random_split("sample_multiclass_classification_data", Array(0.7, 0.3), Array("train", "test"))

    val lr = classification.lr()

    lr ==> "train"

    val model_path = "ml/lr_classification/"

    lr.save(model_path)

    val new_lr = classification.lr()

    new_lr.load(model_path)

    new_lr <== "test"

    sql.show("test", 1000)

    val acc = new_lr <-- "test"

    println(s"ACC is $acc")

  }

  @AfterEach
  def after(): Unit = {}

  @AfterAll
  def after_all(): Unit = {
    sql.uncache_all()
    sql.stop()
  }
}