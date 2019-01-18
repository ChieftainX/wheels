package com.zzy.ts.spark.database

import com.wheels.spark.database.DB
import com.wheels.spark.{Core, SQL}
import com.zzy.ts.spark.DBS
import org.junit.jupiter.api._
import org.junit.jupiter.api.TestInstance.Lifecycle
import com.wheels.common.Conf.WHEEL_SPARK_SQL_JDBC_SAVE_MODE


@TestInstance(Lifecycle.PER_CLASS)
@DisplayName("测试Spark-DB-kafka模块")
class Kafka {

  var sql: SQL = _
  var database: DB = _

  @BeforeAll
  def init_all(): Unit = {
    val conf = Map(
      "spark.master" -> "local[*]",
      "zzy.param" -> "fk",
      WHEEL_SPARK_SQL_JDBC_SAVE_MODE -> "append"
    )

    sql = Core(
      conf = conf,
      hive_support = false
    ).support_sql

    database = sql.support_database

  }

  @BeforeEach
  def init(): Unit = {}

  @Test
  @DisplayName("测试dataframe -> kafka[>=0.10]")
  def ts_kafka(): Unit = {
    DBS.emp(sql)

    val kafka = database.kafka(servers = "localhost:9092", "my-topic")

    kafka <== "emp"

  }

  @Test
  @DisplayName("测试dataframe -> kafka[<0.10]")
  def ts_kafka_low(): Unit = {
    DBS.emp(sql)

    val kafka = database.kafka_low(servers = "localhost:9092", "my-topic")

    kafka <== "emp"

  }

  @AfterEach
  def after(): Unit = {}

  @AfterAll
  def after_all(): Unit = {
    sql.uncache_all()
    sql.stop()
  }
}