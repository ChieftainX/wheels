package com.zzy.ts.spark.database

import com.wheels.spark.database.DB
import com.wheels.spark.{Core, SQL}
import com.zzy.ts.spark.DBS
import org.junit.jupiter.api._
import org.junit.jupiter.api.TestInstance.Lifecycle


@TestInstance(Lifecycle.PER_CLASS)
@DisplayName("测试Spark-DB模块")
class TS {

  var sql: SQL = _
  var database: DB = _

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

    database = sql.support_database

  }

  @BeforeEach
  def init(): Unit = {}


  @Test
  @DisplayName("测试写集群模式的redis功能(注意开启redis-server)")
  def ts_redis(): Unit = {
    DBS.emp(sql)
    sql ==> (
      """
        |select
        |user_id k,height v
        |from emp
      """.stripMargin, "w2redis")

    sql show "w2redis"

    val redis = database.redis(
      Seq(("127.0.0.1", 6379), ("127.0.0.1", 6381), ("127.0.0.1", 6382)),
      life_seconds = 100 * 60
    )

    redis <== "w2redis"


    val df = sql ==>
      """
        |select
        |user_id v,height k
        |from emp
      """.stripMargin

    database.redis(
      Seq(("127.0.0.1", 6379), ("127.0.0.1", 6381)),
      life_seconds = 10
    ) dataframe df


  }


  @Test
  @DisplayName("测试dataframe -> hbase")
  def ts_save_hbase(): Unit = {
    DBS.emp(sql)

    sql ==> (
      """
        |select user_id rk,height,country,org_id
        |from emp
      """.stripMargin,
      "w2hbase")

    val df = sql ==>
      """
        |select user_id rk,height,country,org_id
        |from emp
      """.stripMargin

    sql show "w2hbase"

    val hbase = database.hbase("127.0.0.1")

    hbase <== "w2hbase"

    hbase dataframe(df, "emp")
  }

  @Test
  @DisplayName("测试dataframe -ex> hbase")
  def ts_ex_save_hbase(): Unit = {
    DBS.emp(sql)

    sql ==> (
      """
        |select user_id rk,height,country,org_id
        |from emp
      """.stripMargin,
      "w2hbase_ex", cache = true)

    val hbase = database.hbase("127.0.0.1", ex_save = true)

    hbase <== "w2hbase_ex"

  }

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