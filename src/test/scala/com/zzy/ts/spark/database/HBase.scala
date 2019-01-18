package com.zzy.ts.spark.database

import com.wheels.spark.database.DB
import com.wheels.spark.{Core, SQL}
import com.zzy.ts.spark.DBS
import org.junit.jupiter.api._
import org.junit.jupiter.api.TestInstance.Lifecycle
import com.wheels.common.Conf.WHEEL_SPARK_SQL_JDBC_SAVE_MODE


@TestInstance(Lifecycle.PER_CLASS)
@DisplayName("测试Spark-DB-hbase模块")
class HBase {

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

  @AfterEach
  def after(): Unit = {}

  @AfterAll
  def after_all(): Unit = {
    sql.uncache_all()
    sql.stop()
  }
}