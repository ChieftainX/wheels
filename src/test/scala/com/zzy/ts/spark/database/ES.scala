package com.zzy.ts.spark.database

import com.wheels.spark.database.DB
import com.wheels.spark.{Core, SQL}
import com.zzy.ts.spark.DBS
import org.junit.jupiter.api._
import org.junit.jupiter.api.TestInstance.Lifecycle


@TestInstance(Lifecycle.PER_CLASS)
@DisplayName("测试Spark-DB-ES模块")
class ES {

  var sql: SQL = _
  var database: DB = _
  var es: DB#es = _

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

    es = database.es("wheels/zzy")
  }

  @BeforeEach
  def init(): Unit = {}


  @Test
  @DisplayName("es写入功能")
  def ts_es_write(): Unit = {
    DBS.emp(sql)

    sql ==> ("select md5(user_id) sid,e.* from emp e", "emp",cache = true)

    sql show ("emp",1000)

    es <== "emp"


  }

  @AfterEach
  def after(): Unit = {}

  @AfterAll
  def after_all(): Unit = {
    sql.uncache_all()
    sql.stop()
  }
}