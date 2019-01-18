package com.zzy.ts.spark.database

import com.wheels.spark.database.DB
import com.wheels.spark.{Core, SQL}
import com.zzy.ts.spark.DBS
import org.junit.jupiter.api._
import com.wheels.common.Conf.WHEEL_SPARK_SQL_JDBC_SAVE_MODE
import org.junit.jupiter.api.TestInstance.Lifecycle

@TestInstance(Lifecycle.PER_CLASS)
@DisplayName("测试Spark-DB-jdbc模块")
class JDBC {
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
  @DisplayName("测试jdbc admin")
  def ts_jdbc_admin(): Unit = {
    val jdbc = database.jdbc("com.mysql.cj.jdbc.Driver", "jdbc:mysql://localhost/wheels", "root")
    val admin = jdbc.admin()
    admin.conn.setAutoCommit(false)
    admin.exe("DROP TABLE IF EXISTS `ts_tb`")
    admin.exe(
      """
        |CREATE TABLE `ts_tb` (
        |  `ID` varchar(32) NOT NULL,
        |  `MSG` text,
        |  PRIMARY KEY (`ID`)
        |) ENGINE=InnoDB DEFAULT CHARSET=utf8
      """.stripMargin)
    admin.close()
  }


  @Test
  @DisplayName("测试jdbc save")
  def ts_jdbc_save(): Unit = {
    DBS.emp(sql)

    val jdbc = database.jdbc("com.mysql.cj.jdbc.Driver", "jdbc:mysql://localhost/wheels", "root")

    jdbc <== "emp"
  }

  @Test
  @DisplayName("测试jdbc read")
  def ts_jdbc_read(): Unit = {

    val jdbc = database.jdbc("com.mysql.cj.jdbc.Driver", "jdbc:mysql://localhost/wheels", "root")
    jdbc ==> "emp"
    sql show "emp"
  }


  @AfterEach
  def after(): Unit = {}

  @AfterAll
  def after_all(): Unit = {
    sql.uncache_all()
    sql.stop()
  }
}
