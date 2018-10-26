package com.zzy.ts.spark

import org.junit.jupiter.api._
import com.zhjy.wheel.spark._
import org.junit.jupiter.api.TestInstance.Lifecycle


/**
  * Created by zzy on 2018/10/25.
  */
@TestInstance(Lifecycle.PER_CLASS)
class TS_CORE_AND_SQL {

  var core: Core = _
  var sql: SQL = _

  @BeforeAll
  def init_all(): Unit = {
    val conf = Map(
      "spark.master" -> "local[*]",
      "zzy.param" -> "fk"
    )

    core = Core(
      conf = conf,
      hive_support = false
    )

    sql = core.support_sql

    val spark = sql.spark
    import spark.implicits._
    val emp = Seq(
      ("u-001", 175, "CN", "o-001"),
      ("u-002", 188, "CN", "o-002"),
      ("u-003", 190, "US", "o-001"),
      ("u-004", 175, "CN", "o-001"),
      ("u-005", 155, "JP", "o-002"),
      ("u-006", 145, "JP", "o-002"),
      ("u-007", 166, "JP", "o-002"),
      ("u-008", 148, "CN", "o-002"),
      ("u-009", 172, "CN", "o-003"),
      ("u-010", 169, "US", "o-003")
    ).toDF("user_id", "height", "country", "org_id")

    sql.view(emp, "emp", cache = true)
  }

  @BeforeEach
  def init(): Unit = {
    println("🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎🍎")
  }

  @Test
  def ts_get_session(): Unit = {
    sql.spark.conf.getAll.foreach {
      case (k, v) =>
        println(s"k is [$k] @@ v is [$v]")
    }
  }

  @Test
  def ts_exe(): Unit = {

    sql.exe("select * from emp").show

    sql.exe(
      """
        |select
        |country,count(1) country_count
        |from emp
        |group by country
      """.stripMargin, "country_agg")

    sql.exe(
      """
        |select
        |org_id,count(1) org_count
        |from emp
        |group by org_id
      """.stripMargin, "org_agg")

    sql.exe(
      """
        |select
        |e.*,c.country_count,o.org_count
        |from emp e
        |inner join country_agg c on e.country = c.country
        |full join org_agg o on o.org_id = e.org_id
        |where e.height > 156
      """.stripMargin).show

  }

  @Test
  def ts_save(): Unit = {

    core.save_view("emp")

    core.save_df(
      sql.exe("select * from emp where height<0"),
      "emp_empty")

    core.save_view("emp")

  }

  @AfterEach
  def after(): Unit = {
    println("🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌🍌")
  }

  @AfterAll
  def after_all(): Unit = {
    sql.core.uncache_all()
    sql.stop()
  }


}
