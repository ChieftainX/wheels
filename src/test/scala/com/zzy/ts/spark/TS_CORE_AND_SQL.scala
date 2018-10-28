package com.zzy.ts.spark

import com.zhjy.wheel.common.Time
import com.zhjy.wheel.exception.RealityTableNotFoundException
import org.junit.jupiter.api._
import com.zhjy.wheel.spark._
import com.zhjy.wheel.spark.Core.partition
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions._
import org.apache.spark.sql.catalog.Catalog
import org.junit.jupiter.api.TestInstance.Lifecycle


/**
  * Created by zzy on 2018/10/25.
  */
@TestInstance(Lifecycle.PER_CLASS)
@DisplayName("测试CORE及SQL模块")
class TS_CORE_AND_SQL {

  var sql: SQL = _
  var catalog: Catalog = _

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

    val spark = sql.spark
    catalog = spark.catalog

    println("current database is " + catalog.currentDatabase)

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
      ("u-010", 167, "US", "o-003")
    ).toDF("user_id", "height", "country", "org_id")

    sql register(emp, "emp", cache = true)
  }

  @BeforeEach
  def init(): Unit = {
    println("before exe:" + catalog.listTables.collect.toSeq)
  }

  @Test
  @DisplayName("测试spark传送参数是否正常")
  def ts_spark_params(): Unit = {
    val cf = sql.spark.conf
    assertEquals("fk", cf.get("zzy.param"))
    cf.getAll.foreach {
      case (k, v) =>
        println(s"k is [$k] @@ v is [$v]")
    }
  }

  @Test
  @DisplayName("测试sql执行")
  def ts_exe(): Unit = {

    sql.view(s"emp").distinct.show()

    sql ==> (
      """
        |select
        |country,count(1) country_count
        |from emp
        |group by country
      """.stripMargin, "country_agg")

    sql ==> (
      """
        |select
        |org_id,count(1) org_count
        |from emp
        |group by org_id
      """.stripMargin, "org_agg")

    sql ==> (
      """
        |select
        |e.*,c.country_count,o.org_count
        |from emp e
        |inner join country_agg c on e.country = c.country
        |full join org_agg o on o.org_id = e.org_id
        |where e.height > 156
      """.stripMargin, "emp_res")

    sql show "emp_res"

  }

  @Test
  @DisplayName("测试保存到hive的功能")
  def ts_save(): Unit = {

    sql show "emp"

    val s1 = sql.hive <== "emp"
    assert(s1 > 0l)

    val s2 = sql.hive.save(
      sql ==> "select * from emp where height<0",
      "emp_empty")
    assertEquals(0l, s2)
    val ct_emp_empty = sql count "emp_empty"
    try {
      sql count("emp_empty", true)
    } catch {
      case e: RealityTableNotFoundException =>
        println(e.msg)
        assertEquals("reality table not found: emp_empty", e.msg)
    }

    println(s"emp count[not reality] : $ct_emp_empty")
    assertEquals(ct_emp_empty, 0l)

    val s3 = sql.hive <== ("emp", save_mode = SaveMode.Append)
    assertEquals(s1, s3)

    val s3_ct_emp = sql count "emp"
    println(s"emp count[not reality] : $s3_ct_emp")
    assertEquals(s3_ct_emp, s1)
    println(s"emp count[reality] : ${sql count("emp", true)}")

    val s4 = sql.hive <== ("emp",
      save_mode = SaveMode.Append,
      refresh_view = true)
    assertEquals(s1 + s3 + s4, sql count "emp")

    sql show("emp", 100)

  }

  @Test
  @DisplayName("测试partition功能")
  def ts_partition(): Unit = {
    val p1 = partition("y", "m", "d") + ("2018", "08", "12") + ("2018", "08", "17") + ("2018", "08", "17")
    println(p1)
    println(p1.values)
    assertEquals(2, p1.values.length)

    val p2 = partition("y", "m").table_init
    p2 ++ Time.all_month1year("2018").map(_.split("-").toSeq)
    println(p2.values)
    assertEquals(true, p2.is_init)

    val p3 = partition("country", "org_id").table_init
    p3 + ("CN", "o-001") + ("CN", "o-002") + ("CN", "o-002") + ("JP", "o-002") + ("US", "o-003")

    sql.hive <== ("emp", "emp_p", p = p3)

    val p4 = partition("country").table_init

    sql.hive <== ("emp", "emp_ap", p = p4)

    sql show "emp_ap"

  }

  @AfterEach
  def after(): Unit = {
    println("after exe:" + catalog.listTables.collect.toSeq)
  }

  @AfterAll
  def after_all(): Unit = {
    sql.uncache_all()
    sql.stop()
  }


}
