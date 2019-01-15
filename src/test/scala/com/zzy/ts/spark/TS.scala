package com.zzy.ts.spark

import com.wheels.common.Time
import com.wheels.spark.{Core, SQL}
import com.wheels.exception.RealityTableNotFoundException
import org.junit.jupiter.api._
import org.apache.spark.sql.{Row, SaveMode}
import org.junit.jupiter.api.Assertions._
import org.apache.spark.sql.catalog.Catalog
import org.junit.jupiter.api.TestInstance.Lifecycle
import com.wheels.spark.SQL._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.storage.StorageLevel

/**
  * Created by zzy on 2018/10/25.
  */
@TestInstance(Lifecycle.PER_CLASS)
@DisplayName("测试Spark模块")
class TS {

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
  @DisplayName("测试数据转换[sql 方式]")
  def ts_exe_sql(): Unit = {

    DBS.emp(sql)

    sql show "emp"

    sql ==> (
      """
        select
        country,count(1) country_count
        from emp
        group by country
      """, "tmp_country_agg")

    sql ==> (
      """
        select
        org_id,count(1) org_count
        from emp
        group by org_id
      """, "tmp_org_agg")

    sql ==> (
      """
        select
        e.*,c.country_count,o.org_count
        from emp e,tmp_country_agg c,tmp_org_agg o
        where
        e.country = c.country and
        o.org_id = e.org_id and
        e.height > 156
      """, "emp_res")

    sql show "emp_res"
  }

  @Test
  @DisplayName("测试数据转换[dataframe 方式]")
  def ts_exe_df(): Unit = {

    DBS.emp(sql)

    sql show "emp"

    val emp = sql view "emp"

    val tmp_country_agg = emp
      .groupBy("country")
      .count()
      .as("country_count")

    val tmp_org_agg = emp
      .groupBy("org_id")
      .count()
      .as("org_count")

    val emp_res = emp
      .join(tmp_country_agg, "country")
      .join(tmp_org_agg, "org_id")
      .where("height > 156")

    emp_res.show(truncate = false)
  }

  @Test
  @DisplayName("测试保存到hive的功能")
  def ts_save(): Unit = {
    DBS.emp(sql)

    sql show "emp"

    val s1 = sql <== "emp"
    assert(s1 > 0l)

    val s2 = sql.save(
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

    val s3 = sql <== ("emp", save_mode = SaveMode.Append)
    assertEquals(s1, s3)

    val s3_ct_emp = sql count "emp"
    println(s"emp count[not reality] : $s3_ct_emp")
    assertEquals(s3_ct_emp, s1)
    println(s"emp count[reality] : ${sql count("emp", true)}")

    val s4 = sql <== ("emp",
      save_mode = SaveMode.Append,
      refresh_view = true)
    assertEquals(s1 + s3 + s4, sql count "emp")

    sql show("emp", 100)

  }

  @Test
  @DisplayName("测试保存同一张表到hive的功能")
  def ts_same_table_save(): Unit = {
    DBS.emp(sql)
    sql <== "emp"
    sql read "emp"
    sql ==> ("select *,1 mk from emp", "emp")

    sql <== ("emp", "emp_tmp")

    sql read "emp_tmp"
    sql <== ("emp_tmp", "emp")

  }

  @Test
  @DisplayName("测试partition功能")
  def ts_partition(): Unit = {
    DBS.emp(sql)
    import com.wheels.spark.SQL.partition

    val p1 = partition("y", "m", "d") + ("2018", "08", "12") + ("2018", "08", "17") + ("2018", "08", "17")
    println(p1)
    println(p1.values)
    assertEquals(2, p1.values.length)

    val p2 = partition("y", "m").table_init
    p2 ++ Time.all_month1year("2018").map(_.split("-").toSeq)
    println(p2.values)
    assertEquals(true, p2.is_init)

    val p3 = partition("country", "org_id").table_init
    p3 + ("CN", "o-001") + ("CN", "o-002") ++ Seq(Seq("CN", "o-002"), Seq("JP", "o-002"), Seq("US", "o-003"))

    sql <== ("emp", "emp_p", p = p3)

    val p4 = partition("country")

    sql <== ("emp", "emp_ap", p = p4)

    sql show "emp_ap"

  }

  @Test
  @DisplayName("测试collect_json功能")
  def ts_2json(): Unit = {
    DBS.emp(sql)
    sql ==> ("select *,123 `from` from emp", "emp")
    sql show "emp"

    sql ==> (
      s"""
         |select
         |org_id,${collect_json("height h", "country", "user_id", "from")} msg
         |from emp
         |group by org_id
      """.stripMargin, "zzy_tb")
    sql show "zzy_tb"
  }

  @Test
  @DisplayName("测试super_join[left]功能")
  def ts_super_join_left(): Unit = {
    DBS.incline_table(sql)

    val user_ct_ = sql count "user_dim"

    sql ==> ("select * from user_dim where user_id > 'u-1'", "user_dim")

    val user_ct = sql count "user_dim"

    val study_ct = sql count "study_record"

    println("study count: " + study_ct)

    sql super_join("study_record", "user_dim", Seq("user_id"), "left",
      output_view = "super_res", deal_limit = 10, deal_ct = 1000)

    sql cache "super_res"

    sql show "super_res"

    val super_ct = sql count "super_res"

    assertEquals(super_ct, study_ct)

    sql ==> ("select * from super_res where user_name is null", "res_null")

    assertEquals(sql count "res_null", user_ct_ - user_ct)
  }

  @Test
  @DisplayName("测试super_join[inner]功能")
  def ts_super_join_inner(): Unit = {
    DBS.incline_table(sql)

    sql ==> ("select * from study_record where user_id <> 'u-80'", "study_record")

    val study_ct = sql count "study_record"

    println("study count: " + study_ct)

    sql super_join("study_record", "user_dim", Seq("user_id"),
      output_view = "super_res", deal_limit = 70, deal_ct = 1000)

    sql cache "super_res"

    sql show "super_res"

    val super_ct = sql count "super_res"

    assertEquals(super_ct, study_ct)

    sql ==> ("select * from super_res where user_name is null or course_id is null", "res_null")

    assertEquals(sql count "res_null", 0l)
  }

  @Test
  @DisplayName("测试super_join[outer]功能")
  def ts_super_join_outer(): Unit = {
    DBS.incline_table(sql)

    sql ==> ("select * from study_record where user_id <> 'u-80'", "study_record")

    val study_ct = sql count "study_record"

    println("study count: " + study_ct)

    sql super_join("study_record", "user_dim", Seq("user_id"), "full",
      output_view = "super_res", deal_limit = 10)

    sql cache "super_res"

    sql show "super_res"

    val super_ct = sql count "super_res"

    assertEquals(super_ct, study_ct + 1)

    sql ==> ("select * from super_res where user_name is null or course_id is null", "res_null")

    assertEquals(sql count "res_null", 1l)
  }

  @Test
  @DisplayName("测试clear view功能")
  def ts_clear_view(): Unit = {
    DBS.emp(sql)
    val df = sql view "emp"
    sql register(df, "emp_0")
    sql register(df, "emp_1")
    sql register(df, "emp_2")
    sql register(df, "emp_3")
    sql register(df, "emp_4")
    println("after exe:" + catalog.listTables.collect.map(_.name).toSeq)

    sql.clear_view("emp_2", "emp_3", "emp_555")

    println("clear:" + catalog.listTables.collect.map(_.name).toSeq)

    assertEquals(catalog.listTables.filter(_.isTemporary).count, 4)

    sql.clear_view("emp")

    assertEquals(catalog.listTables.filter(_.isTemporary).count, 3)

    sql.clear_view()
    assertEquals(catalog.listTables.filter(_.isTemporary).count, 0l)
  }

  @Test
  @DisplayName("测试修复保存后会释放缓存的bug")
  def ts_save_cache(): Unit = {
    DBS.emp(sql)
    sql ==> ("select * from emp", "emp", cache = true)
    sql <== "emp"
    val stl = sql.view("emp").storageLevel
    println(stl)
    assertEquals(stl, StorageLevel.MEMORY_AND_DISK)

  }

  @Test
  @DisplayName("测试to_vector功能")
  def ts_to_vector(): Unit = {
    DBS.emp(sql)
    sql ==> (s"select ${to_vector(Seq("height"))} from emp", "emp_v")
    sql show "emp_v"
    sql ==> (s"select *,1.8 nb1,4 nb2,0 nb3 from emp", "emp")
    sql show "emp"
    sql ==> (s"select *,${to_vector(Seq("nb1", "nb2", "nb3", "height", "user_id"), "vectors")} from emp", "emp_vs")
    sql show "emp_vs"
  }

  @Test
  @DisplayName("测试with_col功能")
  def ts_with_col(): Unit = {
    DBS.emp(sql)
    sql show "emp"
    sql with_col("emp",
      Seq(
        ("org_id", DataTypes.StringType),
        ("age", DataTypes.IntegerType)
      ), r => {
      val c = r.getAs[String]("country")
      val o = r.getAs[String]("org_id")
      val h = r.getAs[Int]("height")
      Row(s"[$c]~[$o]~[", h / 10)
    })
    sql col_rename("emp", Map("user_id" -> "uid", "height" -> "h"))
    sql show "emp"
    sql col_select("emp", "country")
    sql distinct "emp"
    sql show "emp"
  }

  @Test
  @DisplayName("测试arrays_hits功能")
  def ts_arrays_hits(): Unit = {
    sql ==> ("select arrays_hits(array('aa','bb','cc'),array('dd','ee','ff'))", "ht")
    sql show "ht"
    sql ==> ("select arrays_hits(array('aa','bb','cc'),array('dd','aa','ff'))", "ht")
    sql show "ht"
  }

  @Test
  @DisplayName("测试batch功能")
  def ts_batch(): Unit = {
    DBS.movielens_ratings(sql)

    val total_count = sql count "movielens_ratings"

    var total_count_ = 0l

    (sql batch "movielens_ratings").foreach(d => {
      val dt = d.cache.count
      d.show
      d.unpersist
      total_count_ += dt
      println(dt)
    })
    assertEquals(total_count, total_count_)

    println(total_count)
  }

  @AfterEach
  def after(): Unit = {
    println("after exe:" + catalog.listTables.collect.toSeq)
    sql.clear_view()
  }

  @AfterAll
  def after_all(): Unit = {
    sql.uncache_all()
    sql.stop()
  }


}
