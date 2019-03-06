package com.zzy.ts.spark.database

import com.wheels.spark.{Core, SQL}
import com.wheels.spark.database.DB
import com.zzy.ts.spark.DBS
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.{ColumnSchema, Schema, Type}
import org.apache.kudu.client.CreateTableOptions
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api._
import org.junit.jupiter.api.TestInstance.Lifecycle

import scala.collection.JavaConversions.seqAsJavaList


@TestInstance(Lifecycle.PER_CLASS)
@DisplayName("测试Spark-DB-KUDU模块")
class KUDU {

  var sql: SQL = _
  var kudu: DB#kudu = _
  var spark: SparkSession = _

  val master = "u16server.zzy.com:7051"

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

    spark = sql.spark
    kudu = sql.support_database.kudu(master)

  }

  @BeforeEach
  def init(): Unit = {}

  @Test
  @DisplayName("KUDU CLI")
  def ts_kudu_client(): Unit = {

    val cli = kudu.client

    //建表
    def csb(name: String, _type: Type): ColumnSchemaBuilder = new ColumnSchema.ColumnSchemaBuilder(name, _type)

    val cols = Seq(
      csb("id", Type.STRING).key(true),
      csb("height", Type.INT16),
      csb("name", Type.STRING)
    ).map(_.build)
    val table = "user"
    val schema = new Schema(seqAsJavaList(cols))
    val cto = new CreateTableOptions()
    val buckets = 3
    val hash_key = cols.filter(_.isKey).map(_.getName)
    cto.addHashPartitions(seqAsJavaList(hash_key), buckets)
    if (cli.tableExists(table)) cli.deleteTable(table)
    cli.createTable(table, schema, cto)
    cli.close()
  }

  @Test
  @DisplayName("KUDU CRUD")
  def ts_kudu_crud(): Unit = {
    DBS.emp(sql)

    val table = "emp"
    sql show table

    //测试数据
    val delete_emp = sql ==>
      """
        |select
        |user_id
        |from emp
        |where org_id='o-001'
      """.stripMargin
    val update_emp = sql ==>
      """
        |select
        |user_id,
        |concat('C-',country) country
        |from emp
        |where org_id<>'o-001'
      """.stripMargin

    val kc = kudu.context
    if (kc.tableExists(table)) kc.deleteTable(table)

    //更新&插入
    kudu <== table
    //删
    kudu.delete(delete_emp, table)
    //更新
    kudu.update(update_emp, table)
    //读
    kudu ==> "emp"

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
