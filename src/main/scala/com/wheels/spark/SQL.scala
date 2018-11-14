package com.wheels.spark

import java.util.Locale

import com.wheels.common.Log
import com.wheels.exception.{IllegalConfException, IllegalParamException, RealityTableNotFoundException}
import com.wheels.spark.database.DB
import com.wheels.spark.ml.ML
import org.apache.log4j.Logger
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col, lit}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

/**
  * Created by zzy on 2018/10/25.
  */
class SQL(spark: SparkSession) extends Core(spark) {

  import SQL._

  def support_ml: ML = new ML(this)

  def support_database: DB = new DB(this)

  private def save_mode: SaveMode = {
    val mode = spark.conf.get("wheel.spark.sql.hive.save.mode")
    mode.toLowerCase(Locale.ROOT) match {
      case "overwrite" => SaveMode.Overwrite
      case "append" => SaveMode.Append
      case "ignore" => SaveMode.Ignore
      case "error" | "default" => SaveMode.ErrorIfExists
      case _ => throw IllegalConfException(s"unknown save mode: $mode." +
        s"accepted save modes are 'overwrite', 'append', 'ignore', 'error'.")
    }
  }

  /**
    * 使用sql进行数据处理
    *
    * @param sql   待执行的sql字符串
    * @param view  执行结果的视图名称，若未填入则不注册视图
    * @param cache 是否写入缓存
    * @param level 写入缓存的级别
    * @return dataframe对象
    */
  def ==>(sql: String, view: String = null,
          cache: Boolean = false,
          level: StorageLevel = StorageLevel.MEMORY_AND_DISK): DataFrame = {
    log.info(s"register ${
      if (view eq null) "no view" else s"view[$view]"
    } sql:$sql")
    val df = spark.sql(sql)
    if (cache) df.persist(level)
    if (view ne null) df.createOrReplaceTempView(view)
    df
  }

  private def format_source: String = spark.conf.get("wheel.spark.sql.hive.save.format")

  private def coalesce_limit: Long = spark.conf.get("wheel.spark.sql.hive.save.file.lines.limit").toLong

  private def refresh_view: Boolean = spark.conf.get("wheel.spark.sql.hive.save.refresh.view").toBoolean

  /**
    * 视图写入hive
    *
    * @param view           视图名称
    * @param table          待写入hive表名称，默认为视图名称
    * @param p              分区表配置对象，默认为写入非分区表
    * @param save_mode      数据入库模式(overwrite:覆盖，append：追加，ignore：若存在则跳过写入，error：若存在则报错)
    * @param format_source  写入数据格式(parquet,orc,csv,json)
    * @param coalesce_limit 写入文件最大行数限制，用于预防小文件产生
    * @param refresh_view   数据写入后是否刷新视图
    * @return 写入数据的行数
    */
  def <==(view: String, table: String = null,
          p: partition = null,
          save_mode: SaveMode = save_mode,
          format_source: String = format_source,
          coalesce_limit: Long = coalesce_limit,
          refresh_view: Boolean = refresh_view): Long = {
    val df = this view view
    val tb = if (table ne null) table else view
    save(df, tb, p, save_mode, format_source, coalesce_limit, refresh_view)
  }

  /**
    * dataframe对象注册到视图
    *
    * @param df    待注册dataframe
    * @param view  视图名称
    * @param cache 是否写入缓存
    * @param level 写入缓存的级别
    * @return dataframe对象
    */
  def register(df: DataFrame, view: String,
               cache: Boolean = false,
               level: StorageLevel = StorageLevel.MEMORY_AND_DISK): DataFrame = {
    if (cache) df.persist(level)
    df.createOrReplaceTempView(view)
    log.info(s"dataframe register view[$view]")
    df
  }

  /**
    * 读取表，并注册为视图
    *
    * @param table   待读取表的名称
    * @param reality 是否读取真实表
    * @param cache   是否写入缓存
    * @param level   写入缓存的级别
    * @return dataframe对象
    */
  def read(table: String,
           reality: Boolean = true,
           cache: Boolean = false,
           level: StorageLevel = StorageLevel.MEMORY_AND_DISK): DataFrame = {
    if (reality) catalog.dropTempView(table)
    val df = {
      var df: DataFrame = null
      try {
        df = spark.table(table)
      } catch {
        case _: org.apache.spark.sql.AnalysisException =>
          throw RealityTableNotFoundException(s"reality table not found: $table")
      }
      df
    }
    if (cache) df.persist(level)
    df.createOrReplaceTempView(table)
    df
  }

  /**
    * 获取视图
    *
    * @param view 视图名称
    * @return dataframe对象
    */
  def view(view: String): DataFrame = spark.table(view)

  /**
    * 获取表/视图的行数
    *
    * @param table   待读取表的名称
    * @param reality 是否读取真实表
    * @return 表/视图的行数
    */
  def count(table: String, reality: Boolean = false): Long = {
    if (reality) this.read(table).count
    else spark.table(table).count
  }

  /**
    * 预览表的数据
    *
    * @param view     视图名称
    * @param limit    预览的行数
    * @param truncate 是否简化输出结果
    * @param reality  是否读取真实表
    */
  def show(view: String, limit: Int = 20, truncate: Boolean = false,
           reality: Boolean = false): Unit = {
    val df = if (reality) this.read(view) else spark.table(view)
    df.show(limit, truncate)
  }

  /**
    * 预览表结构
    *
    * @param view 视图名称
    */
  def desc(view: String): Unit = spark.table(view).printSchema()


  /**
    * 列重命名
    *
    * @param view 视图名称
    * @param o    原始列名
    * @param n    新列名
    */
  def col_rename(view: String, o: String, n: String): Unit =
    spark.table(view).withColumnRenamed(o, n).createOrReplaceTempView(view)

  /**
    * 列重命名（批量）
    *
    * @param view 视图名称
    * @param onm  新老列名映射关系
    */
  def col_rename(view: String, onm: Map[String, String]): Unit = {
    var df = spark.table(view)
    onm.foreach(m => {
      df = df.withColumnRenamed(m._1, m._2)
    })
    df.createOrReplaceTempView(view)
  }

  /**
    * 删除列
    *
    * @param view 视图名称
    * @param cols 要刪除的列
    */
  def col_drop(view: String, cols: String*): Unit =
    spark.table(view).drop(cols: _*).createOrReplaceTempView(view)

  /**
    * 选取指定的列
    *
    * @param view 视图名称
    * @param cols 要选择的列
    */
  def col_select(view: String, cols: String*): Unit =
    spark.table(view).selectExpr(cols: _*).createOrReplaceTempView(view)


  /**
    * 将视图写入hive
    *
    * @param df             待保存dataframe
    * @param table          待写入hive表名称，默认为视图名称
    * @param p              分区表配置对象，默认为写入非分区表
    * @param save_mode      数据入库模式(overwrite:覆盖，append：追加，ignore：若存在则跳过写入，error：若存在则报错)
    * @param format_source  写入数据格式(parquet,orc,csv,json)
    * @param coalesce_limit 写入文件最大行数限制，用于预防小文件产生
    * @param refresh_view   数据写入后是否刷新视图
    * @return 写入数据的行数
    */
  def save(df: DataFrame, table: String,
           p: partition = null,
           save_mode: SaveMode = save_mode,
           format_source: String = format_source,
           coalesce_limit: Long = coalesce_limit,
           refresh_view: Boolean = refresh_view): Long = {
    catalog.dropTempView(table)
    log.info(s"$table[save mode:$save_mode,format source:$format_source] will be save")
    log.info(s"schema is:${df.schema}")
    if (df.storageLevel eq StorageLevel.NONE) df.cache
    val ct = df.count
    ct match {
      case 0l =>
        log.warn(s"$table is empty,skip save")
      case _ =>
        log.info(s"$table length is $ct,begin save")
        if (p eq null) {
          val coalesce_num = (1 + ct / coalesce_limit).toInt
          val writer = df.coalesce(coalesce_num).write
          save_mode match {
            case SaveMode.Append =>
              writer.insertInto(table)
            case _ =>
              writer
                .mode(save_mode).format(format_source)
                .saveAsTable(table)
          }
          log.info(s"$table[$coalesce_num flies] is saved")
        } else {
          if (p.values.isEmpty) {
            p ++ df.select(p.col.map(col): _*).distinct.collect
              .map(r => p.col.map(r.getAs[String]))
          }
          val cols = (df.columns.filterNot(p.col.contains) ++ p.col).map(col)
          val pdf = df.select(cols: _*)
          var is_init = p.is_init
          log.info(s"$table is partition table[init:$is_init],will run ${p.values.length} batch")
          p.values.map(v => v.map(s => s"'$s'")).map(v => v.zip(p.col)
            .map(s => s"${s._2}=${s._1}")).foreach(ps => {
            val pdf_ = pdf.where(ps.mkString(" and ")).cache
            val ct_ = pdf_.count
            val coalesce_num = (1 + ct_ / coalesce_limit).toInt
            val writer = pdf_.coalesce(coalesce_num).write
            if (is_init) {
              writer.mode(save_mode)
                .format(format_source)
                .partitionBy(p.col: _*)
                .saveAsTable(table)
              is_init = false
            }
            else {
              spark.sql(s"alter table $table drop if exists partition (${ps.mkString(",")})")
              writer.insertInto(table)
            }
            log.info(s"$table's partition[$ps] is saved,count:$ct_,file number:$coalesce_num")
            pdf_.unpersist
          })
        }
    }
    df.unpersist
    if (refresh_view && ct > 0l) this read table
    this register(df, table)
    ct
  }

  /**
    * 缓存dataframe
    *
    * @param df  待缓存dataframe
    * @param dfs 支持批量缓存
    */
  def cache(df: DataFrame, dfs: DataFrame*): Unit = {
    log.info("1 dataframe will be cache")
    df.cache
    if (dfs.nonEmpty) {
      log.info(s"${dfs.length} dataframe will be cache")
      dfs.foreach(_.cache)
    }

  }

  /**
    * 缓存视图
    *
    * @param view 视图名称
    */
  def cache(view: String*): Unit = {
    view.foreach(v => {
      log.info(s"$view will be cache")
      catalog.cacheTable(v)
    })
  }

  /**
    * 释放dataframe缓存
    *
    * @param df  dataframe
    * @param dfs 支持批量释放
    */
  def uncache(df: DataFrame, dfs: DataFrame*): Unit = {
    log.info("1 dataframe will be cleared")
    df.unpersist
    log.info("1 dataframe is cleared")
    log.info(s"${dfs.length} dataframe will be cleared")
    dfs.foreach(_.unpersist)
    log.info(s"${dfs.length} dataframe is cleared")
  }

  /**
    * 释放视图缓存
    *
    * @param view 视图名称
    */
  def uncache(view: String*): Unit = {
    view.foreach(v => {
      log.info(s"$view will be cleared")
      catalog.uncacheTable(v)
      log.info(s"$view is cleared")
    })
  }

  /**
    * 释放全部缓存
    */
  def uncache_all(): Unit = {
    log.info("all cache will be cleared")
    catalog.clearCache
    log.info("all cache is cleared")
  }

  def super_join(bigger_view: String, smaller_view: String, join_cols: Seq[String],
                 join_type: String = "inner",
                 output_view: String = "wheels_super_join_res",
                 deal_ct: Int = 10000,
                 deal_limit: Int = 1000,
                 bigger_clv: StorageLevel = StorageLevel.MEMORY_AND_DISK): DataFrame = {

    log.info(s"$bigger_view $join_type super join $smaller_view" +
      s" with cols[${join_cols.mkString(",")}] powered by wheels")
    log.info("begin analyze ......")

    val bigger = this view bigger_view
    val smaller = this view smaller_view

    var bigger_ct = 0l
    var smaller_ct = 0l
    val bigger_mark = "wheels_super_join_bigger_mark"
    val smaller_mark = "wheels_super_join_smaller_mark"

    join_type.toLowerCase(Locale.ROOT) match {
      case "inner" =>
        this register(
          aftercure(product.where(s"$bigger_mark=1 and $smaller_mark=1"))
          , output_view)
      case "left" =>
        this register(
          aftercure(product.where(s"$bigger_mark=1"))
          , output_view)
      case "full" =>
        this register(aftercure(product), output_view)
      case _ =>
        throw IllegalParamException(s"your $join_type not support ! only support [inner,left,full]")
    }

    def product: DataFrame = {
      val bsn = bigger.schema.map(_.name)
      val ssn = smaller.schema.map(_.name)
      join_cols.foreach(c => {
        if (!bsn.contains(c)) throw IllegalParamException(s"$c not in $bigger_view[${bsn.mkString(",")}]")
        if (!ssn.contains(c)) throw IllegalParamException(s"$c not in $smaller_view[${ssn.mkString(",")}]")
      })
      if (bigger.storageLevel eq StorageLevel.NONE) bigger.persist(bigger_clv)
      if (smaller.storageLevel eq StorageLevel.NONE) smaller.cache
      bigger_ct = bigger.count
      smaller_ct = smaller.count
      log.info(s"$bigger_view[$bigger_ct * ${bsn.length}] $join_type super join" +
        s" $smaller_view[$smaller_ct * ${ssn.length}]")
      if (smaller_ct <= deal_limit) {
        log.info(s"smaller[$smaller_ct] <= deal limit[$deal_limit], smaller overfly")
        bigger.withColumn(bigger_mark, lit(1))
          .join(broadcast(smaller).withColumn(smaller_mark, lit(1)), join_cols, "outer")
      } else {
        log.info(s"smaller[$smaller_ct] > deal limit[$deal_limit]")
        val jcs = join_cols.map(col)
        val ct_col = "count"
        val mark_ = bigger.groupBy(jcs: _*).count
          .where(s"$ct_col > $deal_ct")
          .sort(col(ct_col).desc)
          .limit(deal_limit)
          .withColumn(smaller_mark, lit(1)).cache
        val mark_ct = mark_.count
        log.info(s"[mark: $mark_ct | $deal_limit] overfly")
        val tp_10 = mark_.sort(col(ct_col).desc).limit(10).collect.map(_.getAs[Long](ct_col)).mkString(",")
        val mark_min = mark_.sort(col(ct_col)).first.getAs[Long](ct_col)
        log.info(s"[top 10: $tp_10 | min: $mark_min | $deal_ct]")
        val mark = mark_.drop(ct_col)
        val mark_bc = broadcast(mark)
        val smaller_ = smaller.join(mark_bc, join_cols, "left")
        val smaller_p0 = smaller_.where(s"$smaller_mark is null")
          .withColumn(smaller_mark, lit(1))
        val smaller_p1 = smaller_.where(s"$smaller_mark = 1")
        val bigger_ = bigger.withColumn(bigger_mark, lit(1)).join(mark_bc, join_cols, "outer")
        val bigger_p0 = bigger_.where(s"$smaller_mark is null").drop(smaller_mark)
        val bigger_p1 = bigger_.where(s"$smaller_mark = 1").drop(smaller_mark)
        val product_p0 = bigger_p0.join(smaller_p0, join_cols, "outer")
        val product_p1 = bigger_p1.join(broadcast(smaller_p1), join_cols, "outer")
        val product = product_p0.union(product_p1)
        product
      }
    }

    def aftercure(df: DataFrame): DataFrame = df.drop(smaller_mark, bigger_mark)

    this view output_view
  }

  case class functions(spark: SparkSession) {
    val to_vector: UserDefinedFunction =
      spark.udf.register("to_vector", (cols: Seq[Double]) => Vectors.dense(cols.toArray))
    val arrays_hits: UserDefinedFunction =
      spark.udf.register("arrays_hits", (bigger: Seq[String], smaller: Seq[String]) => {
      smaller.exists(bigger.contains)
    })
    val vector2array: UserDefinedFunction =
      spark.udf.register("vector2array", (v: DenseVector) => v.values.toSeq)

  }

  val udf: functions = functions(spark)

}

object SQL {

  def apply(spark: SparkSession): SQL = new SQL(spark)

  lazy val log: Logger = Log.get("wheel>spark>sql")

  /**
    * 分区配置
    *
    * @param col 列名
    */
  case class partition(col: String*) {

    private var vals = new ListBuffer[Seq[String]]

    // 是否初始化表
    var is_init = false

    /**
      * 设置表为初始化
      *
      * @return this
      */
    def table_init: this.type = {
      is_init = true
      this
    }

    /**
      * 获取待分区的列的值
      *
      * @return Seq((分区值1，分区值2，分区值3...分区值n))
      */
    def values: Seq[Seq[String]] = {
      val cl = col.length
      vals.result().map(v => {
        param_verify(cl, v.length)
        v
      }).distinct
    }

    /**
      * 添加分区的值
      *
      * @param value (分区值1，分区值2，分区值3...分区值n)
      * @return this
      */
    def +(value: String*): this.type = {
      vals += value
      this
    }

    /**
      * 批量添加分区的值
      *
      * @param values Seq((分区值1，分区值2，分区值3...分区值n))
      * @return this
      */
    def ++(values: Seq[Seq[String]]): this.type = {
      vals ++= values
      this
    }

    private def param_verify(cl: Int, vl: Int): Unit = {
      if (cl != vl) throw IllegalParamException("partition column length not equal value length")
    }
  }

  /**
    * 将指定列聚合为json
    *
    * @param cols 列名
    */
  def collect_json(cols: Seq[String], alias: String = null): String =
    s"to_json(collect_set(struct(${cols_str(cols)}))) ${set_alias(alias)}"

  /**
    * 将指定列聚合为json
    *
    * @param col 列名
    */
  def collect_json(col: String*): String =
    s"to_json(collect_set(struct(${cols_str(col)})))"

  def to_vector(cols: Seq[String], alias: String = null): String =
    s"to_vector(array(${cols_str(cols, "double")})) ${set_alias(alias)}"

  private val DEFAULT_COL_NAME = "wheels_col"

  private def set_alias(alias: String): String = if (alias eq null) DEFAULT_COL_NAME else s"`$alias`"

  private def col_escape(col: String): String = s"`$col`"

  private def cols_escape(cols: Seq[String]): Seq[String] = cols.map(col_escape)

  private def cols_str(cols: Seq[String], tp: String = null): String = {
    val ces = cols_escape(cols)
    val cols_ = if (tp eq null) ces else ces.map(c => s"cast($c as $tp)")
    cols_.mkString(",")
  }

  lazy val WHEELS_INPUT_COL = "wheels_input_col"
  lazy val WHEELS_OUTPUT_COL = "wheels_output_col"
  lazy val WHEELS_TMP_COL = "wheels_tmp_col"


}
