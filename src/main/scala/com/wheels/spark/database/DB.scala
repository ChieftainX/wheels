package com.wheels.spark.database

import java.util

import com.wheels.exception.IllegalParamException
import com.wheels.spark.SQL
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.{MRJobConfig, OutputFormat}
import redis.clients.jedis.{HostAndPort, JedisCluster}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class DB(sql: SQL) {

  def spark: SparkSession = sql.spark

  case class redis(
                    nodes: Seq[(String, Int)],
                    key_col: String = "k",
                    value_col: String = "v",
                    life_seconds: Int = -1,
                    timeout: Int = 10000,
                    max_attempts: Int = 3,
                    pwd: String = null,
                    batch: Int = 20
                  ) {

    def <==(input: String): Unit = dataframe(sql view input)

    def dataframe(df: DataFrame): Unit = {
      val nodes_ = new util.HashSet[HostAndPort]()
      val timeout_ = timeout
      val max_attempts_ = max_attempts
      val pwd_ = pwd
      val life_seconds_ = life_seconds
      nodes.map(kv => new HostAndPort(kv._1, kv._2)).foreach(nodes_.add)
      df.select(key_col, value_col).coalesce(batch).foreachPartition(rs => {
        var jedis: JedisCluster = null
        val is_forever = life_seconds_ <= 0
        try {
          jedis = new JedisCluster(nodes_, timeout_, timeout_, max_attempts_, pwd_, new GenericObjectPoolConfig())
          while (rs.hasNext) {
            val r = rs.next()
            val k = r.get(0).toString
            val v = r.get(1).toString
            if (is_forever) jedis.set(k, v)
            else jedis.setex(k, life_seconds_, v)
          }
        } catch {
          case e: Exception =>
            throw e
        } finally {
          if (jedis ne null) jedis.close()
        }
      })

    }
  }

  /***
    * @param hbase_zookeeper_quorum zk地址串，多个地址使用英文逗号分隔
    * @param port zk端口好
    * @param rk_col row key 所对应的列名，默认为rk
    * @param family_name 列族名称，默认为cf
    * @param split_keys 预分区字母，默认为0～9，a～f
    * @param overwrite 是否采用完全覆盖写入方式（每次写入前重建表），默认为false
    */
  case class hbase(hbase_zookeeper_quorum: String,
                   port: Int = 2181,
                   rk_col: String = "rk",
                   family_name: String = "cf",
                   split_keys: Seq[String] =
                   Seq("0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
                     "a", "b", "c", "d", "e", "f"),
                   overwrite: Boolean = false) {

    private lazy val sks: Array[Array[Byte]] = split_keys.map(Bytes.toBytes).toArray

    private lazy val family: Array[Byte] = Bytes.toBytes(family_name)

    private def save_job(table: String): Configuration = {
      conf.setClass(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR,
        classOf[TableOutputFormat[_]], classOf[OutputFormat[_, _]])
      conf.set(TableOutputFormat.OUTPUT_TABLE, table)
      conf
    }

    private def init(table: String): Unit = {
      var conn: Connection = null
      var admin: Admin = null
      try {
        conn = create_conn
        admin = conn.getAdmin
        val tn = TableName.valueOf(table)
        val desc = new HTableDescriptor(tn)
        desc.addFamily(new HColumnDescriptor(family))
        val is_exist = admin.tableExists(tn)
        if (overwrite && is_exist) {
          admin.disableTable(tn)
          admin.deleteTable(tn)
        }
        if (overwrite || (!is_exist)) admin.createTable(desc, sks)
      } catch {
        case e: Exception =>
          throw e
      } finally {
        if (admin ne null) admin.close()
        if (conn ne null) conn.close()
      }
    }

    private lazy val conf: Configuration = {
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.property.clientPort", port.toString)
      conf.set("hbase.zookeeper.quorum", hbase_zookeeper_quorum)
      conf
    }

    private def create_conn: Connection =
      ConnectionFactory.createConnection(conf)

    def <==(view: String, table: String = null): Unit = {
      val tb = if (table ne null) table else view
      val df = sql view tb
      dataframe(df, tb)
    }

    def dataframe(df: DataFrame, table: String): Unit = {
      val rk_col_ = rk_col
      val family_ = family
      val cols = df.schema.map(_.name)
      if (!cols.contains(rk_col_)) throw IllegalParamException(s"your dataframe has no rk[$rk_col_] column")
      val cols_ = cols.filter(_ ne rk_col_)
      save(df.where(s"$rk_col_ is not null")
        , table, (r: Row) => {
          val put = new Put(Bytes.toBytes(r.get(r.fieldIndex(rk_col_)).toString))
          cols_.foreach(col => {
            val value = r.get(r.fieldIndex(col))
            if (value != null) put.addColumn(family_, Bytes.toBytes(col), Bytes.toBytes(value.toString))
          })
          (new ImmutableBytesWritable(), put)
        })
    }

    private def save(df: DataFrame,
                     table: String,
                     f: Row => (ImmutableBytesWritable, Put)): Unit = {
      init(table)
      df.rdd.map(f).saveAsNewAPIHadoopDataset(save_job(table))
    }
  }

}


