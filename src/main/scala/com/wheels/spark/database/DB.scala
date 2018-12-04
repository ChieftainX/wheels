package com.wheels.spark.database

import java.util

import com.wheels.exception.IllegalParamException
import com.wheels.spark.SQL
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, KeyValue, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.{Job, MRJobConfig, OutputFormat}
import redis.clients.jedis.{HostAndPort, JedisCluster}
import java.util.Properties

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class DB(sql: SQL) {

  def spark: SparkSession = sql.spark

  /**
    * redis 配置项
    *
    * @param nodes        redis集群地址及端口
    * @param key_col      待写入的key对应的列，默认为k
    * @param value_col    待写入的value对应的列，默认为v
    * @param life_seconds 待写入数据的生命周期，默认为不过期
    * @param timeout      连接redis超时时间
    * @param max_attempts 最大重试次数
    * @param pwd          redis 秘钥
    * @param batch        写入数据批次，默认20
    */
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

  /** *
    * hbase 配置项
    *
    * @param hbase_zookeeper_quorum zk地址串，多个地址使用英文逗号分隔
    * @param port                   zk端口好
    * @param rk_col                 row key 所对应的列名，默认为rk
    * @param family_name            列族名称，默认为cf
    * @param split_keys             预分区字母，默认为0～9，a～f
    * @param overwrite              是否采用完全覆盖写入方式（每次写入前重建表），默认为false
    */
  case class hbase(hbase_zookeeper_quorum: String,
                   port: Int = 2181,
                   rk_col: String = "rk",
                   family_name: String = "cf",
                   split_keys: Seq[String] =
                   Seq("0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
                     "a", "b", "c", "d", "e", "f"),
                   ex_save: Boolean = false,
                   ex_hdfs_site: String = null,
                   ex_hdfs_uri: String = null,
                   ex_save_dir: String = "wheels-database-hbase-temp",
                   overwrite: Boolean = false) {

    private lazy val sks: Array[Array[Byte]] = split_keys.map(Bytes.toBytes).toArray

    private lazy val family: Array[Byte] = Bytes.toBytes(family_name)

    private def save_job(table: String): Configuration = {
      val conf = get_conf
      conf.setClass(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR,
        classOf[TableOutputFormat[_]], classOf[OutputFormat[_, _]])
      conf.set(TableOutputFormat.OUTPUT_TABLE, table)
      conf
    }

    private def save_job_ex(table: String): Job = {
      val conf = get_conf
      conf.set(TableOutputFormat.OUTPUT_TABLE, table)
      conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 10010)
      if (ex_hdfs_site ne null) {
        if (ex_hdfs_uri ne null) conf.set(FileSystem.FS_DEFAULT_NAME_KEY, ex_hdfs_uri)
        conf.addResource(ex_hdfs_site)
      }
      Job.getInstance(conf)
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

    private def get_conf: Configuration = {
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.property.clientPort", port.toString)
      conf.set("hbase.zookeeper.quorum", hbase_zookeeper_quorum)
      conf
    }

    private def create_conn: Connection =
      ConnectionFactory.createConnection(get_conf)

    def <==(view: String, table: String = null): Unit = {
      val tb = if (table ne null) table else view
      val df = sql view view
      dataframe(df, tb)
    }

    def dataframe(df: DataFrame, table: String): Unit = {
      val rk_col_ = rk_col
      val family_ = family
      val cols = df.schema.map(_.name)
      if (!cols.contains(rk_col_)) throw IllegalParamException(s"your dataframe has no rk[$rk_col_] column")
      val cols_ = cols.filter(_ ne rk_col_)
      val input = df.where(s"$rk_col_ is not null")
      if (ex_save) {
        save_ex(input.sort(rk_col_), table, (r: Row) => {
          val rk = Bytes.toBytes(r.get(r.fieldIndex(rk_col_)).toString)
          cols_.sorted.map(col => {
            val value = r.get(r.fieldIndex(col))
            val kv = if (value == null) null else new KeyValue(rk, family_,
              Bytes.toBytes(col), Bytes.toBytes(value.toString))
            (new ImmutableBytesWritable(), kv)
          }).filterNot(_._2 == null)
        })
      } else {
        save(input
          , table, (r: Row) => {
            val put = new Put(Bytes.toBytes(r.get(r.fieldIndex(rk_col_)).toString))
            cols_.foreach(col => {
              val value = r.get(r.fieldIndex(col))
              if (value != null) put.addColumn(family_, Bytes.toBytes(col), Bytes.toBytes(value.toString))
            })
            (new ImmutableBytesWritable(), put)
          })
      }

    }

    private def save(df: DataFrame,
                     table: String,
                     f: Row => (ImmutableBytesWritable, Put)): Unit = {
      init(table)
      df.rdd.map(f).saveAsNewAPIHadoopDataset(save_job(table))
    }

    private def save_ex(df: DataFrame,
                        table: String,
                        f: Row => Seq[(ImmutableBytesWritable, KeyValue)]): Unit = {
      init(table)
      val job = save_job_ex(table)
      val conf = job.getConfiguration
      val dir = s"$ex_save_dir/$table"
      val path = new Path(dir)
      val fs = path.getFileSystem(conf)
      if (fs.exists(path)) fs.delete(path, true)
      job.getConfiguration.set("mapred.output.dir", dir)
      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setMapOutputValueClass(classOf[KeyValue])
      val conn = create_conn
      val htb = conn.getTable(TableName.valueOf(table))
      HFileOutputFormat2.configureIncrementalLoadMap(job, htb)
      df.rdd.flatMap(f).saveAsNewAPIHadoopDataset(job.getConfiguration)
      new LoadIncrementalHFiles(conf).run(Array(dir, table))
      htb.close()
      conn.close()
    }
  }

  /**
    * 用于0.10+版本的kafka
    *
    * @param servers broker地址，多个用逗号分隔
    * @param topic   topic名称
    */
  case class kafka(servers: String, topic: String) {

    def <==(view: String): Unit = {
      val df = spark.table(view)
      dataframe(df)
    }

    def dataframe(df: DataFrame): Unit = {
      df.toJSON.toDF("value")
        .write.format("kafka")
        .option("kafka.bootstrap.servers", servers)
        .option("topic", topic)
        .save()
    }
  }

  /**
    * 用于低于0.10版本的kafka
    *
    * @param servers broker地址，多个用逗号分隔
    * @param topic   topic名称
    * @param batch   批次
    */
  case class kafka_low(servers: String, topic: String, batch: Int = 10) {

    def <==(view: String): Unit = {
      val df = spark.table(view)
      dataframe(df)
    }

    def dataframe(df: DataFrame): Unit = {
      val output = df.toJSON.cache
      val servers_ = servers
      val topic_ = topic
      output.count
      output.coalesce(batch).foreachPartition(values => {
        val props = new Properties()
        props.put("metadata.broker.list", servers_)
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers_)
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
        var producer: KafkaProducer[String, String] = null
        try {
          producer = new KafkaProducer[String, String](props)
          while (values.hasNext) {
            val value = values.next
            producer.send(new ProducerRecord[String, String](topic_, value))
          }
        } catch {
          case e: Exception =>
            throw e
        } finally {
          if (producer ne null) producer.close()
        }

      })
      output.unpersist

    }
  }

}


