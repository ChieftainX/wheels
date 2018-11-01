package com.wheels.spark.database

import java.util

import com.wheels.spark.SQL
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{HostAndPort, JedisCluster}
import org.apache.spark.sql.{DataFrame, SparkSession}

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

}
