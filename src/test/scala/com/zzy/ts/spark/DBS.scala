package com.zzy.ts.spark

import com.wheels.spark.SQL

object DBS {

  def emp(sql: SQL): Unit = {
    import sql.spark.implicits._
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

    sql cache emp

    sql register(emp, "emp")
  }

  def recommend_res(sql: SQL): Unit = {
    import sql.spark.implicits._
    val emp = Seq(
      ("u-001", "i-003", 12.886, "t1"),
      ("u-002", "i-002", 33.886, "t1"),
      ("u-003", "i-001", 77.886, "t1"),
      ("u-004", "i-001", 54.886, "t1"),
      ("u-002", "i-002", 99.886, "t2"),
      ("u-004", "i-001", 22.886, "t2"),
      ("u-001", "i-003", 45.886, "t2"),
      ("u-002", "i-001", 66.886, "t3"),
      ("u-003", "i-003", 0.886, "t3"),
      ("u-004", "i-001", 2.886, "t3")
    ).toDF("user_id", "item_id", "degree", "type")

    sql cache emp

    sql register(emp, "recommend_res")
  }

}
