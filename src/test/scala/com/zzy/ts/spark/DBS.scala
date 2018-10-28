package com.zzy.ts.spark

import com.zhjy.wheel.spark.SQL

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

    sql register(emp, "emp", cache = true)
  }

}
