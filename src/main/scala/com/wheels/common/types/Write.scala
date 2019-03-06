package com.wheels.common.types

object Write extends Enumeration {
  type Write = Value
  val INSERT: Value = Value("INSERT")
  val DELETE: Value = Value("DELETE")
  val UPDATE: Value = Value("UPDATE")
  val UPSERT: Value = Value("UPSERT")
}
