package com.wheels.common.types

object CRUD extends Enumeration {
  type CRUD = Value
  val INSERT: Value = Value("INSERT")
  val DELETE: Value = Value("DELETE")
  val UPDATE: Value = Value("UPDATE")
  val UPSERT: Value = Value("UPSERT")

}
