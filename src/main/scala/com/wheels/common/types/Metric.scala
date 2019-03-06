package com.wheels.common.types

object Metric extends Enumeration {
  type Metric = Value
  val F1: Value = Value("f1")
  val PRECISION: Value = Value("weightedPrecision")
  val RECALL: Value = Value("weightedRecall")
  val ACC: Value = Value("accuracy")
}
