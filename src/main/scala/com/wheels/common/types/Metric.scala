package com.wheels.common.types

class Metric() extends Enum {
  val F1: this.type = $("f1")
  val PRECISION: this.type = $("weightedPrecision")
  val RECALL: this.type = $("weightedRecall")
  val ACC: this.type = $("accuracy")
}

object Metric {
  def apply: Metric = new Metric()
}
