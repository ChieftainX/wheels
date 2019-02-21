package com.wheels.common.types

class Metric() extends Types {
  val F1: this.type = $("f1")
  val PRECISION: this.type = $("weightedPrecision")
  val RECALL: this.type = $("weightedRecall")
  val ACC: this.type = $("accuracy")
}
