package com.wheels.common.types

abstract class Types extends Serializable {
  protected var value: String = _

  protected def $(v: String): this.type = {
    value = v
    this
  }

  def get: String = value
}