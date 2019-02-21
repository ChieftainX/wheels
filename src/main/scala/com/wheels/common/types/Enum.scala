package com.wheels.common.types

abstract class Enum extends Serializable {
  protected var value: String = _

  protected def $(v: String): this.type = {
    value = v
    this
  }

  def get: String = value
}
