package com.wheels

package object exception {

  case class IllegalParamException(msg: String) extends RuntimeException(msg)
  case class IllegalConfException(msg: String) extends RuntimeException(msg)
  case class RealityTableNotFoundException(msg: String) extends RuntimeException(msg)

}
