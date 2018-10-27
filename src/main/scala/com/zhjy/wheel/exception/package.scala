package com.zhjy.wheel

package object exception {

  case class IllegalParamException(msg: String) extends RuntimeException(msg)

}
