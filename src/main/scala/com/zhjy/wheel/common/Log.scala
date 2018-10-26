package com.zhjy.wheel.common

import org.apache.log4j.{Level, Logger}

object Log {

  def get(name: String): Logger = Logger.getLogger(name)

  def log_setter(levels: Map[String, Level]): this.type = {
    levels.foreach {
      case (k, v) => Logger.getLogger(k).setLevel(v)
    }
    this
  }

  def log_setter(key: String, level: Level): this.type = {
    Logger.getLogger(key).setLevel(level)
    this
  }

}
