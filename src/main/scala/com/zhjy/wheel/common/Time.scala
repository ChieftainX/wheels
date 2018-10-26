package com.zhjy.wheel.common


import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
  * Created by zzy on 2018/10/25.
  */
object Time {

  lazy val yyyy_MM_dd_HH_mm_ss: String = "yyyy-MM-dd HH:mm:ss"
  lazy val yyyy_MM_dd: String = "yyyy-MM-dd"
  lazy val yyyy_MM: String = "yyyy-MM"

  /**
    * 获取当前时间的字符串
    *
    * @return String
    */
  def now: String = new SimpleDateFormat(yyyy_MM_dd_HH_mm_ss).format(new Date())

  /**
    * 获取当前天的字符串
    *
    * @return String
    */
  def today: String = new SimpleDateFormat(yyyy_MM_dd).format(new Date())

  /**
    * 获取前一天日期的字符串
    *
    * @return String
    */
  def yesterday: String = {
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    new SimpleDateFormat(yyyy_MM_dd).format(cal.getTime)
  }

  /**
    * 获取前N天日期的字符串
    *
    * @return String
    */
  def before_n_day(n: Int): String = {
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, 0 - n)
    new SimpleDateFormat(yyyy_MM_dd).format(cal.getTime)
  }

  /**
    * 获取前一个月的字符串
    */
  def before_month: String = {
    val cal = Calendar.getInstance()
    cal.setTime(new Date)
    cal.set(Calendar.DAY_OF_MONTH, 1)
    cal.add(Calendar.MONTH, -1)
    new SimpleDateFormat("yyyy-MM").format(cal.getTime)
  }

  /**
    * 获取一年当中的全部月（上限：当前月及以前）
    */
  def all_month1year(year: String): Seq[String] = {
    val ms = ArrayBuffer[String]()
    var last_month = 12
    val now = new SimpleDateFormat(yyyy_MM).format(Calendar.getInstance.getTime).split("-")
    if (year.equals(now(0))) last_month = now(1).toInt
    for (m <- 1 to last_month) ms.append(s"$year-${
      if (m < 10) s"0$m" else m
    }")
    ms.result
  }

  /**
    * 字符串转时间戳
    *
    * @param str 时间字符串
    * @return
    */
  def str2ts(str: String): Timestamp = {
    if (str == null) return null
    Try(new Timestamp(new SimpleDateFormat(yyyy_MM_dd).parse(str.substring(0, 19)).getTime)).getOrElse(null)
  }


  /**
    * 日期字符串转分区元组
    *
    * @param day 日期字符串
    * @return
    */
  def str2partition(day: String): (String, String, String) = {
    val partition = day.split("-")
    (partition(0), partition(1), partition(2))
  }

}

