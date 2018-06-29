package com.adityakonda.cdc

object Test {

  def main(args: Array[String]): Unit = {

    println("sadf")

    println("asdf")

  }

  def toInt(s: String): Option[Int] = {
    try {
      Some(Integer.parseInt(s.trim))
    } catch {
      case e: Exception => None
    }
  }
}
