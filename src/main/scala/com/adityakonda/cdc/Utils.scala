package com.adityakonda.cdc

import scala.io.Source

    /**
    * Created by Aditya Konda on 03/17/2018.
    */

object Utils {

  def getConfig(filePath: String)= {
    Source.fromFile(filePath).getLines().filter(line => line.contains("=")).map{ line =>
      val tokens = line.split("=")
      (tokens(0) -> tokens(1))
    }.toMap
  }

}
