package com.adityakonda.cdc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

class SparkApp {

  def getHiveContext(appName: String): HiveContext ={

    /*    SETTING UP SPARK CONFIGURATION   */
    val conf = new SparkConf()
    conf.setAppName(appName)
    conf.setMaster("yarn-client")

    val sc = new SparkContext(conf)

    /*    SETTING UP HIVE CONTEXT   */
    new HiveContext(sc)
  }

}
