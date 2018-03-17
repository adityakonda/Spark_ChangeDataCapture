package com.adityakonda.cdc

import java.util.Properties
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

    /**
    * Created by Aditya Konda on 03/17/2018.
    */

object TestingSparkJDBC {

  def main(args: Array[String]): Unit = {

    /*    SETTING UP SPARK CONFIGURATION   */
    val conf = new SparkConf()
    conf.setAppName("Aditya")
    conf.setMaster("yarn-client")

    val sc = new SparkContext(conf)

    /*    SETTING UP HIVE CONTEXT   */
    val hiveContext = new HiveContext(sc)

    def getConfig(value: String): String = {
      val configFile = "/home/cloudera/aditya/landing/connection.conf"
      //val configFile = "C:\\Users\\Aditya.Konda\\Documents\\connection.conf"
      Utils.getConfig(configFile).get(value).get
    }

    val jdbcHostname = getConfig("mysqlHostname")
    val jdbcPort = getConfig("mysqlPort")
    val jdbcDatabase = getConfig("mysqlDatabase")

    // Create the MyYSQL JDBC URL without passing in the user and password parameters.
    val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?useSSL=false"

    // Adding username & password
    val connectionProperties = new Properties()
    connectionProperties.put("user", getConfig("mysqlUsername"))
    connectionProperties.put("password", getConfig("mysqlPassword"))

    val deptDF = hiveContext.read.jdbc(jdbcUrl,"departments",connectionProperties)
    deptDF.collect().foreach(print)

  }

}
