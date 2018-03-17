package com.adityakonda.cdc

import java.util.Properties

    /**
    * Created by Aditya Konda on 03/17/2018.
    */

object TestingSparkJDBC extends SparkApp {

  def main(args: Array[String]): Unit = {

    val hiveContext = getHiveContext(getClass.getName)

    def getConfig(value: String): String = {
      val configFile = "/home/cloudera/aditya/landing/connection.conf"
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
