package com.adityakonda.cdc

import java.util.Properties

    /**
    * Created by Aditya Konda on 03/17/2018.
    */

object TestingSparkJDBC extends SparkApp {

  def main(args: Array[String]): Unit = {

    val hiveContext = getHiveContext(getClass.getName)

    def getConfigValueOf(key: String): String = {
      val configFile = "/home/cloudera/aditya/landing/connection.conf"
      Utils.getConfig(configFile).get(key).get
    }

    val jdbcHostname = getConfigValueOf("mysqlHostname")
    val jdbcPort = getConfigValueOf("mysqlPort")
    val jdbcDatabase = getConfigValueOf("mysqlDatabase")

    // Adding username & password
    val connectionProperties = new Properties()
    connectionProperties.put("user", getConfigValueOf("mysqlUsername"))
    connectionProperties.put("password", getConfigValueOf("mysqlPassword"))

    // Create the MyYSQL JDBC URL without passing in the user and password parameters.
    val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?useSSL=false"

    val deptDF = hiveContext.read.jdbc(jdbcUrl,"departments",connectionProperties)
    deptDF.collect().foreach(print)

  }

}
