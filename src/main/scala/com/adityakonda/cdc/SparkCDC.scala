package com.adityakonda.cdc

import java.util.{Calendar, Properties}

import org.apache.spark.sql.{DataFrame, functions}

case  class StageTableStats(source_table: String, target_table: String, etl_processing_time: Option[Long], record_count: Long)

object SparkCDC extends SparkApp {

  val hiveContext = getHiveContext(getClass().toString)

  def getConfigValueOf(key: String): String = {
    val configFile = "/home/cloudera/aditya/landing/connection.conf"
    Utils.getConfig(configFile).get(key).get
  }

  def getJdbcUrl(): String ={

    val jdbcHostname = getConfigValueOf("mysqlHostname")
    val jdbcPort = getConfigValueOf("mysqlPort")
    val jdbcDatabase = getConfigValueOf("mysqlDatabase")

    s"jdbc:mysql://$jdbcHostname:$jdbcPort/$jdbcDatabase?useSSL=false"
  }

  def getJdbcProperties(): Properties ={

    val connectionProperties = new Properties()
    connectionProperties.put("user", getConfigValueOf("mysqlUsername"))
    connectionProperties.put("password", getConfigValueOf("mysqlPassword"))

    connectionProperties
  }

  def stageFromJdbc(tableName: String,
                    lastExtractUnixTime: Long = 0,
                    jdbcUrl: String = getJdbcUrl(),
                    jdbcConnectionProperties: Properties = getJdbcProperties(),
                    tgtDataPath: String = ""): DataFrame ={

    //Get Time that processing stated
    val processingTime = Calendar.getInstance()

    //Extract Data From JDBC Source
    val jdbcTable = hiveContext.read.jdbc(jdbcUrl,tableName,jdbcConnectionProperties).where(s"unix_timestamp(last_update) > $lastExtractUnixTime")

    //Transform Data
    val jdbcTableWithProcTime = jdbcTable.withColumn("etl_processing_time",functions.lit(processingTime.getTime.getTime/1000))
      .withColumn("etl_year",functions.lit(processingTime.get(Calendar.YEAR)))
      .withColumn("etl_month",functions.lit(processingTime.get(Calendar.MONTH) + 1))
      .withColumn("etl_day",functions.lit(processingTime.get(Calendar.DAY_OF_MONTH)))

    jdbcTableWithProcTime.cache()

    hiveContext.sql(s"drop table if exists stage.$tableName")

    //Load Data to unmanaged parquet staging table
    jdbcTableWithProcTime.write
      .format("parquet").mode("Append")
      .partitionBy("etl_year","etl_month")
      .option("path",s"/home/cloudera/$tableName")
      .saveAsTable(s"stage.$tableName")

    jdbcTableWithProcTime
  }

  //TRACK STATISTICS FOR ETL

  //Initialize an empty DataFrame
  var etlStatistics = hiveContext.createDataFrame(Seq(StageTableStats("source_table","target_table",None,0)))
  etlStatistics = etlStatistics.filter("source_table <> 'source_table'")

  def compileStats(srcTableName: String, stagedDF: DataFrame, statsDF: DataFrame = etlStatistics)={

    val etlRecordCount = stagedDF.count()
    //val etlProcessingTime = if(etlRecordCount > 0) Option (stagedDF.agg(max("etlProcessing_time")).collect()(0))
  }
}
