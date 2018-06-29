package com.adityakonda.cdc

import java.util.{Calendar, Properties}

import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions._

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

case  class StageTableStats(source_table: String, target_table: String, etl_processing_time: Option[Long], record_count: Long)

object SparkCDC extends SparkApp {

  val hiveContext = getHiveContext(getClass().toString)

  //SET JOB PARAMETERS
  val sourceSystemName = "db_sakila"
  //val targetDataPath  = "/user/cloudera/"
  val targetDataPath  = "/user/hive/warehouse/sakila.db/"
  val startTime = System.currentTimeMillis / 1000

  def getConfigValueOf(key: String): String = {
    val configFile = "/home/cloudera/landing/connection.conf"
    Utils.getConfig(configFile).get(key).get
  }

  def getJdbcUrl(): String ={

    val jdbcHostname = getConfigValueOf("mysqlHostname")
    val jdbcPort = getConfigValueOf("mysqlPort")
    val jdbcDatabase = getConfigValueOf("mysqlDatabase")

    s"jdbc:mysql://$jdbcHostname:$jdbcPort/$jdbcDatabase?useSSL=false"
    //"jdbc:mysql://localhost:3306/retail_db?useSSL=false"
  }

  val a = "jdbc:mysql://localhost:3306/retail_db?useSSL=false"

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
                    tgtDataPath: String = s"$targetDataPath$sourceSystemName"): DataFrame ={

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
      .option("path",s"/user/hive/warehouse/sakila.db/$tableName")
      .saveAsTable(s"sakila.$tableName")

    jdbcTableWithProcTime
  }

  // Get High Water Mark for DB
  var highWaterMark: Long = 0
  if(hiveContext.tableNames().contains("etl_source_system")){

    val lastExtractTime = hiveContext.table("etl_source_system").filter(s"name = '$sourceSystemName'")
      .agg(max("extract_processing_time")).collect()(0).get(0)

    if(lastExtractTime != null){
      highWaterMark = lastExtractTime.asInstanceOf[Long]
    }
  }

  //TRACK STATISTICS FOR ETL

  //Initialize an empty DataFrame
  var etlStatistics = hiveContext.createDataFrame(Seq(StageTableStats("source_table","target_table",None,0)))
  etlStatistics = etlStatistics.filter("source_table <> 'source_table'")

  def compileStats(srcTableName: String, stagedDF: DataFrame, statsDF: DataFrame = etlStatistics)={
    val etlRecordCount = stagedDF.count()
    val etlProcessingTime: Option[Long] = if(etlRecordCount > 0) Option(stagedDF.agg(max("etl_processing_time")).collect()(0).getLong(0)) else None
    val etlTableStats = StageTableStats(srcTableName,s"stg_$srcTableName",etlProcessingTime,etlRecordCount)
    val etlTableStatsDF = hiveContext.createDataFrame(Seq(etlTableStats))

    etlStatistics = etlStatistics.unionAll(etlTableStatsDF)

    stagedDF.unpersist()
  }

  // Get All Table in Source Schema

  val schemaQuery = "(select TABLE_NAME from INFORMATION_SCHEMA.TABLES where TABLE_TYPE = 'BASE TABLE' and " +
  "TABLE_SCHEMA = 'sakila') as schema_tables"

  val jdbcSrcSchemaTableDF = hiveContext.read.jdbc(getJdbcUrl(), schemaQuery, getJdbcProperties())
  val jdbcSrcSchemaTables = jdbcSrcSchemaTableDF.map(r => r.getString(0)).collect()

  //val jdbcSrcSchemaTables = Array("rental","staff")

  val jdbcSrcSchemaTables_new = Array("rental","staff")

  //Run Staging ETL In Parallel

  val numConcurrentOps = 6
  val parJdbcSrcTables = jdbcSrcSchemaTables.par

  parJdbcSrcTables.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(numConcurrentOps))
  parJdbcSrcTables.map(table => compileStats(table, stageFromJdbc(table, highWaterMark)))

  // Set New High Water Mark
  case  class EtlSourceSystem(name: String = sourceSystemName, extract_processing_time: Long = startTime)
  hiveContext.createDataFrame(Seq(EtlSourceSystem())).write.mode("Append").saveAsTable("etl_source_system")

}
