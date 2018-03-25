package com.adityakonda.cdc

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataType

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

      def castAllTypedColumnsTo(df: DataFrame, sourceType: DataType, targetType: DataType): DataFrame = {
        df.schema.filter(_.dataType == sourceType).foldLeft(df) {
          case (acc, col) => acc.withColumn(col.name, df(col.name).cast(targetType))
        }
      }

      def getTableIntFields(filePath: String, tableName: String) = {
        Source.fromFile(filePath).getLines().filter(line => line.contains(",")).map{ line =>
          val tokens = line.split(",")
          (tokens(0) -> tokens(1))
        }.toMap.get(tableName).get.split('|')
      }
}
