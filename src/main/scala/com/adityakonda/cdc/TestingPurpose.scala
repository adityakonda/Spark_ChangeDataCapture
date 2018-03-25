package com.adityakonda.cdc

object TestingPurpose extends App {


  val tableFieldPath = "C:\\Users\\Aditya.Konda\\Documents\\test_data\\scala_testing.csv"
  val tableName = "Employees_new"

  for (field <- Utils.getTableIntFields(tableFieldPath,tableName)) {
    println(field.trim.toUpperCase)
  }

}
