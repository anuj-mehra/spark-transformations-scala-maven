package com.poc.spark.app2

import com.poc.spark.app2.UsingSort2.sparkSession
import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.Row

object CheckForEmptyDataFrame extends App{

  val sparkSession = SparkSessionConfig("CheckForEmptyDataFrame", true).getSparkSession

  method1

  def method1: Unit = {

    val sampleData = List(("James", "Sales", "NY", 90000, 34, 10000),
      ("Michael", "Sales", "NY", 86000, 56, 20000),
      ("Robert", "Sales", "CA", 81000, 30, 23000),
      ("Maria", "Finance", "CA", 90000, 24, 23500),
      ("Raman", "Finance", "CA", 99000, 40, 24500),
      ("ASD", "Finance", "CA", 99000, 40, 24000),
      ("Scott", "Finance", "NY", 83000, 36, 19000),
      ("Jen", "Finance", "NY", 79000, 53, 15000),
      ("Jeff", "Marketing", "CA", 80000, 25, 18000),
      ("Kumar", "Marketing", "NY", 91000, 50, 21000)
    )

    val schema = List("employee_name", "department", "state", "salary", "age", "bonus")

    import sparkSession.implicits._
    val df = sampleData.toDF(schema:_*)
    import org.apache.spark.sql.functions._

    val df1 = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], df.schema)

    (df1.isEmpty) match {
      case true =>
        println("--1: dataframe is empty--")
      case false =>
        println("--1: dataframe is NOT empty--")
    }

    val row: Array[Row] = df1.take(1)

    (df1.take(1).size == 0) match {
      case true =>
        println("--2: dataframe is empty--")
      case false =>
        println("--2: dataframe is NOT empty--")
    }

  }


}

