package com.poc.spark.app

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.SparkSession

object UnionDataframes extends App{

  val sparkSession = SparkSessionConfig("UnionDataframes", true).getSparkSession

  val processor  = new UnionDataframes
  // processor defaultUnion sparkSession
  processor unionByName sparkSession
}

class UnionDataframes extends Serializable {

  def defaultUnion: (SparkSession) => Unit = (spark:SparkSession) => {

    val data1 = List(("100", "Software"),
      ("200","Finance"))

    val data2 = List(("300","Management"),
      ("400","Sales"),
      ("500","HR"))

    val schemaColumns = List("deptId", "deptName")

    import spark.implicits._
    val df1 = data1.toDF(schemaColumns:_*)
    val df2 = data1.toDF(schemaColumns:_*)

    val mergedDf = df1.union(df2)
    mergedDf.show(false)
  }

  def unionByName: (SparkSession) => Unit = (spark:SparkSession) => {
    val data1 = List(("100", "Software"),
      ("200","Finance"))

    val data2 = List(("300","Management"),
      ("400","Sales"),
      ("500","HR"))

    val schemaColumns = List("deptId", "deptName")

    import spark.implicits._
    val df1 = data1.toDF(schemaColumns:_*)
    val df2 = data1.toDF(schemaColumns:_*)

    val mergedDf = df1.unionByName(df2)
    mergedDf.show(false)
  }
}
