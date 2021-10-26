package com.poc.spark.app

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object SortOnColumnBasis extends App with Serializable {

  implicit val sparkSession = SparkSessionConfig("SortOnColumnBasis", true).getSparkSession

  val processor = new SortOnColumnBasis
  processor sortOnId sparkSession

}

class SortOnColumnBasis extends Serializable {

  def sortOnId: (SparkSession) => Unit = (sparkSession: SparkSession) => {

    val inputData = List(("5", "a", "mehra"),
      ("2", "b", "mehra"),
      ("1", "c", "mehra"),
      ("4", "d", "mehra"),
      ("3", "e", "mehra"))

    val schemaColumns = List("id", "fname", "lname")

    import sparkSession.implicits._
    val df = inputData.toDF(schemaColumns:_*)

    df.show(false)
    val df2 = df.sort(col("id").desc)
    df2.show(false)

  }

  def sortAnArrayColumn: (SparkSession) => Unit = (sparkSession: SparkSession) => {

  }

}
