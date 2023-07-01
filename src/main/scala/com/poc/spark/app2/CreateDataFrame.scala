package com.poc.spark.app2

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object CreateDataFrame extends App with Serializable {

  //option1

  createDfFromTxtFile

  def option1: Unit = {
    implicit val sparkSession: SparkSession = SparkSessionConfig("CreateDataFrame", true).getSparkSession

    val personList: List[(String, String, Int, String)] = List(("fn1", "ln1", 20, "M"),
    ("fn2", "ln2", 21, "M"),
    ("fn3", "ln3", 22, "F"),
    ("fn4", "ln4", 23, "F"))

    val schema = List("first_name","last_name", "age", "sex")
    import sparkSession.implicits._
    val df = personList.toDF(schema:_*)
    df.show(false)
  }

  def createDfFromTxtFile: Unit = {
    implicit val sparkSession: SparkSession = SparkSessionConfig("CreateDataFrame", true).getSparkSession

    val inputFilePath = "/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/sample-text-file.txt"

    import org.apache.spark.sql.functions._
    val schema = StructType(List(StructField("id", StringType , true),
      StructField("firstName", StringType , true),
      StructField("lastName", StringType , true),
      StructField("age", StringType , true),
      StructField("location", StringType , true)))
    val encoder = RowEncoder(schema)

    val df = sparkSession.read.text(inputFilePath).map(row=> {
      val values = row.mkString(",").split(",")
      Row(values(0), values(1), values(2), values(3), values(4))
    })(encoder)
    df.show(false)
  }

}
