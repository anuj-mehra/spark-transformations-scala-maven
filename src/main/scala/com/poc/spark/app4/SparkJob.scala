package com.poc.spark.app4

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SparkJob extends App with  Serializable{

  implicit val sparkSession: SparkSession = SparkSessionConfig("SparkJob", true).getSparkSession

  val inputFilePath = "/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/sample-text-file.txt"

  val schema1 = StructType(List(
    StructField("id", StringType , true),
    StructField("firstName", StringType , true),
    StructField("lastName", StringType , true),
    StructField("age", StringType , true),
    StructField("location", StringType , true)))
  val encoder = RowEncoder(schema1)

  val df = sparkSession.read.text(inputFilePath).map(row=> {
    val values = row.mkString(",").split(",")
    Row(values(0), values(1), values(2), values(3), values(4))
  })(encoder).drop(Seq("age"):_*)
  //df.show(false)


  val df2 = sparkSession.read.text(inputFilePath).map(row=> {
    val values = row.mkString(",").split(",")
    Row(values(0), values(1), values(2), values(3), values(4))
  })(encoder).drop(Seq("location"):_*)
  //df2.show(false)


  val df3 = df.join(df2, Seq("firstName", "lastName"), "inner")
  df3.show(false)


  Thread.sleep(10000000L)

}
