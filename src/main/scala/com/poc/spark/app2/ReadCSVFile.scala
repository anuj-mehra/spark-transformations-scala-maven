package com.poc.spark.app2

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ReadCSVFile extends App{

  implicit val sparkSession = SparkSessionConfig("ReadCSVFile", true).getSparkSession

  val csvFilePath = "/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/csv-file-1.csv"

  val schema = StructType(List(
    StructField("firstName", StringType , true),
    StructField("middleName", StringType , true),
    StructField("lastName", StringType , true),
    StructField("city", StringType , true)
  ))

  val encoder = RowEncoder(schema)

  val df: DataFrame = sparkSession.read.option("delimiter","|").option("header", "true").csv(csvFilePath)
   //df.show(false)
  import org.apache.spark.sql.functions._
  val df1 = df.select(col("firstName"), col("middleName"))
  val df2 = df.select("firstName", "middleName")

  //df2.show(false)

  val df3 = df2.select("firstName")
  val df4 = df2.select("firstName")

  //val df5 = df3.join(df4, Seq("firstName"), "inner")
  //df5.show(false)


  val personList: List[String] = List("fn1", "fn2")
  val schemaX = List("firstName")
  import sparkSession.implicits._
  val df5 = personList.toDF(schemaX:_*)
  val df6 = df3.join(df5, Seq("firstName"), "inner")
  df6.show(false)

}
