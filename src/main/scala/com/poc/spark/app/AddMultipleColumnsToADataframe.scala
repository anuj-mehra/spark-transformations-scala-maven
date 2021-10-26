package com.poc.spark.app

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.{Column, Row, SaveMode}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object AddMultipleColumnsToADataframe extends App with Serializable{

  val spark = SparkSessionConfig("AddMultipleColumnsToADataframe", true).getSparkSession
  val inputData = List(("anuj", "mehra"),("kiyansh", "mehra"))
  val schema = List("first_name", "last_name")

  import spark.implicits._
  val df = inputData.toDF(schema:_*)

  val df2 = df.withColumn("place", lit("Delhi").cast(StringType))
    .withColumn("age", when(col("first_name") ==="anuj", 36).otherwise(2))

  df2.show(false)
  df2.printSchema()
}