package com.poc.spark.app

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object AddColumnWhenOtherwise extends App with Serializable{

  val spark = SparkSessionConfig("AddColumnWhenOtherwise", true).getSparkSession
  val inputData = List(("anuj", "mehra"),("kiyansh", "mehra"))
  val schema = List("first_name", "last_name")

  import spark.implicits._
  val df = inputData.toDF(schema:_*)

  val df2 = df.withColumn("age", when(col("first_name") === "anuj", lit(36).cast(IntegerType)).otherwise(lit(2)))

  df2.show(false)

  df2.printSchema()
}
