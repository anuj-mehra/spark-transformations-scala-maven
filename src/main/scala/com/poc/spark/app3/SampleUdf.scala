package com.poc.spark.app3

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.SparkSession

object SampleUdf extends App {

  val sparkSession: SparkSession = SparkSessionConfig("CreateDataframe", true).getSparkSession

  val data = List(("anuj", "mehra", "a"),
    ("kiyansh", "mehra", "b"),
    ("priyanka", "goyal", "c"))
  val schema = List("first_name", "last_name", " code")

  import sparkSession.implicits._
  val df = data.toDF(schema:_*)
  df.show(false)

  import org.apache.spark.sql.functions._
  val df2 = df.withColumn("upper-case-name", upper(col("first_name")))
  df2.show(false)

  /*
    val adder = (a: Int, b: Int) => a + b
  val adder2 = (a: Int, b: Int) => a + b
  val adder3: (Int, Int) => Int = (a: Int, b: Int) => a+ b
  val adder4: (Int, Int) => Int = _ + _
   */
  def getUpperName: (String)=> String = {
    _.toUpperCase
  }

  import org.apache.spark.sql.functions.udf

  val upperNameUdf = udf(getUpperName)

  val df3 = df.withColumn("upper-case-from-udf", upperNameUdf(col("first_name")))
  df3.show(false)

}
