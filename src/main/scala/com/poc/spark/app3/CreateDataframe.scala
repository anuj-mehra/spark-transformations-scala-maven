package com.poc.spark.app3

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.SparkSession

object CreateDataframe extends App{

  val sparkSession: SparkSession = SparkSessionConfig("CreateDataframe", true).getSparkSession

  val data = List(("anuj", "mehra", "a"),
                  ("kiyansh", "mehra", "b"),
                  ("priyanka", "goyal", "c"))
  val schema = List("first_name", "last_name", " code")

  import sparkSession.implicits._
  val df = data.toDF(schema:_*)
  df.show(false)
  
}
