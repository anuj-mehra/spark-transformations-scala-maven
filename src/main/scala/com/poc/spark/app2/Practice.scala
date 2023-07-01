package com.poc.spark.app2

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.SparkSession

object Practice extends App with Serializable {

  implicit val sparkSession: SparkSession = SparkSessionConfig("Practice", true).getSparkSession

  val personList = List(("fn1", "ln1", 20, "M"),
    ("fn2", "ln2", 21, "M"),
    ("fn3", "ln3", 22, "F"),
    ("fn4", "ln4", 23, "F"))

  val schema = List("first_name", "last_name", "age", "sex")

  import sparkSession.implicits._
  val df = personList.toDF(schema:_*)

  import org.apache.spark.sql.functions._
  val df2 = df.withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name") ))
    .withColumn("full_name_2", concat_ws(" ", col("first_name"), col("last_name")))
  df2.show(false)
}
