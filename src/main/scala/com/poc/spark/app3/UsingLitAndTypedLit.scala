package com.poc.spark.app3

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.SparkSession

object UsingLitAndTypedLit extends App{

  val sparkSession: SparkSession = SparkSessionConfig("CreateDataframe", true).getSparkSession

  val data = List(("anuj", "mehra", "a"),
    ("kiyansh", "mehra", "b"),
    ("priyanka", "goyal", "c"))

  val schema = List("first_name", "last_name", " code")

  import sparkSession.implicits._
  val df = data.toDF(schema:_*)

  import org.apache.spark.sql.functions._
  val df2 = df.withColumn("city", when(col("first_name") === "anuj" , "Pune").otherwise(lit("pune")))
  //df2.show(false)

  val df3 = df2.withColumn("pincode", typedLit[Long](411014))
  //df3.show(false)
  //df3.printSchema()

  val dfWithNullColumn = df3.withColumn("null-value-col", typedLit[Option[Long]](None))
  dfWithNullColumn.printSchema()
  dfWithNullColumn.show(false)
}
