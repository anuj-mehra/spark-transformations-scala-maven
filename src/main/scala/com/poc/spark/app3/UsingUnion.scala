package com.poc.spark.app3

import com.poc.spark.config.SparkSessionConfig

object UsingUnion extends App{

  val sparkSession = SparkSessionConfig("WordCount", true).getSparkSession

  val data1 = List(("a1", "b1"), ("a2", "b2"))
  val data2 = List(("a11", "b11"), ("a22", "b22"))
  val schema = List("col1", "col2")

  import sparkSession.implicits._
  val df1 = data1.toDF(schema:_*)
  val df2 = data2.toDF(schema:_*)

  val df3 = df1.unionByName(df2)
  //df3.show(false)

  val data3 = List(("a33"))
  val schema2 = List("col1")
  val df4 = data3.toDF(schema2:_*)

  val finalDf = df3.unionByName(df4, true)
  finalDf.show(false)
}
