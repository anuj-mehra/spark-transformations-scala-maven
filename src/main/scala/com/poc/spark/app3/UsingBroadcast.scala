package com.poc.spark.app3

import com.poc.spark.config.SparkSessionConfig

object UsingBroadcast extends App{

  val sparkSession = SparkSessionConfig("WordCount", true).getSparkSession

  val data1 = List(("a1", "b1"),
                     ("a2", "b2"))
  val data2 = List(("a11", "c11"),
                    ("a2", "c22"))
  val schema1 = List("col1", "col2")
  val schema2 = List("col1", "col3")

  import sparkSession.implicits._
  val df1 = data1.toDF(schema1:_*)
  val df2 = data2.toDF(schema2:_*)

  import org.apache.spark.sql.functions.broadcast
  // val dfFinal = df1.join(broadcast(df2), Seq("col1"), "leftanti")

  val dfFinal = df1.join(broadcast(df2),
    df1("col1") === df2("col1")
    && df1("col1") === df2("col1"), "leftanti")
  dfFinal.show(false)



}
