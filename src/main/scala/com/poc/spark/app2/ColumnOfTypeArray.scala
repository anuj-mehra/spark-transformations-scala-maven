package com.poc.spark.app2

import com.poc.spark.config.SparkSessionConfig

object ColumnOfTypeArray extends App{

  val sparkSession = SparkSessionConfig("ColumnOfTypeArray", true).getSparkSession

  val inputDf = sparkSession.read.text("/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/word-count.txt")
  inputDf.show(false)

  import org.apache.spark.sql.functions._
  val df1 = inputDf.withColumn("value", split(col("value"), " "))
  /*df1.printSchema
  df1.show(false)*/
  val df2 = df1.withColumn("find third row", when(array_contains(col("value"), "third"), "this is third row")
    .otherwise(typedLit[Option[String]](None)))
  df2.show(false)
}
