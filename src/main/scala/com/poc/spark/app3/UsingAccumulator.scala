package com.poc.spark.app3

import com.poc.spark.config.SparkSessionConfig

object UsingAccumulator extends App{

  val sparkSession = SparkSessionConfig("WordCount", true).getSparkSession
  val df = sparkSession.read.text("/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/word-count.txt")

  import org.apache.spark.sql.functions._
  val df1 = df.withColumn("value", explode(split(col("value"), " ")))

  val longAcc = 
  df1.foreach((row) => {

  })
}
