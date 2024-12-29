package com.poc.spark.app3

import com.poc.spark.config.SparkSessionConfig

object WordCount extends App {

  val sparkSession = SparkSessionConfig("WordCount", true).getSparkSession
  val df = sparkSession.read.text("/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/word-count.txt")

  import org.apache.spark.sql.functions._
  val df2 = df.withColumn("value", explode(split(col("value"), " ")))
  df2.show(false)

  df2.printSchema

  //df2.groupBy(col("value")).agg(count("value")).orderBy(col("count(value)")).show(false)

  df2.groupBy(col("value")).agg(count("value")).withColumnRenamed("count(value)", "count").show(false)
}
