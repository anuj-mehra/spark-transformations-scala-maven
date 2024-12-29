package com.poc.spark.app

import com.poc.spark.app3.UsingAccumulator.sparkSession
import com.poc.spark.config.SparkSessionConfig

object SaveAsCSV extends App with Serializable {

  implicit val sparkSession = SparkSessionConfig("SaveAsCSV", true).getSparkSession

  val df = sparkSession.read.text("/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/word-count.txt")

  import org.apache.spark.sql.functions._
  val df1 = df.withColumn("value", explode(split(col("value"), " ")))

}
