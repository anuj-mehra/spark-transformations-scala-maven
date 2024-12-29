package com.poc.spark.app3

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.Row

// https://sparkbyexamples.com/spark/spark-foreachpartition-vs-foreach-explained/
// foreach and foreachPartition : both are actions
object ForEachPartitionAndForEach extends App{

  val sparkSession = SparkSessionConfig("WordCount", true).getSparkSession
  val df = sparkSession.read.text("/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/word-count.txt")

  import org.apache.spark.sql.functions._
  val df1 = df.withColumn("value", explode(split(col("value"), " ")))

  df1.show(false)

  /*df1.foreach(row => {
    println(row.getAs[String](0))
  })*/

 df1.foreachPartition((rows: Iterator[Row]) => {

    rows.foreach(row => println(row.getAs[String](0)))
  })
}
