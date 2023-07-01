package com.poc.spark.app

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object WordCount extends App{

  val sparkSession = SparkSessionConfig("WordCount", true).getSparkSession

  import sparkSession.implicits._

  import org.apache.spark.sql.functions._
  // new way
  val usingExplodeDf = sparkSession.read.text("/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/word-count.txt")
  val df: DataFrame = usingExplodeDf.withColumn("value", explode(split(col("value"), " "))).withColumn("count", lit(1))
  df.printSchema
  df.show(false)

  val df2: DataFrame = df.groupBy("value").sum("count")
  df2.show(false)

  // legacy way
  val inputDf = sparkSession.read.text("/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/word-count.txt")
    .flatMap((row) => {
      val words: Array[String] = row.mkString(" ").split(" ")
      words
    }).toDF("word3")
  val df3 = inputDf.groupBy("word3").agg(count("word3")).withColumnRenamed("count(word3)","count")
  df3.show(false)
}
