package com.poc.spark.app4

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.SparkSession

object WordCount extends App {

  implicit val sparkSession: SparkSession = SparkSessionConfig("WordCount", true).getSparkSession
  val obj = new WordCount
  val inputFilePath = "/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/word-count.txt"

  obj.process(inputFilePath)
}

class WordCount(implicit sparkSession: SparkSession) extends Serializable {

  def process(inputfilePath: String): Unit = {

    val options = Map("header" -> "false", "inferSchema"-> "true")
    val inputDf = sparkSession.read.options(options).csv(inputfilePath)
    inputDf.show(false)

    import org.apache.spark.sql.functions._
    val modifiedDf = inputDf.withColumn("word", explode(split(col("_c0"), " "))).select("word").withColumn("count", lit(1))
    modifiedDf.show(false)

    val finalDf = modifiedDf.groupBy(col("word")).agg(count("word"))
    finalDf.show(false)
  }

}
