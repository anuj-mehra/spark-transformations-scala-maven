package com.poc.spark.app3

import com.poc.spark.app3.WordCount.sparkSession
import com.poc.spark.config.SparkSessionConfig
import org.apache.commons.codec.Encoder
import org.apache.spark.sql.{Encoders, Row}
import org.apache.spark.sql.catalyst.encoders.RowEncoder

object Transformations extends App with Serializable{

  val sparkSession = SparkSessionConfig("WordCount", true).getSparkSession
  val df = sparkSession.read.text("/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/word-count.txt")

  val df2 = df.flatMap((row) => {

    val words: Array[String] = row.mkString.split(" ")
    words
  })(Encoders.STRING)

  df2.show(false)

  import org.apache.spark.sql.functions._
  val df3 = df2.groupBy("value").agg(count("value"))
  df3.show(false)

  import sparkSession.implicits._
  val df4 = df3.map((row) => {
    val length = getLength(row.getAs[String]("value"))
    (row.getAs[String]("value"), row.getAs[Long]("count(value)"), length)
  }).toDF("a", "b", "c")

  def getLength: (String) =>Int = {
    _.length
  }

  df4.show(false)
}
