package com.poc.spark.app2

import com.poc.spark.app2.CreateDataFrame2.sparkSession
import com.poc.spark.app2.WordCount2.sparkSession
import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object WordCount3 extends App{

  val sparkSession = SparkSessionConfig("WordCount3", true).getSparkSession

  val inputDf = sparkSession.read.text("/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/word-count.txt")
  inputDf.show(false)

  way3(inputDf)
  // Find the word count
  def way1(inputDf: DataFrame): Unit = {
    import sparkSession.implicits._
    val flatMapDf = inputDf.flatMap((row) => {
      val values = row.mkString(" ").split(" ")
      values
    }).groupBy("value").count()

    flatMapDf.show(false)
  }

  def way2(inputDf: DataFrame): Unit = {
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._
    val flatMapDf = inputDf.flatMap((row) => {
      val values = row.mkString(" ").split(" ")
      values
    }).withColumn("counter", lit(1)).groupBy("value").sum("counter").withColumnRenamed("sum(counter)", "count")

    flatMapDf.show(false)
  }

  def way3(inputDf: DataFrame): Unit = {
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._

   /* val df = inputDf.withColumn("value", explode(split(col("value"), " "))).groupBy("value").count()
    df.show(false)*/

    val df = inputDf.withColumn("value", explode(split(col("value"), " ")))
      .withColumn("count", lit(1).cast(IntegerType))
      .groupBy("value")
      .agg(
        sum(col("count")).as("count"),
        avg(col("count")).as("avg")
      )
    df.show(false)

  }

}
