package com.poc.spark.app4

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.SparkSession

object ComplexDataTypes extends App {

  val sparkSession: SparkSession = SparkSessionConfig("ComplexDataTypes", true).getSparkSession

  dfWithArray

  def dfWithArray: Unit = {
    val df = sparkSession.read.text("/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/word-count.txt")
    import org.apache.spark.sql.functions._

    val df2 = df.withColumn("value", split(col("value"), " ")).withColumn("constant", lit("constant"))
    df2.show(false)

    df2.printSchema

    println(df2.schema.prettyJson)


  }

}
