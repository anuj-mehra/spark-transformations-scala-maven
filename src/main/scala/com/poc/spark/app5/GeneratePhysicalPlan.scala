package com.poc.spark.app5

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, rank, row_number}

object GeneratePhysicalPlan extends App{

  implicit val spark: SparkSession = SparkSessionConfig("GeneratePhysicalPlan", true).getSparkSession

  val options = Map("delimiter"->",",
    "header"->"true",
    "inferSchema"-> "true")
  val csvDf1 = spark.read.options(options).csv("/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/join-left-data.csv");

  val csvDf2 = spark.read.options(options).csv("/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/join-right-data.csv");

  csvDf1.show(false)
  csvDf2.show(false)

  //val joinedDf  = csvDf1.join(broadcast(csvDf2),  csvDf1("empid") === csvDf2("empid"), "inner")
  //joinedDf.show(false)

  val joinedDf = csvDf1.join(csvDf2,  Seq("empid"), "inner")
  joinedDf.explain(true)
  joinedDf.show(false)

  val allProperties = spark.conf.getAll
  allProperties.foreach { case (key, value) =>
    println(s"$key -> $value")
  }


  val windowSpec = Window.partitionBy("empdept").orderBy("empid")
  //val finalDf = joinedDf.withColumn("rownum", row_number().over(windowSpec))
  val finalDf = joinedDf.withColumn("rownum", rank().over(windowSpec))
  //val finalDf = joinedDf.withColumn("rownum", row_number().over(windowSpec))
  finalDf.explain(true)
  finalDf.show(false)
}
