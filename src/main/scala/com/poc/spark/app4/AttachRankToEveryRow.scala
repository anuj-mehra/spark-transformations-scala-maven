package com.poc.spark.app4

import com.poc.spark.app4.AttachRankToEveryRow.{inputFilePath, sparkSession}
import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StructField, StructType}

object AttachRankToEveryRow extends App{

  implicit val sparkSession: SparkSession = SparkSessionConfig("AttachRankToEveryRow", true).getSparkSession
  val obj = new AttachRankToEveryRow
  val inputFilePath = "/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/csv-file-1.csv"

  obj.option1UsingZipWithIndex
}

class AttachRankToEveryRow extends Serializable {

  def option1UsingZipWithIndex: Unit = {
    val options = Map("inferSchema"-> "true",
      "delimiter"->"|")

    val inputDf = sparkSession.read.options(options).csv(inputFilePath)
    // Convert DataFrame to RDD and add index
    val rddWithIndex: RDD[Row] = inputDf.rdd.zipWithIndex().map { case (row, index) =>
      Row.fromSeq(row.toSeq :+ index) // Add index as a new column
    }

    val newSchema = StructType(inputDf.schema.fields :+ StructField("index", LongType, nullable = false))
    val dfWithIndex = sparkSession.createDataFrame(rddWithIndex, newSchema)

    dfWithIndex.show(false)
  }
}
