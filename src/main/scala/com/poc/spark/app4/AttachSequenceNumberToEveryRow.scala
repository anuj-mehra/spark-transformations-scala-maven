package com.poc.spark.app4

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object AttachSequenceNumberToEveryRow extends App{

  implicit val sparkSession: SparkSession = SparkSessionConfig("AttachSequenceNumberToEveryRow", true).getSparkSession
  val obj = new AttachSequenceNumberToEveryRow
  val inputFilePath = "/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/csv-file-with-header-trailor.csv"

  obj.addIndex(inputFilePath)
}

class AttachSequenceNumberToEveryRow(implicit sparkSession: SparkSession) extends Serializable {

  def addIndex(inputCsvFilePath: String): Unit = {

    val df = sparkSession.read.csv(inputCsvFilePath)
    df.show(false)

    val dataWithIndexRdd: RDD[Row] = df.rdd.zipWithIndex().map { case (row, index) =>
      Row.fromSeq(row.toSeq :+ index) // Add index as a new column
    }

    val schema = StructType(List(StructField("row", StringType, true), StructField("index", LongType, true)))
    val dataWithIndexDf = sparkSession.createDataFrame(dataWithIndexRdd, schema)
    dataWithIndexDf.show(false)

    import org.apache.spark.sql.functions._
    val headerDf = dataWithIndexDf.where(col("index") === 0 || col("index") === 1)
    headerDf.show(false)

    val maxIndex: DataFrame = dataWithIndexDf.agg(max(col("index")).as("index"))
    val trailorDf = dataWithIndexDf.join(maxIndex, Seq("index"), "inner").select("row", "index")
    trailorDf.show(false)

    val dataDf = dataWithIndexDf.except(trailorDf).except(headerDf)
    dataDf.show(false)
    println("------")

    val schemaColumns: Array[Row] = headerDf.where(col("index") === 1).select("row")
      .withColumn("row", explode(split(col("row"), "|"))).collect()
    val schemaNames = schemaColumns.foreach(r => {
      r.getAs[String]("row")
    })
    println(schemaNames)
  }
}