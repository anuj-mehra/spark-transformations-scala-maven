package com.poc.spark.app

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object ACreateDataframe extends App with Serializable {

  implicit val sparkSession = SparkSessionConfig("ACreateDataframe", true).getSparkSession

  val processor = new ACreateDataframe
  //processor createDataframeFromList sparkSession
  //processor createDataframeFromListCastType sparkSession
  //processor createDataframeFromListOfTypeRow sparkSession
  //processor createDataframeFromText sparkSession
  //processor createDataframeFromCsvFile sparkSession
}

class ACreateDataframe extends Serializable {

  def createDataframeFromList: (SparkSession) => Unit = (sparkSession: SparkSession) => {

    val inputData = List(("anuj mehra", 36L), ("priyanka goyal", 36L))
    val schema = List("name", "age")

    import sparkSession.implicits._
    val df = inputData.toDF(schema:_*)
    df.show(false)
    df.printSchema()
  }

  def createDataframeFromListCastType: (SparkSession) => Unit = (sparkSession: SparkSession) => {

    val inputData = List(("anuj mehra", 36), ("priyanka goyal", 36))
    val schema = List("name", "age")

    import sparkSession.implicits._
    val df = inputData.toDF(schema:_*)
    df.show(false)
    df.printSchema()

    val df2 = df.withColumn("age", col("age").cast(DoubleType))
    df2.printSchema()
    df2.show(false)
  }

  def createDataframeFromListOfTypeRow: (SparkSession) => Unit = (sparkSession: SparkSession) => {

    val inputData = List(Row("anuj mehra", 36), Row("priyanka goyal", 36))

    val schema = StructType(List(StructField("name", StringType, true), StructField("age", IntegerType, true)))

    val rdd = sparkSession.sparkContext.parallelize(inputData)
    val df = sparkSession.createDataFrame(rdd,schema)

    df.show(false)
  }

  def createDataframeFromText: (SparkSession) => Unit = (sparkSession : SparkSession) => {

    val inputFilePath = "/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/sample-text-file.txt"

    val schema = StructType(List(
      StructField("id", StringType , true),
      StructField("firstName", StringType , true),
      StructField("lastName", StringType , true),
      StructField("age", StringType , true),
      StructField("location", StringType , true)
    ))

    val encoder = RowEncoder(schema)
    val df: DataFrame = sparkSession.read.text(inputFilePath)
      .map((row) => {
        val values = row.mkString(",").split(",")
         Row(values(0), values(1), values(2), values(3), values(4))
      })(encoder)

    df.show(false)
  }

  def createDataframeFromCsvFile: (SparkSession) => Unit = (sparkSession: SparkSession) => {

    val csvFilePath = "/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/csv-file-1.csv"

    val df = sparkSession.read
      .option("header", true)
      .option("delimiter","|")
      .csv(csvFilePath)

    df.show(false)
  }

}

