package com.poc.spark.app2

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object CreateDataFrame2 extends App{

  val sparkSession = SparkSessionConfig("CreateDataFrame2", true).getSparkSession

  /*val df1 = createDataFrameFromList
  df1.show(false)*/
  def createDataFrameFromList : DataFrame ={

    val personList: List[(String, String, Int, String)] =
      List(("fn1", "ln1", 20, "M"),
        ("fn2", "ln2", 21, "M"),
        ("fn3", "ln3", 22, "F"),
        ("fn4", "ln4", 23, "F"))

    val schema = List("first_name","last_name", "age", "sex")

    import sparkSession.implicits._
    val  df= personList.toDF(schema:_*)
    df
  }

  /*val inputFilePath = "/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/sample-text-file.txt"
  val df2 = createDataFrameFromCSV(inputFilePath)
  df2.show(false)*/
  // Creating a Closure
  def createDataFrameFromCSV: (String) => DataFrame = (inputFilePath: String) => {
    val df = sparkSession.read.text(inputFilePath)
    val schema = StructType(List(StructField("id", StringType , true),
      StructField("firstName", StringType , true),
      StructField("lastName", StringType , true),
      StructField("age", StringType , true),
      StructField("location", StringType , true)))
    val encoder = RowEncoder(schema)

    df.map((row)=> {
      val values = row.mkString(",").split(",")
      Row(values(0), values(1), values(2), values(3), values(4))
    })(encoder)
  }


  val df3 = createDataFrameFromRowList
  df3.show(false)
  def createDataFrameFromRowList: DataFrame = {
    val personList: List[Row] =
      List(Row("fn1", "ln1", 20, "M"),
        Row("fn2", "ln2", 21, "M"),
        Row("fn3", "ln3", 22, "F"),
        Row("fn4", "ln4", 23, "F"))

    val schema = StructType(List(
      StructField("first_name", StringType, true),
      StructField("last_name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("sex", StringType, true)
    ))

    val df = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(personList), schema)

    df
  }

}
