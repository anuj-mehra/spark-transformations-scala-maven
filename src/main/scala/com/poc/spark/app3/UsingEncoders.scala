package com.poc.spark.app3

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object UsingEncoders  extends App{

  val sparkSession = SparkSessionConfig("WordCount", true).getSparkSession

  val data = List(("a1", "b1", "c1"), ("a2", "b2", "c2"), ("a3","b3","c3"))
  val schema = List("cola", "colb", "colc")

  import sparkSession.implicits._
  val df = data.toDF(schema:_*)

  //df.show(false)

  val newSchema: StructType = df.schema.add(StructField("cold", StringType, true))
  val encoder = RowEncoder(newSchema)

  val df2 = df.map(row => {

    Row(row.get(0), row.get(1), row.get(2), convertToUpper(row.getAs[String](2)))
  })(encoder)

  def convertToUpper: (String) => String = {
    _.toUpperCase
  }

  //df2.show(false)

  import sparkSession.implicits._
  val df3 = df.map(row => {

    (row.getAs[String](0), row.getAs[String](1), row.getAs[String](2), convertToUpper(row.getAs[String](2)))
  }).toDF("ncola", "ncolb", "ncolc", "ncold")

  df3.show(false)
}
