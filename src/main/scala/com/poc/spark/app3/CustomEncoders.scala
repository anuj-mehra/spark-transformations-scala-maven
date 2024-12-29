package com.poc.spark.app3

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField}

object CustomEncoders extends App{

  val sparkSession: SparkSession = SparkSessionConfig("CreateDataframe", true).getSparkSession

  val data = List(("anuj", "mehra", "a"),
    ("kiyansh", "mehra", "b"),
    ("priyanka", "goyal", "c"))

  val schema = List("first_name", "last_name", " code")

  import sparkSession.implicits._
  val df = data.toDF(schema:_*)
  //df.show(false)

  val existingSchema = df.schema
  val updatedSchema = existingSchema.add(StructField("city", StringType, true))

  val rowencoder = RowEncoder(updatedSchema)

  val df2 = df.map(row => {

    Row(row.getAs[String](0), row.getAs[String](1), row.getAs[String](2), "Pune")
  })(rowencoder)

  df2.show(false)


}
