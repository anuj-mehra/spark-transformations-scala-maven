package com.poc.spark.app

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.SaveMode

object Test extends App{

  val sparkSession = SparkSessionConfig("Test", true).getSparkSession

  val data = List(("Delhi", "Delhi", "M", "Anuj"),
    ("Delhi", "Delhi", "M", "ABC"),
    ("Delhi", "OldDelhi", "M", "ABC"),
     ("Delhi", "Delhi", "F", "ASD"),
    ("UP", "Agra", "F", "PQR") ,
    ("MP", "Satna", "M", "ZXC"))

  val schema = List("state", "city", "gender", "name")

  import sparkSession.implicits._
  val df = data.toDF(schema:_*)

  df.show(false)

  df.write.partitionBy("state", "gender", "city").mode(SaveMode.Overwrite).parquet("/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/multi-col-partitioned--parquet")

  import org.apache.spark.sql.functions._
  //df.write.partitionBy("state").bucketBy(2, "gender").mode(SaveMode.Overwrite).parquet("/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/bucket-partitioned--parquet")
}
