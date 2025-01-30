package com.poc.spark.app

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.{SaveMode, SparkSession}

object SaveAsAvro extends App{

  implicit val spark: SparkSession = SparkSessionConfig("GeneratePhysicalPlan", true).getSparkSession

  /*val data = List(("1", "FirstName1", "MiddleName1", "LastName1", "001"),
    ("2", "FirstName2", "MiddleName2","LastName2", "001"),
    ("3", "FirstName3", "MiddleName3","LastName3", "003"),
    ("4", "FirstName4", "MiddleName4","LastName4", "004"));

  val schema = List("EmpId", "FirstName", "MiddleName", "LastName", "DeptId")*/

  val data = List(("001", "Technology"),
    ("002", "Sales"),
    ("003", "HR"),
    ("004", "Finance"))

  val schema = List("DeptId", "DeptName")

  /*val data = List(("001", "India", "Pune"),
    ("001", "UK", "London"),
    ("003", "Germany", "Berlin"),
    ("004", "India", "Mumbai"))

  val schema = List("DeptId", "Country", "City")*/

  import spark.implicits._

  val df = data.toDF(schema:_*)
  df.show(false)

  df.write.mode(SaveMode.Overwrite).format("avro").save("/Users/anujmehra/git/apache-beam/src/main/resources/lineage/data/department")

}
