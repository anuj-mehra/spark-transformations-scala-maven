package com.poc.spark.app2

import com.poc.spark.config.SparkSessionConfig

object UsingGroupBy extends App{

  val spark = SparkSessionConfig("UsingGroupBy", true).getSparkSession

  method2

  def method1: Unit = {

    import spark.implicits._
    val simpleData = Seq(("James","Sales","NY",90000,34,10000),
      ("Michael","Sales","NY",86000,56,20000),
      ("Robert","Sales","CA",81000,30,23000),
      ("Maria","Finance","CA",90000,24,23000),
      ("Raman","Finance","CA",99000,40,24000),
      ("ASD","Finance","CA",99000,40,24000),
      ("Scott","Finance","NY",83000,36,19000),
      ("Jen","Finance","NY",79000,53,15000),
      ("Jeff","Marketing","CA",80000,25,18000),
      ("Kumar","Marketing","NY",91000,50,21000)
    )
    val df = simpleData.toDF("employee_name","department","state","salary","age","bonus")
    df.show()

    val groupedDf = df.groupBy("department","state").sum("salary")
    groupedDf.show(false)
  }

  def method2: Unit = {
    import spark.implicits._
    val simpleData = List(("James","Sales","NY",90000,34,10000),
      ("Michael","Sales","NY",86000,56,20000),
      ("Robert","Sales","CA",81000,30,23000),
      ("Maria","Finance","CA",90000,24,23000),
      ("Raman","Finance","CA",99000,40,24000),
      ("ASD","Finance","CA",99000,40,24000),
      ("Scott","Finance","NY",83000,36,19000),
      ("Jen","Finance","NY",79000,53,15000),
      ("Jeff","Marketing","CA",80000,25,18000),
      ("Kumar","Marketing","NY",91000,50,21000)
    )
    val schema = List("employee_name","department","state","salary","age","bonus")
    val df = simpleData.toDF(schema:_*)
    df.show()

    import org.apache.spark.sql.functions._
    val groupedDf = df.groupBy("department","state").agg(
      sum("salary").as("salary_sum"),
      avg("salary").as("avg_salary"),
      min("salary").as("min_salary"),
      max("salary").as("max_salary"),
    )
    groupedDf.show(false)

    groupedDf.printSchema()
  }


}
