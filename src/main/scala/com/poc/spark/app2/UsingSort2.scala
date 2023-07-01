package com.poc.spark.app2

import com.poc.spark.app2.UsingSort.sparkSession
import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.DataFrame

object UsingSort2 extends App{

  val sparkSession = SparkSessionConfig("UsingSort2", true).getSparkSession

  import org.apache.spark.sql.functions._
  method1

  def method1: Unit = {

    val sampleData = List(("James", "Sales", "NY", 90000, 34, 10000),
      ("Michael", "Sales", "NY", 86000, 56, 20000),
      ("Robert", "Sales", "CA", 81000, 30, 23000),
      ("Maria", "Finance", "CA", 90000, 24, 23500),
      ("Raman", "Finance", "CA", 99000, 40, 24500),
      ("ASD", "Finance", "CA", 99000, 40, 24000),
      ("Scott", "Finance", "NY", 83000, 36, 19000),
      ("Jen", "Finance", "NY", 79000, 53, 15000),
      ("Jeff", "Marketing", "CA", 80000, 25, 18000),
      ("Kumar", "Marketing", "NY", 91000, 50, 21000)
    )

    val schema = List("employee_name", "department", "state", "salary", "age", "bonus")

    import sparkSession.implicits._
    val df = sampleData.toDF(schema:_*)
    import org.apache.spark.sql.functions._

    val df1 = df.sort(col("age").asc).coalesce(1)
    df1.show(false)

    df1.foreach((row) => {
      println(row.getAs[String]("age"))
    })

    (df1.isEmpty) match {
      case true =>
        println("--dataframe is empty--")
      case false =>
        println("--dataframe is NOT empty--")
    }
  }


}
