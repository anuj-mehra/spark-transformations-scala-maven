package com.poc.spark.app

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.SparkSession

//https://sqlandhadoop.com/spark-dataframe-groupby-aggregate-functions/
object AggregateOperations extends App{
  val sparkSession = SparkSessionConfig("AggregateOperations", true).getSparkSession

  val processor  = new AggregateOperations
  processor findSum sparkSession
}

class AggregateOperations {

  def findSum : (SparkSession) => Unit  = (spark: SparkSession) => {

    val simpleData = Seq(("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("James", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100)
    )
    import spark.implicits._
    val df = simpleData.toDF("employee_name", "department", "salary")
    df.show()

  }

}
