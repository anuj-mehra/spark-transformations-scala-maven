package com.poc.spark.app

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Encode

object GroupAndReduce extends App{

  val sparkSession = SparkSessionConfig("GroupAndReduce", true).getSparkSession

  val processor  = new GroupAndReduce
  processor doGroupByKey sparkSession
}

class GroupAndReduce {

  /* The type returned by groupBy is RelationalGroupedDataset.
    GroupBy:** groupBy is similar to the group by clause in traditional SQL language,
    but the difference is that groupBy() can group multiple columns with multiple column names.
    For example, you can do groupBy according to "id" and "name".
     */
  def doGroupBy : (SparkSession) => Unit  = (spark: SparkSession) => {

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

    import org.apache.spark.sql.functions._
    /*val df2 = df.groupBy("department").sum("salary")
    df2.show(false)

    val df3 = df.groupBy("department").agg(sum("salary").as("salary_sum"))
    df3.show(false)*/

    val df4 = df.groupBy("department").agg(count("department"))
    df4.show(false)

    df.agg(max("salary")).show(false)
  }

  // But unlike groupBy, the type returned by groupByKey is KeyValueGroupedDataset.
  def doGroupByKey : (SparkSession) => Unit  = (spark: SparkSession) => {
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

   /* import org.apache.spark.sql.functions._
    val df = df.groupByKey((row) => {
      row.getAsString("department")
    })(Encode)*/


  }

  def doReduceBy : (SparkSession) => Unit  = (spark: SparkSession) => {

  }
}
