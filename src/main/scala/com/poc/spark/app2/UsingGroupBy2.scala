package com.poc.spark.app2

import com.poc.spark.app2.UsingGroupBy.spark
import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.encoders.RowEncoder

object UsingGroupBy2 extends App{

  val spark = SparkSessionConfig("UsingGroupBy2", true).getSparkSession

  method1

  def method1: Unit = {

    import spark.implicits._
    val simpleData = Seq(("James", "Sales", "NY", 90000, 34, 10000),
      ("Michael", "Sales", "NY", 86000, 56, 20000),
      ("Robert", "Sales", "CA", 81000, 30, 23000),
      ("Maria", "Finance", "CA", 90000, 24, 23000),
      ("Raman", "Finance", "CA", 99000, 40, 24000),
      ("ASD", "Finance", "CA", 99000, 40, 24000),
      ("Scott", "Finance", "NY", 83000, 36, 19000),
      ("Jen", "Finance", "NY", 79000, 53, 15000),
      ("Jeff", "Marketing", "CA", 80000, 25, 18000),
      ("Kumar", "Marketing", "NY", 91000, 50, 21000)
    )
    val df = simpleData.toDF("employee_name", "department", "state", "salary", "age", "bonus")
    //df.show()

    import org.apache.spark.sql.functions._
    val df1: DataFrame = df.groupBy(col("department")).agg(
      sum("salary").as("salary_sum"),
      count("department").as("count_employees"),
      avg("age").as("average_age")
    )

    //df1.show(false)

    val encoder = RowEncoder(df1.schema)

    val df2 = df.groupBy(col("department")).agg(
      collect_list(col("employee_name")).as("employee_names")
    )
    /*df2.show(false)

    df2.printSchema*/

    val df3 = df.groupBy(col("department")).agg(
      concat_ws("_", collect_list(col("employee_name"))).as("employee_names")
    )
    //df3.show(false)

    val df4 = df.groupBy(col("department")).agg(
      collect_list(col("employee_name")).as("employee_names")
    ).withColumn("concatinated_names",concat_ws("_", col("employee_names")))

    //df4.show(false)

    val df5 = df.withColumn("scope", when(col("department") === "Sales", "Good")
    .when(col("department") === "Finance", "VGood").otherwise("Average"))

    df5.show(false)
  }

}
