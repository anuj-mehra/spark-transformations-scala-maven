package com.poc.spark.app

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.SparkSession

object JoinDataframesOnMultiplecolumns extends App{

  val sparkSession = SparkSessionConfig("JoinDataframes", true).getSparkSession

  val processor  = new JoinDataframesOnMultiplecolumns
  processor joinOnMultipleColumns2 sparkSession

}

class JoinDataframesOnMultiplecolumns {

  def joinOnMultipleColumns:(SparkSession) => Unit = (spark: SparkSession) => {

    val inputDataEmployeeDetails = List(("Pune", "100", "A","Mehra"),
      ("Pune", "100","B","Mehra"),
      ("Hyderabad", "200","C","Mehra"),
      ("Hyderabad", "300","D","Mehra"),
      ("Hyderabad", "400","E","Mehra"))
    val schemaColumns1 = List("city", "deptId","fName", "lName")

    val inputDataDepartmentDetails = List(("100", "Software", "Hyderabad"),
      ("200","Finance", "Hyderabad"),
      ("300","Management", "Pune"),
      ("400","Sales", "Pune"),
      ("500","HR", "Pune"))
    val schemaColumns2 = List("deptId", "deptName", "city")

    import spark.implicits._
    val df1 = inputDataEmployeeDetails.toDF(schemaColumns1:_*)
    val df2 = inputDataDepartmentDetails.toDF(schemaColumns2:_*)

    val joinedDf = df1.join(df2, Seq("deptId", "city") ,"rightouter")
    joinedDf.show(false)
  }

  def joinOnMultipleColumns2:(SparkSession) => Unit = (spark: SparkSession) => {

    val inputDataEmployeeDetails = List(("Pune", "100", "A","Mehra"),
      ("Pune", "100","B","Mehra"),
      ("Hyderabad", "200","C","Mehra"),
      ("Hyderabad", "300","D","Mehra"),
      ("Hyderabad", "400","E","Mehra"))
    val schemaColumns1 = List("city", "deptId","fName", "lName")

    val inputDataDepartmentDetails = List(("100", "Software", "Hyderabad"),
      ("200","Finance", "Hyderabad"),
      ("300","Management", "Pune"),
      ("400","Sales", "Pune"),
      ("500","HR", "Pune"))
    val schemaColumns2 = List("deptId2", "deptName", "city2")

    import spark.implicits._
    val df1 = inputDataEmployeeDetails.toDF(schemaColumns1:_*)
    val df2 = inputDataDepartmentDetails.toDF(schemaColumns2:_*)

    val joinedDf = df1.join(df2, df1("deptId") === df2("deptId2") && df1("city") === df2("city2"),"rightouter")
      .drop(Seq("deptId2", "city2"):_*)
    joinedDf.show(false)
  }
}
