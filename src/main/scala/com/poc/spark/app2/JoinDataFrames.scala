package com.poc.spark.app2

import com.poc.spark.app2.JoinDataFrames.sparkSession
import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.SparkSession

object JoinDataFrames extends App{

  val sparkSession: SparkSession = SparkSessionConfig("JoinDataframes", true).getSparkSession

  val obj = new JoinDataFrames

  obj def1 sparkSession
}

class JoinDataFrames {

  val def1: (SparkSession) => Unit = (spark: SparkSession) => {
    val inputDataEmployeeDetails = List(("1", "100", "A","Mehra"),
      ("2", "100","B","Mehra"),
      ("3", "200","C","Mehra"),
      ("4", "300","D","Mehra"),
      ("5", "400","E","Mehra"))

    val schemaColumns1 = List("empId", "deptId","fName", "lName")

    val inputDataDepartmentDetails = List(("100", "Software"),
      ("200","Finance"),
      ("300","Management"),
      ("400","Sales"),
      ("500","HR"))

    val schemaColumns2 = List("deptId", "deptName")

    import spark.implicits._
    val df1 = inputDataEmployeeDetails.toDF(schemaColumns1:_*)
    val df2 = inputDataDepartmentDetails.toDF(schemaColumns2:_*)

    val joinedDf = df2.join(df1, Seq("deptId"), "leftanti")
    joinedDf.show(false)
  }

}
