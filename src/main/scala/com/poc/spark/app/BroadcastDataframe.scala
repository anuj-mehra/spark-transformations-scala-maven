package com.poc.spark.app

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.SparkSession

object BroadcastDataframe extends App with Serializable {

  val spark = SparkSessionConfig("BroadcastDataframe", true).getSparkSession

  val processor  = new BroadcastDataframe
  processor process spark
}

class BroadcastDataframe extends Serializable {

  def process:(SparkSession) => Unit = (spark: SparkSession) => {

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

    import org.apache.spark.sql.functions._
    val joinedDf = df1.join(broadcast(df2), Seq("deptId"))
    joinedDf.show(false)

  }

}
