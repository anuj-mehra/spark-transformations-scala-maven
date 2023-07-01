package com.poc.spark.app2

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.JoinType._
import org.apache.spark.sql.functions.broadcast

object BroadcastDataFrame extends App{

  val spark: SparkSession = SparkSessionConfig("BroadcastDataframe", true).getSparkSession

  val obj = new BroadcastDataFrame
  obj.broadcastJoin(spark)

}

class BroadcastDataFrame {

  val broadcastJoin: (SparkSession) => Unit = (spark: SparkSession) => {
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
    import org.apache.spark.sql.functions.col
    val employeesDf = inputDataEmployeeDetails.toDF(schemaColumns1:_*).repartition(col("deptId"))
    val departmentDf = inputDataDepartmentDetails.toDF(schemaColumns2:_*).repartition(col("deptId"))

    val joinColumns  = Seq("deptId")
    val joinedDf = employeesDf.join(broadcast(departmentDf),joinColumns, "inner")
    joinedDf.show(false)
  }

}
