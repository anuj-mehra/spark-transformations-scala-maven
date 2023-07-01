package com.poc.spark.app

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.SparkSession

object PivotDataFrame extends App {

  val sparkSession = SparkSessionConfig("PivotDataFrame", true).getSparkSession

  val processor  = new PivotDataFrame
  processor pivotDf1 sparkSession

}
class PivotDataFrame extends Serializable {

  def pivotDf1 : (SparkSession) => Unit  = (spark: SparkSession) => {

    val data = Seq(("SecId1","Cusip","100"),
      ("SecId1","Isin","200"),
      ("SecId2","Cusip","300"),
      ("SecId2","Isin","400"),
      ("SecId3","Cusip","500"))

    import spark.sqlContext.implicits._
    val df = data.toDF("SecurityId","SecurityType","Value")
    df.show()

    import org.apache.spark.sql.functions._
    val df2 = df.groupBy("SecurityId").pivot(col("SecurityType")).agg(first("Value"))
    df2.show(false)
  }

  def pivotDf2 : (SparkSession) => Unit  = (spark: SparkSession) => {

    val data = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico"))

    import spark.sqlContext.implicits._
    val df = data.toDF("Product","Amount","Country")
    df.show()

    import org.apache.spark.sql.functions._
    val df2 = df.groupBy(col("Product")).pivot(col("Country")).sum("Amount")
    df2.show(false)
  }
}
