package com.poc.spark.app4

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Row, SparkSession}

object AddRankUsingWindows extends App{

  implicit val sparkSession: SparkSession = SparkSessionConfig("AddRankUsingWindows", true).getSparkSession
  val obj = new AddRankUsingWindows

  obj.addRank
}
class AddRankUsingWindows(implicit sparkSession: SparkSession) extends Serializable {

  def addRank: Unit = {

    val transactionsData = List(
      ("1", "anuj mehra", 2020, 100),
      ("2", "anuj mehra", 2020, 900),
      ("3", "priyanka goyal", 2020, 1000),
      ("4", "priyanka goyal", 2021, 2000),
      ("5", "priyanka goyal", 2022, 3000),
      ("6", "priyanka goyal", 2022, 4000),
      ("7", "mukesh ambani", 2022, 3000),
      ("7", "anuj mehra", 2022, 1000)
    )
    val schema = List("sno", "customername", "year", "amount")

    import sparkSession.implicits._
    val df = transactionsData.toDF(schema:_*)

    import org.apache.spark.sql.functions._
    val modifiedDf = df.select("customername","year","amount")
      .groupBy("customername","year").agg(sum("amount").as("totalamount"))

    //modifiedDf.show(false)

    // Define a Window specification
    val windowSpec = Window.partitionBy("customername","year").orderBy(desc("totalamount"))

    // Add a rank column based on year and amount spent
    val rankedDf = modifiedDf.withColumn("rank", rank().over(windowSpec))
    rankedDf.show(false)
  }

}
