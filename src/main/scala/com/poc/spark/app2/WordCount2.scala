package com.poc.spark.app2

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.DataFrame

object WordCount2 extends App{

  val sparkSession = SparkSessionConfig("WordCount2", true).getSparkSession

  import org.apache.spark.sql.functions._

  //usingSplitAndExplode
  //usingFlatMap
  usingFlatMapAndGroupBy

  private def usingSplitAndExplode: Unit = {

    // new way --> using split + explode
    val inputDf = sparkSession.read.text("/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/word-count.txt")
    inputDf.show(false)
    val explodeSplitDf = inputDf.withColumn("value", explode(split(col("value"), " "))).withColumn("count", lit(1))
    explodeSplitDf.show(false)

    val finalDf = explodeSplitDf.groupBy("value").sum("count")
    finalDf.show(false)

  }

  private def usingFlatMap: Unit = {
    import sparkSession.implicits._
    val inputDf = sparkSession.read.text("/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/word-count.txt")
    val flatMapDf = inputDf.flatMap((row) => {
      val values = row.mkString(" ").split(" ")

      values
    }).withColumn("count", lit(1)).groupBy("value").sum("count").sort("sum(count)")

    flatMapDf.show(false)

  }

  private def usingFlatMapAndGroupBy: Unit = {
    import sparkSession.implicits._
    val inputDf = sparkSession.read.text("/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/word-count.txt")
    val flatMapDf = inputDf.flatMap((row) => {
      val values = row.mkString(" ").split(" ")

      values
    }).groupBy("value").count()

    flatMapDf.show(false)

  }

}
