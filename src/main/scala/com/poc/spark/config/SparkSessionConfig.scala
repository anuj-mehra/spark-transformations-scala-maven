package com.poc.spark.config

import org.apache.spark.sql._

class SparkSessionConfig(appName: String, isLocal: Boolean) {

  private val sparkBuilder = SparkSession.builder().appName(appName)
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.sql.broadcastTimeout", "36000")
    .config("spark.driver.maxResultSize", 0)
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.join.preferSortMergeJoin", "false")
    .config("avro.mapred.ignore.inputs.without.extension", "true")
  // Sorting is not needed with Shuffle Hash Joins inside the partitions.
  //sparkBuilder.config("spark.sql.join.preferSortMergeJoin", "false")

  if (isLocal) {
    sparkBuilder.master("local[*]")
  }

  private val spark: SparkSession = sparkBuilder.getOrCreate()
  //spark.sparkContext.hadoopConfiguration.set("avro.mapred.ignore.inputs.without.extension", "false")


  def getSparkSession: SparkSession = {
    spark
  }

  def close: Unit = {
    spark.close()
  }

}

object SparkSessionConfig {

  def apply(appName: String) = new SparkSessionConfig(appName, false)

  def apply(appName: String, isLocal: Boolean) = new SparkSessionConfig(appName, isLocal)
}
