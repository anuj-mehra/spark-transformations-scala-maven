package com.poc.spark.app

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType}

object NestedColumns extends App with Serializable {

  val sparkSession = SparkSessionConfig("NestedColumns", true).getSparkSession

  val processor = new NestedColumns
  processor createDataFrameWithNestedColumn sparkSession
}

class NestedColumns extends Serializable {

  def createDataFrameWithNestedColumn: (SparkSession) => Unit = (sparkSession: SparkSession) => {

    val inputdata = List(Row("1", Row("Anuj", "Mehra")),
      Row("2", Row("B", "Mehra")),
      Row("3", Row("C", "Mehra")),
      Row("4", Row("D", "Mehra")),
      Row("5", Row("E", "Mehra"))
    )

    val schema: StructType = new StructType()
      .add("S.No", StringType, true)
      .add("Name", new StructType()
        .add("FirstName", StringType)
        .add("LastName", StringType))

    import sparkSession.implicits._
    val df = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(inputdata), schema)
    df.printSchema()
    df.show(false)
  }

}
