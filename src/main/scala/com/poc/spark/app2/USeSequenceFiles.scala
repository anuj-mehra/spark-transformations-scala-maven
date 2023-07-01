package com.poc.spark.app2

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

import scala.reflect.ClassTag

case class AccountsData(empId: String, deptId: String, fName: String, lName: String)
case class PositionsData(positionId: String, date: String)

object UseSequenceFiles extends App{

  implicit val sparkSession: SparkSession = SparkSessionConfig("UseSequenceFiles", true).getSparkSession
  import sparkSession.implicits._
  import org.apache.spark.sql.functions._

  val accountdata = List(AccountsData("1", "100", "FN1", "LN1"),
    AccountsData("2", "100", "FN2", "LN2"))
  val accountsDf = accountdata.toDF()

  val positionsData = List(PositionsData("10011001", "01-Jan-2022"),
    PositionsData("20012001", "02-Jan-2022"))
  val positionsDf = positionsData.toDF()

  implicit def single[A](implicit c: ClassTag[A]): Encoder[A] = Encoders.kryo[A](c)
  implicit def tuple2[A1, A2](implicit e1: Encoder[A1],
                              e2: Encoder[A2]
                             ): Encoder[(A1, A2)] = Encoders.tuple[A1, A2](e1, e2)


  val sequenceFileSchema = List("key", "actual-dataframe")
  val df1 = List(("accounts-data", accountsDf)).toDF(sequenceFileSchema:_*)
  val df2 = List(("positions-data", positionsData)).toDF(sequenceFileSchema:_*)

  val combinedDf = df1.unionByName(df2)

  /* val convertAccountType : (Array[Byte] => AccountsData) = { byteArray =>
    import java.io.ByteArrayInputStream
    import java.io.ObjectInputStream
    val in = new ByteArrayInputStream(byteArray)
    val is = new ObjectInputStream(in)
    val accountsData = is.readObject.asInstanceOf[AccountsData]
    accountsData
  }*/

  combinedDf.show(false)

  /* val convertAccountTypeUdf = udf(convertAccountType)

  combinedDf.show()*/

  /*val acctRetainedDf1 = combinedDf
    .filter(col("key") === "accounts-data")

  acctRetainedDf1.printSchema()*/

  //val acctRetainedDf2 = df1.filter(col("seq-file-key") === "account-list-2").select(col("actual-dataframe"))

 /*
  val rdd1: RDD[(String, String)] = inputData._1.withColumn("file_name", lit("accounts-file-1")).rdd.map((row) => {
    (row.getAs[String](0), row.getAs[String](1))
  })
  val rdd2: RDD[(String, String)] = inputData._2.withColumn("file_name", lit("accounts-file-2")).rdd.map((row) => {
    (row.getAs[String](0), row.getAs[String](1))
  })

  rdd1.saveAsSequenceFile(“output.seq”)*/

}
