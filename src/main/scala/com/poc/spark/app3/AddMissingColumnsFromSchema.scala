package com.poc.spark.app3

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object AddMissingColumnsFromSchema extends App{

  val schema = StructType(List(StructField("first_name",StringType, true),
    StructField("middle_name",StringType, true),
    StructField("last_name",StringType, true)))

  val updatedSchema = schema.add(StructField("age", IntegerType, true))

  println(updatedSchema)
}
