package com.st05

import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("CFMain")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val data = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/Reviews_100.csv")

    data.show()
    println(data.schema)



    spark.stop()
  }

}
