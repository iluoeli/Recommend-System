package com.st05

import com.st05.cf.JaccardItemCF
import org.apache.spark.sql.{Row, SparkSession}

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
      .select("userId", "productId", "score")
      .rdd.map{ row => (row.getString(0), row.getString(1), row.getInt(2).toFloat)}

    val cf = new JaccardItemCF[String, String]()
    val userWithRecmds = cf.train(data).collect()

    for ((userId, recmds) <- userWithRecmds) {
      println(s"user=$userId\trecommendations=${recmds.mkString(",")}")
    }

    spark.stop()
  }

}
