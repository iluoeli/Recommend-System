package com.st05

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SparkBaseSuite extends FunSuite with BeforeAndAfterAll {
  var sc: SparkContext = _

  override def beforeAll(): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getCanonicalName)
    sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
  }

  override def afterAll(): Unit = {
    if (sc != null) {
      sc.stop()
    }
  }

//  test("spark base test") {
//    val data = sc.parallelize(0 until 10)
//    assert(data.count() == 10)
//  }
}
