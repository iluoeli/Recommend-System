package com.st05.cf

import com.st05.SparkBaseSuite
import org.apache.spark.rdd.RDD

class JaccardItemCFSuite extends SparkBaseSuite {
  var data: RDD[(Int, Int, Float)] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    data = sc.parallelize(Seq(
    // (userId, productId, score)
      (1, 0, 1),
      (1, 2, 1),
      (2, 1, 1),
      (3, 0, 1),
      (3, 1, 1),
      (3, 2, 1)
    ))
  }

  test("correctness of jaccard-similarity based method") {
    val cf = new JaccardItemCF[Int, Int]()
    val userWithRecommendations = cf.train(data).collect()

    for ((userId, recmds) <- userWithRecommendations) {
      println(s"user=$userId\trecommendations=${recmds.mkString(",")}")
    }
  }
}
