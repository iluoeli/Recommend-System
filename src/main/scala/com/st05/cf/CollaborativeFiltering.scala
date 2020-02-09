package com.st05.cf

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

// CF
class CollaborativeFiltering extends Logging{

  def train(data: RDD[(Int, Int, Float)]): RDD[(Int, Array[(Int, Float)])] = {
    null
  }

  def train(dataset: Dataset[_]): Dataset[(Int, Array[(Int, Float)])] = {
    null
  }

}

class ItemCF extends CollaborativeFiltering {

  // Based on word co-occurrence
  override def train(data: RDD[(Int, Int, Float)]): RDD[(Int, Array[(Int, Float)])] = {
    // user-item matrix
    val userRecords = data.map{ case (userId, itemId, rating) => (userId, (itemId, rating)) }
      .groupByKey().persist()

    logWarning(s"number of userRecords: ${userRecords.count()}")

    // co-occurrence matrix
    val cooccurMat = userRecords.flatMap{ case (userId, iter) =>
      // making item pairs
      val itemsWithRating = iter.toArray
      var itemPairsWithFreq = ArrayBuffer[((Int, Int), Int)]()
      for ((item1, _) <- itemsWithRating) {
        for ((item2, _) <- itemsWithRating if item2 != item1) {
          val pairWithFreq = ((item1, item2), 1)
          itemPairsWithFreq += pairWithFreq
        }
      }
      itemPairsWithFreq
    }.reduceByKey(_ + _).map{ case ((item1Id, item2Id), freq) => (item1Id, (item2Id, freq))}

//    logWarning(s"co-occurrence matrix: ${cooccurMat.collect().mkString(", ")}")

    val itemUserMat = userRecords.flatMap{ case (userId, iter) =>
      iter.map{ case (itemId, rating) => (itemId, (userId, rating))}
    }

//    logWarning(s"item-user matrix: ${itemUserMat.collect().mkString(", ")}")

    val userWithRecommendations = itemUserMat.join(cooccurMat).map{ case (item1Id, ((userId, rating), (item2Id, freq))) =>
        val pref = rating * freq
      ((userId, item2Id), pref)
    }.reduceByKey(_ + _).map{ case ((userId, itemId), pref) => (userId, (itemId, pref))}
      .groupByKey().map{ case (userId, iter) =>
      val recommendations = iter.toArray.sortBy(-_._2).take(10) // TODO: parameter
      (userId, recommendations)
    }

    userWithRecommendations
  }

  override def train(dataset: Dataset[_]): Dataset[(Int, Array[(Int, Float)])] = {
    val rdd = dataset.rdd.map { case Row(userId: Int, itemId: Int, rating: Float) =>
      (userId, itemId, rating)
    }
    super.train(dataset)
  }

}

class ItemCosineSimCF extends CollaborativeFiltering {

  override def train(data: RDD[(Int, Int, Float)]): RDD[(Int, Array[(Int, Float)])] = {
    val userRecords = data.map{ case (userId, itemId, rating) => (userId, (itemId, rating)) }
      .groupByKey().persist()

    logWarning(s"number of userRecords: ${userRecords.count()}")

    val itemSimMap = userRecords.flatMap{ case (userId, iter) =>
        // making item pairs
        val itemsWithRating = iter.toArray
        var itemPairsWithRating = ArrayBuffer[((Int, Int), (Float, Float))]()
        for ((item1, rating1) <- itemsWithRating) {
          for ((item2, rating2) <- itemsWithRating if item2 != item1) {
            val pairWithRating = ((item1, item2), (rating1, rating2))
            itemPairsWithRating += pairWithRating
          }
        }
        // TODO: optimize
        itemPairsWithRating
    }.groupByKey().map{ case ((item1Id, item2Id), iter) =>
        var cnt = 0
        var (xx, yy, xy) = (1e-6, 1e-6, 0.0) // assign a particle to xx and yy to avoid overflow
        // TODO: fix the computation of cosine similarity
        for ((rating1, rating2) <- iter) {
          xx += rating1 * rating1
          yy += rating2 * rating2
          xy += rating1 * rating2
          cnt += 1
        }
        val sim = xy / math.sqrt(xx * yy)
        println(s"$item1Id, $item2Id, sim=$sim")
        (item1Id, (item2Id, sim, cnt))
    }.groupByKey().map{ case (itemId, iter) =>
        val topLinked = iter.toArray.sortBy(-_._2).take(10) // k-nearest neighbor
        (itemId, topLinked)
    }.collectAsMap()

    logWarning(s"item similarity map: ${itemSimMap.mkString(", ")}")

    // find candidates
    val sc = data.sparkContext
    val itemSimMapBC = sc.broadcast(itemSimMap)

    val userWithRecommendations = userRecords.map{ case (userId, iter) =>
      val itemSimMap = itemSimMapBC.value
      val candToConf = mutable.HashMap[Int, Float]()  // map candidates to confidence
      val candToSim = mutable.HashMap[Int, Float]()   // map candidates to similarity
      // TODO: drop purchased item ?
      for ((purchasedItemId, rating) <- iter) {
        val neighbours = itemSimMap.getOrElse(purchasedItemId, Array())
        for ((candItemId, sim, _) <- neighbours) {
          candToConf(candItemId) = (candToConf.getOrElse(candItemId, 0.0f) + sim.toFloat * rating)
          candToSim(candItemId) = candToSim.getOrElse(candItemId, 0.0f) + sim.toFloat
        }
      }

      // normalize and recommend
      val recommendations = candToConf.map{ case (itemId, confidence) => (itemId, confidence / candToSim(itemId))}
        .toArray.sortBy(-_._2).take(10) // TODO: 10 should be a parameter
      (userId, recommendations)
    }

    userWithRecommendations
  }
}