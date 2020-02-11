package com.st05.cf

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


class JaccardItemCF(private val numRecommendations: Int) extends Logging with Serializable {

    def this() = this(10)

    def train(data: RDD[(Int, Int, Float)]): RDD[(Int, Array[(Int, Float)])] = {
      val sc = data.sparkContext
      // handle persist
      val storageLevel = data.getStorageLevel
      if (storageLevel == StorageLevel.NONE) {
        logWarning("The input data is not directly cached, which may hurt performance if its"
          + " parent RDDs are also uncached.")
      }

      val userRecords = data.map{ case (userId, itemId, rating) => (userId, (itemId, rating)) }
        .groupByKey().persist(StorageLevel.MEMORY_AND_DISK)

      // collect the number of times items were used
      val itemToNum = userRecords.flatMap{ case (_, iter) =>
        iter.map{ case (itemId, _) => (itemId, 1)} // TODO: support numerical rating (now boolean)
      }.reduceByKey(_ + _).collectAsMap()

      val itemToNumBC = sc.broadcast(itemToNum)

      val itemSimMap = userRecords.flatMap{ case (_, iter) =>
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
      }.reduceByKey(_ + _).map{ case ((item1Id, item2Id), freq) =>
        val itemToNum = itemToNumBC.value
        val item1Freq = itemToNum(item1Id)
        val item2Freq = itemToNum(item2Id)
        // NOTE: Jaccard Similarity of (X, Y) = |X intersect Y| / |X union Y|
        val jaccardSim = freq.toFloat / (item1Freq + item2Freq - freq)
        (item1Id, (item2Id, jaccardSim))
      }.groupByKey().map{ case (itemId, iter) =>
        val topLinked = iter.toArray.sortBy(-_._2).take(10)
        (itemId, topLinked)
      }.collectAsMap()

      val itemSimMapBC = sc.broadcast(itemSimMap)

      val userWithRecommendations = userRecords.map{ case (userId, iter) =>
        val itemSimMap = itemSimMapBC.value
        val candToConf = mutable.HashMap[Int, Float]()  // map candidates to confidence
        // TODO: drop purchased item ?
        for ((purchasedItemId, rating) <- iter) {
          val neighbours = itemSimMap.getOrElse(purchasedItemId, Array())
          for ((candItemId, sim) <- neighbours) {
            // TODO: min-heap
            candToConf(candItemId) = math.max(candToConf.getOrElse(candItemId, 0.0f), sim.toFloat * rating)
          }
        }
        val recommendations = candToConf.toArray.sortBy(-_._2).take(numRecommendations)
        (userId, recommendations)
      }

      userWithRecommendations
    }
  }