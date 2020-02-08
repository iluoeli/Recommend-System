package cf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

// CF
class CollaborativeFiltering {

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

    // co-occurrence matrix
    val cooccurMat = userRecords.flatMap{ case (userId, iter) =>
      // making item pairs
      val itemsWithRating = iter.toArray
      val itemPairsWithFreq = ArrayBuffer[((Int, Int), Int)]()
      for ((item1, _) <- itemsWithRating) {
        for ((item2, _) <- itemsWithRating) {
          val pairWithFreq = ((item1, item2), 1)
          itemPairsWithFreq :+ pairWithFreq
        }
      }
      itemPairsWithFreq
    }.reduceByKey(_ + _).map{ case ((item1Id, item2Id), freq) => (item1Id, (item2Id, freq))}

    val itemUserMat = userRecords.flatMap{ case (userId, iter) =>
      iter.map{ case (itemId, rating) => (itemId, (userId, rating))}
    }

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

    val itemSimMap = userRecords.flatMap{ case (userId, iter) =>
        // making item pairs
        val itemsWithRating = iter.toArray
        val itemPairsWithRating = ArrayBuffer[((Int, Int), (Float, Float))]()
        for ((item1, rating1) <- itemsWithRating) {
          for ((item2, rating2) <- itemsWithRating) {
            val pairWithRating = ((item1, item2), (rating1, rating2))
            itemPairsWithRating :+ pairWithRating
          }
        }
        // TODO: optimize
        itemPairsWithRating
    }.groupByKey().map{ case ((item1Id, item2Id), iter) =>
        var cnt = 0
        var (xx, yy, xy) = (1e-6, 1e-6, 0.0) // assign a particle to xx and yy to avoid overflow
        for ((rating1, rating2) <- iter) {
          xx += rating1 * rating1
          yy += rating2 * rating2
          xy += rating1 * rating2
          cnt += 1
        }
        val sim = xy / math.sqrt(xx * yy)
        (item1Id, (item2Id, sim, cnt))
    }.groupByKey().map{ case (itemId, iter) =>
        val topLinked = iter.toArray.sortBy(-_._2).take(10) // k-nearest neighbor
        (itemId, topLinked)
    }.collectAsMap()

    // find candidates
    val sc = data.sparkContext
    val itemSimMapBC = sc.broadcast(itemSimMap)

    val userWithRecommendations = userRecords.map{ case (userId, iter) =>
      val itemSimMap = itemSimMapBC.value
      val candToConf = mutable.HashMap[Int, Float]()  // map candidates to confidence
      val candToSim = mutable.HashMap[Int, Float]()   // map candidates to similarity
      // TODO: drop purchased item ?
      for ((purchasedItemId, rating) <- iter) {
        for ((candItemId, sim, _) <- itemSimMap(purchasedItemId)) {
          candToConf(candItemId) += sim * rating
          candToSim(candItemId) += sim
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