/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.clustering

import breeze.linalg.{Vector => BV}

import scala.reflect.ClassTag
import scala.util.Random._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext._

/**
 * :: DeveloperApi ::
 *
 * StreamingKMeansModel extends MLlib's KMeansModel for streaming
 * algorithms, so it can keep track of the number of points assigned
 * to each cluster, and also update the model by doing a single iteration
 * of the standard KMeans algorithm.
 *
 * The update algorithm uses the "mini-batch" KMeans rule,
 * generalized to incorporate forgetfullness (i.e. decay).
 * The basic update rule (for each cluster) is:
 *
 * c_t+1 = [(c_t * n_t) + (x_t * m_t)] / [n_t + m_t]
 * n_t+t = n_t + m_t
 *
 * Where c_t is the previously estimated centroid for that cluster,
 * n_t is the number of points assigned to it thus far, x_t is the centroid
 * estimated on the current batch, and m_t is the number of points assigned
 * to that centroid in the current batch.
 *
 * This update rule is modified with a decay factor 'a' that scales
 * the contribution of the clusters as estimated thus far.
 * If a=1, all batches are weighted equally. If a=0, new centroids
 * are determined entirely by recent data. Lower values correspond to
 * more forgetting.
 *
 * Decay can optionally be specified as a decay fraction 'q',
 * which corresponds to the fraction of batches (or points)
 * after which the past will be reduced to a contribution of 0.5.
 * This decay fraction can be specified in units of 'points' or 'batches'.
 * if 'batches', behavior will be independent of the number of points per batch;
 * if 'points', the expected number of points per batch must be specified.
 */
@DeveloperApi
class StreamingKMeansModel(
    override val clusterCenters: Array[Vector],
    val clusterCounts: Array[Long]) extends KMeansModel(clusterCenters) {

  // do a sequential KMeans update on a batch of data
  def update(data: RDD[Vector], a: Double, units: String): StreamingKMeansModel = {

    val centers = clusterCenters
    val counts = clusterCounts

    // find nearest cluster to each point
    val closest = data.map(point => (this.predict(point), (point.toBreeze, 1.toLong)))

    // get sums and counts for updating each cluster
    type WeightedPoint = (BV[Double], Long)
    def mergeContribs(p1: WeightedPoint, p2: WeightedPoint): WeightedPoint = {
      (p1._1 += p2._1, p1._2 + p2._2)
    }
    val pointStats: Array[(Int, (BV[Double], Long))] =
      closest.reduceByKey{mergeContribs}.collectAsMap().toArray

    // implement update rule
    for (newP <- pointStats) {
      // store old count and centroid
      val oldCount = counts(newP._1)
      val oldCentroid = centers(newP._1).toBreeze
      // get new count and centroid
      val newCount = newP._2._2
      val newCentroid = newP._2._1 / newCount.toDouble
      // compute the normalized scale factor that controls forgetting
      val decayFactor = units match {
        case "batches" =>  newCount / (a * oldCount + newCount)
        case "points" => newCount / (math.pow(a, newCount) * oldCount + newCount)
      }
      // perform the update
      val updatedCentroid = oldCentroid + (newCentroid - oldCentroid) * decayFactor
      // store the new counts and centers
      counts(newP._1) = oldCount + newCount
      centers(newP._1) = Vectors.fromBreeze(updatedCentroid)
    }

    new StreamingKMeansModel(centers, counts)
  }

}

@DeveloperApi
class StreamingKMeans(
     var k: Int,
     var a: Double,
     var units: String) extends Logging {

  protected var model: StreamingKMeansModel = new StreamingKMeansModel(null, null)

  def this() = this(2, 1.0, "batches")

  /** Set the number of clusters. */
  def setK(k: Int): this.type = {
    this.k = k
    this
  }

  /** Set the decay factor directly (for forgetful algorithms). */
  def setDecayFactor(a: Double): this.type = {
    this.a = a
    this
  }

  /** Set the decay units for forgetful algorithms ("batches" or "points"). */
  def setUnits(units: String): this.type = {
    if (units != "batches" && units != "points") {
      throw new IllegalArgumentException("Invalid units for decay: " + units)
    }
    this.units = units
    this
  }

  /** Set decay fraction in units of batches. */
  def setDecayFractionBatches(q: Double): this.type = {
    this.a = math.log(1 - q) / math.log(0.5)
    this.units = "batches"
    this
  }

  /** Set decay fraction in units of points. Must specify expected number of points per batch. */
  def setDecayFractionPoints(q: Double, m: Double): this.type = {
    this.a = math.pow(math.log(1 - q) / math.log(0.5), 1/m)
    this.units = "points"
    this
  }

  /** Specify initial explicitly directly. */
  def setInitialCenters(initialCenters: Array[Vector]): this.type = {
    val clusterCounts = Array.fill(this.k)(0).map(_.toLong)
    this.model = new StreamingKMeansModel(initialCenters, clusterCounts)
    this
  }

  /** Initialize random centers, requiring only the number of dimensions. */
  def setRandomCenters(d: Int): this.type = {
    val initialCenters = (0 until k).map(_ => Vectors.dense(Array.fill(d)(nextGaussian()))).toArray
    val clusterCounts = Array.fill(0)(d).map(_.toLong)
    this.model = new StreamingKMeansModel(initialCenters, clusterCounts)
    this
  }

  /** Return the latest model. */
  def latestModel(): StreamingKMeansModel = {
    model
  }

  /**
   * Update the clustering model by training on batches of data from a DStream.
   * This operation registers a DStream for training the model,
   * checks whether the cluster centers have been initialized,
   * and updates the model using each batch of data from the stream.
   *
   * @param data DStream containing vector data
   */
  def trainOn(data: DStream[Vector]) {
    this.isInitialized
    data.foreachRDD { (rdd, time) =>
      model = model.update(rdd, this.a, this.units)
    }
  }

  /**
   * Use the clustering model to make predictions on batches of data from a DStream.
   *
   * @param data DStream containing vector data
   * @return DStream containing predictions
   */
  def predictOn(data: DStream[Vector]): DStream[Int] = {
    this.isInitialized
    data.map(model.predict)
  }

  /**
   * Use the model to make predictions on the values of a DStream and carry over its keys.
   *
   * @param data DStream containing (key, feature vector) pairs
   * @tparam K key type
   * @return DStream containing the input keys and the predictions as values
   */
  def predictOnValues[K: ClassTag](data: DStream[(K, Vector)]): DStream[(K, Int)] = {
    this.isInitialized
    data.mapValues(model.predict)
  }

  /**
   * Check whether cluster centers have been initialized.
   *
   * @return Boolean, True if cluster centrs have been initialized
   */
  def isInitialized: Boolean = {
    if (Option(model.clusterCenters) == None) {
      logError("Initial cluster centers must be set before starting predictions")
      throw new IllegalArgumentException
    } else {
      true
    }
  }

}
