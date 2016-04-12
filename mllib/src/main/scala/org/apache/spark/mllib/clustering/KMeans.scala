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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.BLAS.{axpy, gemm, scal}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils
import org.apache.spark.util.random.XORShiftRandom

/**
 * K-means clustering with support for multiple parallel runs and a k-means++ like initialization
 * mode (the k-means|| algorithm by Bahmani et al). When multiple concurrent runs are requested,
 * they are executed together with joint passes over the data for efficiency.
 *
 * This is an iterative algorithm that will make multiple passes over the data, so any RDDs given
 * to it should be cached by the user.
 */
@Since("0.8.0")
class KMeans private (
    private var k: Int,
    private var maxIterations: Int,
    private var runs: Int,
    private var initializationMode: String,
    private var initializationSteps: Int,
    private var epsilon: Double,
    private var seed: Long,
    private var blockSize: Int) extends Serializable with Logging {

  /**
   * Constructs a KMeans instance with default parameters: {k: 2, maxIterations: 20, runs: 1,
   * initializationMode: "k-means||", initializationSteps: 5, epsilon: 1e-4, seed: random}.
   */
  @Since("0.8.0")
  def this() = this(2, 20, 1, KMeans.K_MEANS_PARALLEL, 5, 1e-4, Utils.random.nextLong(), 128)

  /**
   * Number of clusters to create (k).
   */
  @Since("1.4.0")
  def getK: Int = k

  /**
   * Set the number of clusters to create (k). Default: 2.
   */
  @Since("0.8.0")
  def setK(k: Int): this.type = {
    require(k > 0,
      s"Number of clusters must be positive but got ${k}")
    this.k = k
    this
  }

  /**
   * Maximum number of iterations allowed.
   */
  @Since("1.4.0")
  def getMaxIterations: Int = maxIterations

  /**
   * Set maximum number of iterations allowed. Default: 20.
   */
  @Since("0.8.0")
  def setMaxIterations(maxIterations: Int): this.type = {
    require(maxIterations >= 0,
      s"Maximum of iterations must be nonnegative but got ${maxIterations}")
    this.maxIterations = maxIterations
    this
  }

  /**
   * The initialization algorithm. This can be either "random" or "k-means||".
   */
  @Since("1.4.0")
  def getInitializationMode: String = initializationMode

  /**
   * Set the initialization algorithm. This can be either "random" to choose random points as
   * initial cluster centers, or "k-means||" to use a parallel variant of k-means++
   * (Bahmani et al., Scalable K-Means++, VLDB 2012). Default: k-means||.
   */
  @Since("0.8.0")
  def setInitializationMode(initializationMode: String): this.type = {
    KMeans.validateInitMode(initializationMode)
    this.initializationMode = initializationMode
    this
  }

  /**
   * :: Experimental ::
   * Number of runs of the algorithm to execute in parallel.
   */
  @Since("1.4.0")
  @deprecated("Support for runs is deprecated. This param will have no effect in 2.0.0.", "1.6.0")
  def getRuns: Int = runs

  /**
   * :: Experimental ::
   * Set the number of runs of the algorithm to execute in parallel. We initialize the algorithm
   * this many times with random starting conditions (configured by the initialization mode), then
   * return the best clustering found over any run. Default: 1.
   */
  @Since("0.8.0")
  @deprecated("Support for runs is deprecated. This param will have no effect in 2.0.0.", "1.6.0")
  def setRuns(runs: Int): this.type = {
    internalSetRuns(runs)
  }

  // Internal version of setRuns for Python API, this should be removed at the same time as setRuns
  // this is done to avoid deprecation warnings in our build.
  private[mllib] def internalSetRuns(runs: Int): this.type = {
    if (runs <= 0) {
      throw new IllegalArgumentException("Number of runs must be positive")
    }
    if (runs != 1) {
      logWarning("Setting number of runs is deprecated and will have no effect in 2.0.0")
    }
    this.runs = runs
    this
  }

  /**
   * Number of steps for the k-means|| initialization mode
   */
  @Since("1.4.0")
  def getInitializationSteps: Int = initializationSteps

  /**
   * Set the number of steps for the k-means|| initialization mode. This is an advanced
   * setting -- the default of 5 is almost always enough. Default: 5.
   */
  @Since("0.8.0")
  def setInitializationSteps(initializationSteps: Int): this.type = {
    require(initializationSteps > 0,
      s"Number of initialization steps must be positive but got ${initializationSteps}")
    this.initializationSteps = initializationSteps
    this
  }

  /**
   * The distance threshold within which we've consider centers to have converged.
   */
  @Since("1.4.0")
  def getEpsilon: Double = epsilon

  /**
   * Set the distance threshold within which we've consider centers to have converged.
   * If all centers move less than this Euclidean distance, we stop iterating one run.
   */
  @Since("0.8.0")
  def setEpsilon(epsilon: Double): this.type = {
    require(epsilon >= 0,
      s"Distance threshold must be nonnegative but got ${epsilon}")
    this.epsilon = epsilon
    this
  }

  /**
   * The random seed for cluster initialization.
   */
  @Since("1.4.0")
  def getSeed: Long = seed

  /**
   * Set the random seed for cluster initialization.
   */
  @Since("1.4.0")
  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  /**
   * Block size for stacking input data in matrices to speed up the computation.
   * Data is stacked within partitions. If block size is more than remaining data in
   * a partition then it is adjusted to the size of this data.
   * Recommended size is between 10 and 1000.
   */
  @Since("2.0.0")
  def getBlockSize: Int = blockSize

  /**
   * Set the block size for stacking input data in matrices.
   */
  @Since("2.0.0")
  def setBlockSize(blockSize: Int): this.type = {
    this.blockSize = blockSize
    this
  }

  // Initial cluster centers can be provided as a KMeansModel object rather than using the
  // random or k-means|| initializationMode
  private var initialModel: Option[KMeansModel] = None

  /**
   * Set the initial starting point, bypassing the random initialization or k-means||
   * The condition model.k == this.k must be met, failure results
   * in an IllegalArgumentException.
   */
  @Since("1.4.0")
  def setInitialModel(model: KMeansModel): this.type = {
    require(model.k == k, "mismatched cluster count")
    initialModel = Some(model)
    this
  }

  /**
   * Train a K-means model on the given set of points; `data` should be cached for high
   * performance, because this is an iterative algorithm.
   */
  @Since("0.8.0")
  def run(data: RDD[Vector]): KMeansModel = {

    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("The users input data is not directly cached, which may hurt performance if its"
        + " parent RDDs are also uncached.")
    }

    val zippedData = data.map { x =>
      val norm = Vectors.norm(x, 2.0)
      new VectorWithNorm(x, norm)
    }

    val centers = initialModel match {
      case Some(kMeansCenters) => {
        kMeansCenters.clusterCenters.map(s => new VectorWithNorm(s))
      }
      case None => {
        if (initializationMode == KMeans.RANDOM) {
          initRandom(zippedData)
        } else {
          initKMeansParallel(zippedData)
        }
      }
    }

    val samplePoint = data.first()
    val dims = samplePoint.size
    if (samplePoint.isInstanceOf[SparseVector]) {
      logWarning("KMeans will be inefficient if the input data is Sparse Vector.")
    }

    val blockData = zippedData.mapPartitions { iter =>
      iter.grouped(blockSize).map { points =>
        val realSize = points.size
        val pointsArray = new Array[Double](realSize * dims)
        val pointsNormArray = new Array[Double](realSize)
        var numRows = 0

        points.foreach { point =>
          System.arraycopy(point.vector.toArray, 0, pointsArray, numRows * dims, dims)
          pointsNormArray(numRows) = point.norm * point.norm
          numRows += 1
        }
        val pointMatrix = new DenseMatrix(numRows, dims, pointsArray, true)

        (pointMatrix, pointsNormArray)
      }
    }

    blockData.persist()
    blockData.count()
    val model = runAlgorithm(blockData, centers)
    blockData.unpersist()

    // Warn at the end of the run as well, for increased visibility.
    if (blockData.getStorageLevel == StorageLevel.NONE) {
      logWarning("The users input data was not directly cached, which may hurt performance if its"
        + " parent RDDs are also uncached.")
    }
    model
  }

  /**
   * Implementation of K-Means algorithm.
   */
  private def runAlgorithm(
      data: RDD[(DenseMatrix, Array[Double])],
      centers: Array[VectorWithNorm]): KMeansModel = {

    val sc = data.sparkContext
    val dims = centers(0).vector.size

    var done = false
    var costs = 0.0
    var iteration = 0
    val iterationStartTime = System.nanoTime()

    // Execute Lloyd's algorithm until converged or reached the max number of iterations
    while (iteration < maxIterations && !done ) {
      type WeightedPoint = (Vector, Long)
      def mergeContribs(x: WeightedPoint, y: WeightedPoint): WeightedPoint = {
        axpy(1.0, x._1, y._1)
        (y._1, x._2 + y._2)
      }

      val costAccums = sc.accumulator(0.0)

      // Construct centers array and broadcast it
      val centersArray = new Array[Double](k * dims)
      val centersNormArray = new Array[Double](k)
      var i = 0
      centers.foreach { center =>
        System.arraycopy(center.vector.toArray, 0, centersArray, i * dims, dims)
        centersNormArray(i) = math.pow(center.norm, 2.0)
        i += 1
      }
      val bcCentersArray = sc.broadcast(centersArray)
      val bcCentersNormArray = sc.broadcast(centersNormArray)

      // Find the sum and count of points mapping to each center
      val totalContribs = data.flatMap { case (pointMatrix, pointsNormArray) =>
        val thisCentersArray = bcCentersArray.value
        val thisCentersNormArray = bcCentersNormArray.value

        val k = thisCentersNormArray.length
        val numRows = pointMatrix.numRows

        val sums = Array.fill(k)(Vectors.zeros(dims))
        val counts = Array.fill(k)(0L)

        // Construct centers matrix
        val centerMatrix = new DenseMatrix(dims, k, thisCentersArray)

        // Construct point^2 + center^2 matrix
        val a2b2 = new Array[Double](numRows * k)
        var i = 0
        while (i < k) {
          var j = 0
          while (j < numRows) {
            a2b2(i * numRows + j) = pointsNormArray(j) + thisCentersNormArray(i)
            j += 1
          }
          i += 1
        }

        // Compute distance matrix
        val distanceMatrix = new DenseMatrix(numRows, k, a2b2)
        gemm(-2.0, pointMatrix, centerMatrix, 1.0, distanceMatrix)

        i = 0
        while (i < numRows) {
          var minCost = Double.PositiveInfinity
          var min = -1
          var j = 0
          while (j < k) {
            val distance = distanceMatrix.values(i + j * numRows)
            if (distance < minCost) {
              minCost = distance
              min = j
            }
            j += 1
          }
          costAccums += minCost
          val sum = sums(min)
          val value = pointMatrix.values.slice(i * dims, (i + 1) * dims)
          axpy(1.0, Vectors.dense(value), sum)
          counts(min) += 1
          i += 1
        }

        val contribs = for (j <- 0 until k) yield {
          (j, (sums(j), counts(j)))
        }
        contribs.iterator
      }.reduceByKey(mergeContribs).collectAsMap()


      bcCentersArray.unpersist(blocking = false)
      bcCentersNormArray.unpersist(blocking = false)

      var changed = false
      var j = 0
      while (j < k) {
        val (sum, count) = totalContribs(j)
        if (count != 0) {
          scal(1.0 / count, sum)
          val newCenter = new VectorWithNorm(sum)
          if (KMeans.fastSquaredDistance(newCenter, centers(j)) > epsilon * epsilon) {
            changed = true
          }
          centers(j) = newCenter
        }
        j += 1
      }

      costs = costAccums.value
      if (!changed) {
        logInfo("Run finished in " + (iteration + 1) + " iterations")
        done = true
      }
      iteration += 1
    }

    val iterationTimeInSeconds = (System.nanoTime() - iterationStartTime) / 1e9
    logInfo(s"Iterations took " + "%.3f".format(iterationTimeInSeconds) + " seconds.")
    if (iteration == maxIterations) {
      logInfo(s"KMeans reached the max number of iterations: $maxIterations.")
    } else {
      logInfo(s"KMeans converged in $iteration iterations.")
    }
    logInfo(s"The cost is $costs.")

    new KMeansModel(centers.map(_.vector))
  }

  /**
   * Initialize cluster centers at random.
   */
  private def initRandom(data: RDD[VectorWithNorm]): Array[VectorWithNorm] = {
    // Sample cluster centers in one pass
    data.takeSample(true, k, new XORShiftRandom(this.seed).nextInt()).toSeq.toArray
  }

  /**
   * Initialize cluster centers using the k-means|| algorithm by Bahmani et al.
   * (Bahmani et al., Scalable K-Means++, VLDB 2012). This is a variant of k-means++ that tries
   * to find with dissimilar cluster centers by starting with a random center and then doing
   * passes where more centers are chosen with probability proportional to their squared distance
   * to the current cluster set. It results in a provable approximation to an optimal clustering.
   *
   * The original paper can be found at http://theory.stanford.edu/~sergei/papers/vldb12-kmpar.pdf.
   */
  private def initKMeansParallel(data: RDD[VectorWithNorm]): Array[VectorWithNorm] = {
    // Initialize empty centers and point costs.
    val centers = ArrayBuffer.empty[VectorWithNorm]
    var costs = data.map(_ => Double.PositiveInfinity)

    // Initialize first center to a random point.
    val seed = new XORShiftRandom(this.seed).nextInt()
    val sample = data.takeSample(true, 1, seed)(0)
    val newCenters = ArrayBuffer(sample.toDense)

    /** Merges new centers to centers. */
    def mergeNewCenters(): Unit = {
      centers ++= newCenters
      newCenters.clear()
    }

    // On each step, sample 2 * k points on average with probability proportional
    // to their squared distance from the centers. Note that only distances between points
    // and new centers are computed in each iteration.
    var step = 0
    while (step < initializationSteps) {
      val bcNewCenters = data.context.broadcast(newCenters)
      val preCosts = costs
      costs = data.zip(preCosts).map { case (point, cost) =>
        math.min(KMeans.pointCost(bcNewCenters.value, point), cost)
      }.persist(StorageLevel.MEMORY_AND_DISK)
      bcNewCenters.unpersist(blocking = false)
      val sumCosts = costs.aggregate(0.0)(_ + _, _ + _)

      preCosts.unpersist(blocking = false)
      val chosen = data.zip(costs).mapPartitionsWithIndex { (index, pointsWithCosts) =>
        val rand = new XORShiftRandom(seed ^ (step << 16) ^ index)
        pointsWithCosts.flatMap { case (p, c) =>
          val rs = rand.nextDouble() < 2.0 * c * k / sumCosts
          if (rs) Some(p) else None
        }
      }.collect()
      mergeNewCenters()
      chosen.foreach { case p => newCenters += p.toDense }
      step += 1
    }

    mergeNewCenters()
    costs.unpersist(blocking = false)

    // Finally, we might have a set of more than k candidate centers; weight each
    // candidate by the number of points in the dataset mapping to it and run a local k-means++
    // on the weighted centers to pick just k of them
    val bcCenters = data.context.broadcast(centers)
    val weightMap = data.map { p =>
      (KMeans.findClosest(bcCenters.value, p)._1, 1.0)
    }.reduceByKey(_ + _).collectAsMap()
    bcCenters.unpersist(blocking = false)

    val myCenters = centers.toArray
    val myWeights = myCenters.indices.map(i => weightMap.getOrElse(i, 0.0)).toArray
    val finalCenters = LocalKMeans.kMeansPlusPlus(0, myCenters, myWeights, k, 30)

    finalCenters
  }
}


/**
 * Top-level methods for calling K-means clustering.
 */
@Since("0.8.0")
object KMeans {

  // Initialization mode names
  @Since("0.8.0")
  val RANDOM = "random"
  @Since("0.8.0")
  val K_MEANS_PARALLEL = "k-means||"

  /**
   * Trains a k-means model using the given set of parameters.
   *
   * @param data Training points as an `RDD` of `Vector` types.
   * @param k Number of clusters to create.
   * @param maxIterations Maximum number of iterations allowed.
   * @param runs Number of runs to execute in parallel. The best model according to the cost
   *             function will be returned. (default: 1)
   * @param initializationMode The initialization algorithm. This can either be "random" or
   *                           "k-means||". (default: "k-means||")
   * @param seed Random seed for cluster initialization. Default is to generate seed based
   *             on system time.
   */
  @Since("1.3.0")
  def train(
      data: RDD[Vector],
      k: Int,
      maxIterations: Int,
      runs: Int,
      initializationMode: String,
      seed: Long): KMeansModel = {
    new KMeans().setK(k)
      .setMaxIterations(maxIterations)
      .internalSetRuns(runs)
      .setInitializationMode(initializationMode)
      .setSeed(seed)
      .run(data)
  }

  /**
   * Trains a k-means model using the given set of parameters.
   *
   * @param data Training points as an `RDD` of `Vector` types.
   * @param k Number of clusters to create.
   * @param maxIterations Maximum number of iterations allowed.
   * @param runs Number of runs to execute in parallel. The best model according to the cost
   *             function will be returned. (default: 1)
   * @param initializationMode The initialization algorithm. This can either be "random" or
   *                           "k-means||". (default: "k-means||")
   */
  @Since("0.8.0")
  def train(
      data: RDD[Vector],
      k: Int,
      maxIterations: Int,
      runs: Int,
      initializationMode: String): KMeansModel = {
    new KMeans().setK(k)
      .setMaxIterations(maxIterations)
      .internalSetRuns(runs)
      .setInitializationMode(initializationMode)
      .run(data)
  }

  /**
   * Trains a k-means model using specified parameters and the default values for unspecified.
   */
  @Since("0.8.0")
  def train(
      data: RDD[Vector],
      k: Int,
      maxIterations: Int): KMeansModel = {
    train(data, k, maxIterations, 1, K_MEANS_PARALLEL)
  }

  /**
   * Trains a k-means model using specified parameters and the default values for unspecified.
   */
  @Since("0.8.0")
  def train(
      data: RDD[Vector],
      k: Int,
      maxIterations: Int,
      runs: Int): KMeansModel = {
    train(data, k, maxIterations, runs, K_MEANS_PARALLEL)
  }

  /**
   * Returns the index of the closest center to the given point, as well as the squared distance.
   */
  private[mllib] def findClosest(
      centers: TraversableOnce[VectorWithNorm],
      point: VectorWithNorm): (Int, Double) = {
    var bestDistance = Double.PositiveInfinity
    var bestIndex = 0
    var i = 0
    centers.foreach { center =>
      // Since `\|a - b\| \geq |\|a\| - \|b\||`, we can use this lower bound to avoid unnecessary
      // distance computation.
      var lowerBoundOfSqDist = center.norm - point.norm
      lowerBoundOfSqDist = lowerBoundOfSqDist * lowerBoundOfSqDist
      if (lowerBoundOfSqDist < bestDistance) {
        val distance: Double = fastSquaredDistance(center, point)
        if (distance < bestDistance) {
          bestDistance = distance
          bestIndex = i
        }
      }
      i += 1
    }
    (bestIndex, bestDistance)
  }

  /**
   * Returns the K-means cost of a given point against the given cluster centers.
   */
  private[mllib] def pointCost(
      centers: TraversableOnce[VectorWithNorm],
      point: VectorWithNorm): Double =
    findClosest(centers, point)._2

  /**
   * Returns the squared Euclidean distance between two vectors computed by
   * [[org.apache.spark.mllib.util.MLUtils#fastSquaredDistance]].
   */
  private[clustering] def fastSquaredDistance(
      v1: VectorWithNorm,
      v2: VectorWithNorm): Double = {
    MLUtils.fastSquaredDistance(v1.vector, v1.norm, v2.vector, v2.norm)
  }

  private[spark] def validateInitMode(initMode: String): Boolean = {
    initMode match {
      case KMeans.RANDOM => true
      case KMeans.K_MEANS_PARALLEL => true
      case _ => false
    }
  }
}

/**
 * A vector with its norm for fast distance computation.
 *
 * @see [[org.apache.spark.mllib.clustering.KMeans#fastSquaredDistance]]
 */
private[clustering]
class VectorWithNorm(val vector: Vector, val norm: Double) extends Serializable {

  def this(vector: Vector) = this(vector, Vectors.norm(vector, 2.0))

  def this(array: Array[Double]) = this(Vectors.dense(array))

  /** Converts the vector to a dense vector. */
  def toDense: VectorWithNorm = new VectorWithNorm(Vectors.dense(vector.toArray), norm)
}
