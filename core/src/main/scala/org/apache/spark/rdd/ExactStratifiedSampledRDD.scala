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

package org.apache.spark.rdd

import org.apache.spark.util.Utils

import scala.collection.Map
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.hashing.byteswap64

import cern.jet.random.Poisson
import cern.jet.random.engine.DRand

import org.apache.spark.{Logging, Partition, TaskContext}
import org.apache.spark.SparkContext._
import org.apache.spark.util.random.{BinomialBounds, PoissonBounds, XORShiftRandom}

/**
 * Represents an RDD obtained from exact stratified sampling of the parent RDD.
 *
 * A preselection-waitlisting algorithm is used to guarantee that we can obtain the exact sample
 * size for each stratum with high probability.
 *
 * @param prev parent RDD
 * @param withReplacement whether to sample with replacement or without
 * @param fractions map of keys (strata) to sampling probabilities
 * @param seed random seed
 */
private[rdd] class ExactStratifiedSampledRDD[K: ClassTag, V: ClassTag](
    @transient prev: RDD[(K, V)],
    @transient withReplacement: Boolean,
    @transient fractions: Map[K, Double],
    seed: Long = Utils.random.nextLong()) extends RDD[(K, V)](prev) {

  fractions.foreach { case (stratum, fraction) =>
    if (withReplacement) {
      require(fraction >= 0.0, "Fraction cannot be negative for sampling with replacement but " +
        s"found $fraction for stratum $stratum.")
    } else {
      require(fraction >= 0.0 && fraction <= 1.0, "Fraction must be in [0, 1] for sampling " +
        s"without replacement but found $fraction for stratum $stratum")
    }
  }

  import org.apache.spark.rdd.ExactStratifiedSampledRDD._

  val reviewer: Reviewer[K] = {
    val tasks = prev.countByKey().map { case (stratum, count) =>
      val sampleSize = math.ceil(fractions(stratum) * count).toLong
      Task(stratum, count, sampleSize)
    }
    val rv = Reviewer.create(withReplacement, tasks)
    val results = prev.mapPartitionsWithIndex { case (idx, iter) =>
      val theSeed = byteswap64(seed ^ idx)
      Iterator(rv.preselect(iter, theSeed))
    }.reduce { case (results1, results2) =>
      results1.merge(results2)
    }
    rv.review(results)
    rv
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = {
    val theSeed = byteswap64(seed ^ context.partitionId)
    reviewer.select(firstParent[(K, V)].iterator(split, context), theSeed)
  }

  override protected def getPartitions: Array[Partition] = prev.partitions

  override val partitioner = prev.partitioner
}

private[rdd] object ExactStratifiedSampledRDD {

  /**
   * A stratified sampling task.
   *
   * @param stratum stratum
   * @param count total number of items on this stratum
   * @param sampleSize the desired sample size
   */
  case class Task[K](stratum: K, count: Long, sampleSize: Long)

  /**
   * Trait for reviewers that selects the exact number of candidates for each stratum.
   */
  trait Reviewer[K] extends Serializable with Logging {

    /**
     * Preselects candidates and returns the selection results.
     * @param candidates an iterator of (key, value) pairs, where key indicates the stratum
     * @param seed random seed
     */
    def preselect[V](candidates: Iterator[(K, V)], seed: Long): SelectionResultMap[K]

    /**
     * Reviews the aggregated selection results and decides the thresholds to use for selection.
     */
    def review(results: SelectionResultMap[K]): Unit

    /**
     * Makes the final selection using the same procedure as preselection.
     *
     * @param candidates an iterator of (key, value) pairs, where key indicates the stratum
     * @param seed random seed
     */
    def select[V](candidates: Iterator[(K, V)], seed: Long): Iterator[(K, V)]
  }

  object Reviewer {

    /**
     * Creates a reviewer.
     *
     * @param withReplacement whether to sample with replacement or without
     * @param tasks sampling tasks
     */
    def create[K](withReplacement: Boolean, tasks: Iterable[Task[K]]): Reviewer[K] = {
      if (withReplacement) {
        new PoissonReviewer(tasks)
      } else {
        new BernoulliReviewer(tasks)
      }
    }
  }

  /**
   * A reviewer that selects the exact number of candidates without replacement for each stratum,
   * with high probability.
   *
   * @param tasks sampling tasks
   */
  class BernoulliReviewer[K](@transient tasks: Iterable[Task[K]]) extends Reviewer[K] {

    import org.apache.spark.rdd.ExactStratifiedSampledRDD.BernoulliReviewer.Policy

    var preselectionPolicies: Map[K, Policy] =
      tasks.map { case Task(stratum, count, sampleSize) =>
        val lb = BinomialBounds.lower(count, sampleSize)
        val ub = BinomialBounds.upper(count, sampleSize)
        (stratum, Policy(lb, ub))
      }.toMap

    var selectionThresholds: Map[K, Double] = _

    override def preselect[V](candidates: Iterator[(K, V)], seed: Long): SelectionResultMap[K] = {
      val results = new SelectionResultMap[K]
      val random = new XORShiftRandom(seed)
      candidates.foreach { case (stratum, _) =>
        val score = random.nextDouble()
        val policy = preselectionPolicies(stratum)
        if (score < policy.preselect) {
          results(stratum).numSelected += 1
        } else if (score < policy.waitlist) {
          results(stratum).waitingList += score
        }
      }
      results
    }

    override def review(results: SelectionResultMap[K]) {
      selectionThresholds =
        tasks.map { case Task(stratum, _, sampleSize) =>
          val result = results(stratum)
          val policy = preselectionPolicies(stratum)
          val numCandidates = result.numSelected + result.waitingList.size
          val selectionThreshold = if (result.numSelected > sampleSize) {
            logWarning(s"Selected ${result.numSelected} items from stratum $stratum, " +
              s"while the ideal sample size is $sampleSize.")
            policy.preselect
          } else if (numCandidates <= sampleSize) {
            if (numCandidates < sampleSize) {
              logWarning(s"Selected $numCandidates items from stratum $stratum, while the ideal " +
                s"sample size is $sampleSize.")
            }
            policy.waitlist
          } else {
            val orderedWaitingList = result.waitingList.sorted
            orderedWaitingList((sampleSize - result.numSelected).toInt)
          }
          (stratum, selectionThreshold)
        }.toMap
      preselectionPolicies = null
    }

    override def select[V](candidates: Iterator[(K, V)], seed: Long): Iterator[(K, V)] = {
      val random = new XORShiftRandom(seed)
      candidates.filter { case (stratum, _) =>
        random.nextDouble() < selectionThresholds(stratum)
      }
    }
  }

  object BernoulliReviewer {

    /**
     * Policy for [[org.apache.spark.rdd.ExactStratifiedSampledRDD.BernoulliReviewer]].
     *
     * @param preselect threshold for preselection
     * @param waitlist threshold for waitlisting
     */
    case class Policy(preselect: Double, waitlist: Double)
  }

  /**
   * A reviewer that selects the exact number of candidates with replacement for each stratum, with
   * high probability.
   *
   * @param tasks sampling tasks
   */
  class PoissonReviewer[K](@transient tasks: Iterable[Task[K]]) extends Reviewer[K] {

    import org.apache.spark.rdd.ExactStratifiedSampledRDD.PoissonReviewer.Policy

    private val policies = tasks.map { case Task(stratum, count, sampleSize) =>
      val lb = PoissonBounds.lower(sampleSize) / count
      val ub = math.max(PoissonBounds.upper(sampleSize) / count, 1e-10)
      (stratum, Policy(lb, ub - lb))
    }.toMap

    override def preselect[V](candidates: Iterator[(K, V)], seed: Long): SelectionResultMap[K] = {
      val results = new SelectionResultMap[K]
      val poisson = new Poisson(1.0, new DRand(seed.toInt))
      val uniform = new XORShiftRandom(byteswap64(seed))
      candidates.foreach { case (stratum, _) =>
        val policy = policies(stratum)
        results.select(stratum, poisson.nextInt(policy.preselect))
        val w = poisson.nextInt(policy.waitlist)
        var i = 0
        while (i < w) {
          results.waitlist(stratum, uniform.nextDouble())
          i += 1
        }
      }
      results
    }

    override def review(results: SelectionResultMap[K]) {
      tasks.foreach { case Task(stratum, _, sampleSize) =>
        val result = results(stratum)
        val numCandidates = result.numSelected + result.waitingList.size
        val threshold = if (result.numSelected > sampleSize) {
          logWarning(s"Selected ${result.numSelected} items from stratum $stratum, " +
            s"while the ideal sample size is $sampleSize.")
          0.0
        } else if (numCandidates <= sampleSize) {
          if (numCandidates < sampleSize) {
            logWarning(s"Selected $numCandidates items from stratum $stratum, while the ideal " +
              s"sample size is $sampleSize.")
          }
          1.0
        } else {
          val orderedWaitingList = result.waitingList.sorted
          orderedWaitingList((sampleSize - result.numSelected).toInt)
        }
        policies(stratum).threshold = threshold
      }
    }

    override def select[V](candidates: Iterator[(K, V)], seed: Long): Iterator[(K, V)] = {
      val poisson = new Poisson(1.0, new DRand(seed.toInt))
      val uniform = new XORShiftRandom(byteswap64(seed))
      candidates.flatMap { case (stratum, item) =>
        val policy = policies(stratum)
        var numSelected = poisson.nextInt(policy.preselect)
        val w = poisson.nextInt(policy.waitlist)
        var i = 0
        while (i < w) {
          if (uniform.nextDouble() < policy.threshold) {
            numSelected += 1
          }
          i += 1
        }
        Iterator.fill(numSelected)((stratum, item))
      }
    }
  }

  object PoissonReviewer {
    
    /**
     * Policy for [[org.apache.spark.rdd.ExactStratifiedSampledRDD.PoissonReviewer]].
     *
     * @param preselect Poisson mean for preselection
     * @param waitlist Poisson mean for waitlisting
     * @param threshold threshold for the waiting list
     */
    case class Policy(preselect: Double, waitlist: Double, var threshold: Double = Double.NaN)
  }

  /**
   * Preselection results for all strata.
   */
  class SelectionResultMap[K] extends Serializable {

    private val results = mutable.Map.empty[K, SelectionResult]

    /** Returns the selection result for the input stratum. */
    def apply(stratum: K): SelectionResult = {
      results.getOrElseUpdate(stratum, new SelectionResult)
    }

    /** Select a candidate from the input stratum for k times. */
    def select(stratum: K, times: Long) {
      this(stratum).numSelected += times
    }

    /** Waitlist a candidate with the given score from the input stratum. */
    def waitlist(stratum: K, score: Double) {
      this(stratum).waitingList += score
    }

    /** Merges other selection results. */
    def merge(other: SelectionResultMap[K]): this.type = {
      other.results.foreach { case (stratum, result) =>
        this(stratum).merge(result)
      }
      this
    }
  }

  /**
   * Preselection result for a single stratum.
   */
  class SelectionResult extends Serializable {

    /** number selected candidates */
    var numSelected = 0L
    /** scores of candidates on the waiting list */
    val waitingList = ArrayBuffer.empty[Double]

    /** Merges another selection result. */
    def merge(other: SelectionResult): this.type = {
      waitingList ++= other.waitingList
      numSelected += other.numSelected
      this
    }
  }
}
