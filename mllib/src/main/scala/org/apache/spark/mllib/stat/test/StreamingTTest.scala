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

package org.apache.spark.mllib.stat.test

import org.apache.commons.math3.distribution.TDistribution
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream
import scala.math._

/**
 * :: DeveloperApi ::
 * StreamingTTest implements online performance of a t-test for streaming data.
 * It can be used to analyze an A/B test where observations are supplied
 * from a DStream (@see [[org.apache.spark.streaming.dstream.DStream]])
 *
 * StreamingTTest implements a two-sample t-test with unpaired observations, where
 * the population variances are not assumed to be equal (the so-called Welch's t-test)
 *
 * An instance of StreamingTTest will continually update its summary statistics
 * for calculation of the t-test, including population means, variances, and counts.
 *
 * If a windowLength is specified, t-tests are performed on a sliding window of n seconds. n must
 * be a positive integer. If set to 0, no windowing is performed.
 *
 * If peacePeriod is provided, the t-test will ignore
 * the n initial batches. If the window length is non-zero, then this peace period
 * is ignored. Must be a positive integer. If set to 0, no peace period is used.
 *
 * @constructor Create a new StreamingTTest object.
 * @param peacePeriod The number of batches that will be ignored at the beginning of the test
 *                    If 0, all batches will be used
 * @param windowLength The duration of the window for performing windowed t-test.  Only data from
 *                     most recent batches will be used. If 0, no windowing is performed, and
 *                     all batches contribute to the test results.
 */
@DeveloperApi
class StreamingTTest(protected val peacePeriod: Long = 0,
                     protected val windowLength: Duration = Seconds(0))
  extends StreamingABTest[TTestSummaryStats,Double,TTestResult] {

  /** Instantiate the Map storing the population summaries */
  protected val testStats = collection.mutable.Map(true -> TTestSummaryStats(),
    false -> TTestSummaryStats())

  /** Pass the population summaries to the tester */
  protected def performTest(pop1: TTestSummaryStats, pop2: TTestSummaryStats): TTestResult =
    TTester.performTest(pop1, pop2)

  /**
   * Extract summary statistics from an RDD of (Boolean, Double) pairs
   * TODO: Make this abstract. Am having a serialization error when trying to
   * move this to the StreamingABTest trait
   *
   * @param rdd RDD containing the data to be summarized
   */
  protected def getStatsFromRDD(rdd: RDD[(Boolean, Double)]): Map[Boolean, TTestSummaryStats] = {
    rdd.combineByKey[TTestSummaryStats]((x: Double) => TTestSummaryStats(x,0.0,1),
      (s1: TTestSummaryStats, x: Double) => s1.combine(TTestSummaryStats(x,0.0,1)),
      (s1: TTestSummaryStats, s2: TTestSummaryStats) =>
        s1.combine(s2)).collect.toMap.withDefaultValue(TTestSummaryStats())
  }

  /**
   * getTestResults takes a DStream of (Boolean, Double) pairs. Each pair represents
   * one observation from the experiment, where the Boolean represents the population
   * (treatment or control, probably), and the Double is the outcome.
   *
   * getTestResults returns a DStream of TTestResult, one for each batch.
   *
   * TODO: make this abstract (having compile-time error about ClassTag not available
   * when I try to move this to the StreamingABTest trait)
   *
   * @param data DStream of (Boolean, Double) pairs
   * @return DStream[TTestResult] containing test results
   */
  def getTestResults(data: DStream[(Boolean, Double)]): DStream[TTestResult] = {
    getSummaryStats(data).transform((rdd: RDD[(Boolean, TTestSummaryStats)]) => {
      val summaryStats = rdd.collect().toMap
      val result: TTestResult = TTester.performTest(summaryStats(true), summaryStats(false))
      rdd.sparkContext.parallelize(List(result))
    })
  }
}

private[stat] object TTester {

  /**
   * Perform the unpaired t-test on the provided summary statistics
   * Population variances are not assumed equal (aka Welch's t-test)
   *
   * @param pop1 Summary statistics from population 1
   * @param pop2 Summary statistics from population 2
   * @return TTestResult containing test result statistics
   */
  def performTest(pop1: TTestSummaryStats, pop2: TTestSummaryStats): TTestResult = {
    if (pop1.n <= 1 || pop2.n <= 1) {
      new TTestResult(1.0, 0, 0,
        "No test performed, as at least one group does not have sufficient data",
        "No hypothesis tested, as at least one group does not have sufficient data")
    } else {
      // Calculate degrees-of-freedom correction for the two populations
      val sigCorrect1 = pop1.n.toDouble / (pop1.n - 1)
      val sigCorrect2 = pop2.n.toDouble / (pop2.n - 1)

      // Calculate the combined variance of the populations
      val combinedVar = sigCorrect1 * pop1.sigma / pop1.n + sigCorrect2 * pop2.sigma / pop2.n

      // Get the t-statistic for a paired test
      val t = (pop1.mu - pop2.mu) / sqrt(combinedVar)

      // Calculate the degrees of freedom with the Welch–Satterthwaite equation
      val dfNum = pow(combinedVar, 2)
      val dfDenom = pow(sigCorrect1 * pop1.sigma / pop1.n, 2) / (pop1.n - 1) +
        pow(sigCorrect2 * pop2.sigma / pop2.n, 2) / (pop2.n - 1)
      val df = dfNum / dfDenom

      // Calculate the p-value
      val tDist = new TDistribution(df)
      val p = 2 - 2 * tDist.cumulativeProbability(abs(t))
      new TTestResult(p, df, t, "Unpaired t-test with unequal variances",
        "Two populations have the same mean")
    }
  }
}

/**
 * TTestSummaryStats stores summary statistics necessary to calculate
 * Welch's t-test: mean, variance, and size of the population.
 * The variance is stored as (sum of squares) / size; degrees of freedom
 * correction is not performed here, but instead when calculating
 * t-statistic.
 *
 * @param mu mean of population
 * @param sigma variance of population (without degrees of freedom calculation)
 * @param n size of the population
 */
private[test] case class TTestSummaryStats(mu: Double = 0.0,
                                           sigma:Double = 0.0, n:Long = 0)
  extends SummaryStatistics[TTestSummaryStats] {

  /**
   * Combine this set of summary statistics with another.
   * See [[http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm]]
   * for details on the method used here
   *
   * @param other summary statistics from other population
   * @return new set of combined summary statistics
   */
  def combine(other: TTestSummaryStats): TTestSummaryStats = {
    val n_comb = n + other.n
    if (n_comb == 0) {
      // If no data, return empty statistics
      TTestSummaryStats()
    } else {

      val delta = other.mu - mu

      val mu_comb = mu + delta * (other.n.toDouble / n_comb)

      val m_comb = sigma * n + other.sigma * other.n +
        pow(delta,2) * ((n * other.n).toDouble / n_comb)

      val v_comb = m_comb / n_comb

      TTestSummaryStats(mu_comb, v_comb, n_comb)
    }
  }
}
