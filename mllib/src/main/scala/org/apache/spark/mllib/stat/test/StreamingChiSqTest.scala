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

import org.apache.spark.mllib.linalg.Matrices
import spire.implicits._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream
import scala.math._

/**
 * :: DeveloperApi ::
 * StreamingChiSqTest implements online performance of a Chi-square test for streaming data.
 * It can be used to analyze an A/B test where observations are supplied
 * from a DStream (@see [[org.apache.spark.streaming.dstream.DStream]])
 *
 * An instance of StreamingChiSqTest will continually update its summary statistics
 * for calculation of the test; this includes counts of all values seen so far in each population.
 *
 * For the case of the Chi-square test, the DStream should contain (Boolean, Double) pairs, but
 * the Double is interpreted as an indicator of a discrete outcome. The full set of outcomes needs
 * to be present in both populations; otherwise, no test is performed.
 *
 * The Chi-square test is a test for independence between outcome and treatment group. Also
 * known as the Pearson's chi-square test.
 *
 * If a windowLength is specified, t-tests are performed on a sliding window of n seconds. n must
 * be a positive integer. If set to 0, no windowing is performed.
 *
 * If peacePeriod is provided, the t-test will ignore
 * the n initial batches. If the window length is non-zero, then this peace period
 * is ignored. Must be a positive integer. If set to 0, no peace period is used.
 *
 *
 * @constructor Create a new StreamingTTest object.
 * @param peacePeriod The number of batches that will be ignored at the beginning of the test
 *                    If 0, all batches will be used
 * @param windowLength The duration of the window for performing windowed t-test.  Only data from
 *                     most recent batches will be used. If 0, no windowing is performed, and
 *                     all batches contribute to the test results.
 */
@DeveloperApi
class StreamingChiSqTest(protected val peacePeriod: Long = 0,
                     protected val windowLength: Duration = Seconds(0))
  extends StreamingABTest[ChiSqTestSummaryStats,Int,ChiSqTestResult] {

  /** Instantiate the Map storing the population summaries */
  protected val testStats = collection.mutable.Map(true -> ChiSqTestSummaryStats(),
    false -> ChiSqTestSummaryStats())

  /** Pass the population summaries to the tester */
  protected def performTest(pop1: ChiSqTestSummaryStats,
                            pop2: ChiSqTestSummaryStats): ChiSqTestResult =
    ChiSqTester.performTest(pop1, pop2)

  /**
   * Extract summary statistics from an RDD of (Boolean, Double) pairs
   * TODO: Make this abstract. Am having a serialization error when trying to
   * move this to the StreamingABTest trait
   *
   * @param rdd RDD containing the data to be summarized
   */
  protected def getStatsFromRDD(rdd: RDD[(Boolean, Double)]):
  Map[Boolean, ChiSqTestSummaryStats] = {
    rdd.combineByKey[ChiSqTestSummaryStats]((x: Double) =>
      ChiSqTestSummaryStats(Map( x -> 1.toLong ).withDefaultValue(0)),
      (s1: ChiSqTestSummaryStats, x: Double) =>
        s1.combine(ChiSqTestSummaryStats(Map( x -> 1.toLong ).withDefaultValue(0))),
      (s1: ChiSqTestSummaryStats, s2: ChiSqTestSummaryStats) =>
        s1.combine(s2)).collect.toMap.withDefaultValue(ChiSqTestSummaryStats())
  }

  /**
   * getTestResults takes a DStream of (Boolean, Double) pairs. Each pair represents
   * one observation from the experiment, where the Boolean represents the population
   * (treatment or control, probably), and the Double is the outcome. The Double represents a
   * discrete outcome, and the test is only performed if both populations contain observations from
   * the same set of outcomes.
   *
   * getTestResults returns a DStream of ChiSqTestResult, one for each batch.
   *
   * TODO: make this abstract (having compile-time error about ClassTag not available
   * when I try to move this to the StreamingABTest trait)
   *
   * @param data DStream of (Boolean, Double) pairs
   * @return DStream[TTestResult] containing test results
   */
  def getTestResults(data: DStream[(Boolean, Double)]): DStream[ChiSqTestResult] = {
    getSummaryStats(data).transform((rdd: RDD[(Boolean, ChiSqTestSummaryStats)]) => {
      val summaryStats = rdd.collect().toMap
      val result: ChiSqTestResult = ChiSqTester.performTest(summaryStats(true), summaryStats(false))
      rdd.sparkContext.parallelize(List(result))
    })
  }
}

private[stat] object ChiSqTester {

  /**
   * Perform the Chi-square test on the provided summary statistics.
   * Delegate to @[[org.apache.spark.mllib.stat.test.ChiSqTest.chiSquaredMatrix()]]
   *
   * @param pop1 Summary statistics from population 1
   * @param pop2 Summary statistics from population 2
   * @return ChiSqTestResult containing test result statistics
   */
  def performTest(pop1: ChiSqTestSummaryStats, pop2: ChiSqTestSummaryStats): ChiSqTestResult = {

    // Build vectors of counts
    val allKeys = (pop1.counts.keys ++ pop2.counts.keys).toArray
    val pop1Counts = allKeys.map(pop1.counts(_).toDouble)
    val pop2Counts = allKeys.map(pop2.counts(_).toDouble)

    try {
      ChiSqTest.chiSquaredMatrix(Matrices.dense(numRows = pop1Counts.length,
        numCols = 2, values = pop1Counts ++ pop2Counts))
    } catch { case x: IllegalArgumentException => {
      new ChiSqTestResult(1.0, 0, 0, "No test performed due to insufficient data",
        "No hypothesis was tested due to insufficient data")
    }}
  }
}

/**
 * Store the summaries of data necessary to perform Chi-square test. This stores the
 * counts of all outcomes seen so far.
 * @param counts Map from outcome (represented as a Double) to count of observations
 */
private[test] case class ChiSqTestSummaryStats(val counts: Map[Double, Long]
                                               = Map[Double, Long]().withDefaultValue(0))
  extends SummaryStatistics[ChiSqTestSummaryStats] {

  /**
   * Combine this set of summary statistics with another. Outer join of two Maps.
   * @param other summary statistics from other population
   * @return new set of combined summary statistics
   */
  def combine(other: ChiSqTestSummaryStats): ChiSqTestSummaryStats = {
    ChiSqTestSummaryStats(counts + other.counts)
  }
}
