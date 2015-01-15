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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream

/**
 * :: DeveloperApi ::
 * StreamingABTest is the abstract class to represent online performance of a statistical test
 * for streaming data. It can be implemented to analyze an A/B test where
 * observations are supplied from a DStream (@see [[org.apache.spark.streaming.dstream.DStream]])
 *
 * An instance of a StreamingABTest subclass will continually update its summary statistics
 * for calculation of the test.
 *
 * If a windowLength is specified, tests are performed on a sliding window of n seconds. n must
 * be a positive integer. If set to 0, no windowing is performed.
 *
 * If peacePeriod is provided, the test will ignore
 * the n initial batches. If the window length is non-zero, then this peace period
 * is ignored. Must be a positive integer. If set to 0, no peace period is used.
 *
 * @tparam SummaryType Class that stores summary statistics for the statistical test
 * @tparam DF Type of degrees of freedom (probably Double or Int)
 *            TODO: figure out how to get rid of this
 * @tparam TestResultType Class that stores test results for the statistical test
 */
@DeveloperApi
trait StreamingABTest[SummaryType <: SummaryStatistics[SummaryType], DF,
TestResultType <: TestResult[DF]] {

  /**
   * Stores the duration of the initial peace period in number of batches. The first
   * peacePeriod batches of data will be ignored. If windowLength is non-zero, this is
   * ignored.
   */
  protected val peacePeriod: Long

  /**
   * Stores the duration of the window for windowed streaming computation. Only the most
   * recent data will be used in calculating the test.
   * @see [[org.apache.spark.streaming.dstream.DStream.window]]
   */
  protected val windowLength: Duration

  /*
   * Mutable map from population indicator to summary statistics
   * Enables the test to maintain state between successive RDDs provided
   * by the DStream object
   */
  protected val testStats: collection.mutable.Map[Boolean, SummaryType]

  /**
   * Perform the statistical test comparing the two populations
   * @param pop1 summary of results from population 1
   * @param pop2 summary of results from population 2
   */
  protected def performTest(pop1: SummaryType, pop2: SummaryType): TestResultType

  /** Store the number of batches seen so far */
  protected var numBatches: Long = 0

  /** Extract summary statistics from an RDD of (Boolean, Double) pairs */
  protected def getStatsFromRDD(rdd: RDD[(Boolean, Double)]): Map[Boolean, SummaryType]

  /** Incorporate data from a new RDD into the summary statistics */
  protected def updateStatsFromRDD(rdd: RDD[(Boolean,Double)]): RDD[(Boolean, SummaryType)] = {

    // Increment the batch counter
    numBatches = numBatches + 1

    // Only update stats if we have passed the peace period
    if (numBatches > peacePeriod) {
      // Get summary statistics for the new RDD only
      val newStats = getStatsFromRDD(rdd)

      // Incorporate the new summary statistics into the previous statistics
      List(true, false).foreach(grp => {
        testStats(grp) = testStats(grp).combine(newStats(grp))
      })
    }

    // Return an RDD containing updated summary statistics
    rdd.sparkContext.parallelize(List((true,testStats(true)), (false,testStats(false))))
  }

  /**
   * getCurrentTest performs the test on the summary
   * statistics currently stored in the StreamingABTest object.
   * Should be called only after calling the object's getTestResults
   * on a DStream, and starting the stream.
   *
   * Used for unit tests in @[[org.apache.spark.mllib.stat]]
   *
   * @return TestResultType object containing results of t-test
   */
  protected[stat] def getCurrentTest(): TestResultType = {
    performTest(testStats(true), testStats(false))
  }

  /**
   * getSummaryStats takes a DStream of (Boolean, Double) pairs. Each pair represents
   * one observation from the experiment, where the Boolean represents the population
   * (treatment or control, probably), and the Double is the outcome.
   *
   * getSummaryStats returns a DStream of (Boolean, SummaryType), one for each batch. These
   * TestResultTypes contain the summary statistics from the data
   * in the DStream
   *
   * @param data DStream of (Boolean, Double) pairs
   * @return DStream[(Boolean, SummaryType)] containing summary statistics for all
   *         data provided by the stream so far
   */
  protected def getSummaryStats(data: DStream[(Boolean, Double)]):
  DStream[(Boolean, SummaryType)] = {
    if (windowLength.isZero) {
      data.transform(updateStatsFromRDD(_))
    } else {
      data.window(windowLength).transform(rdd => {
        val curStats = getStatsFromRDD(rdd)
        rdd.sparkContext.parallelize(List((true,curStats(true)), (false,curStats(false))))
      })
    }
  }

  /**
   * getTestResults takes a DStream of (Boolean, Double) pairs. Each pair represents
   * one observation from the experiment, where the Boolean represents the population
   * (treatment or control, probably), and the Double is the outcome.
   *
   * getTestResults returns a DStream of TestResultType, one for each batch. These
   * TestResults contain the results of performing the statistical test.
   * @param data DStream of (Boolean, Double) pairs
   * @return DStream[TestResultType] containing test results
   */
  def getTestResults(data: DStream[(Boolean, Double)]): DStream[TestResultType]
}
