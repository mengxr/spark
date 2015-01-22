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

import scala.reflect.ClassTag
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, Duration}


/**
 * :: DeveloperApi ::
 * StreamingABTest implements online AB testing for streaming data.  The data must be provided
 * to getTestResults as a DStream[(Boolean, Double)], where the Boolean indicates the treatment
 * group and the Double is the outcome.
 *
 * Upon construction, StreamingABTest must be provided an object or class instance to calculate
 * summary statistics from the data and a method to perform the statistical test. The summary
 * statistics will be stored in memory as class members, so should be of constant size.
 *
 * By default, StreamingABTest will continually update the summary statistics such that they
 * incorporate all data seen so far. This can be adjusted such that there is an initial
 * peace period where no data is considered, or can be changed to reflect a sliding window
 * on the data instead of the full dataset.
 *
 * @tparam SummaryType Type that stores summary statistics for the statistical test. Must implement
 *                     [[org.apache.spark.mllib.stat.test.SummaryStatistics]]. Should be of
 *                     constant size.
 * @tparam TestResultType Type that stores test results. Must implement the trait
 *                        [[org.apache.spark.mllib.stat.test.TestResult]]
 * @param summaryFactory Instance of SummaryType. This will provide methods to instantiate
 *                       empty summaries, and summaries from single observations.
 * @param tester Method to perform the statistical test given two SummaryType instances, one from
 *               each population.
 * @param peacePeriod  If specified, the test will ignore the peacePeriod initial batches.
 *                     If the window length is non-zero, then peacePeriod is ignored.
 *                     Must be a positive integer. If set to 0, no peace period is used.
 * @param windowLength If specified, tests are performed on a sliding window of
 *                     windowLength seconds. Must be a positive integer. If set to 0, no windowing
 *                     is performed.
 *                     @see [[org.apache.spark.streaming.dstream.DStream.window]]
 */
@DeveloperApi
class StreamingABTest[SummaryType <: SummaryStatistics[SummaryType],
TestResultType <: TestResult[_] : ClassTag]
(val summaryFactory: SummaryType, tester: (SummaryType, SummaryType) => TestResultType,
 protected val peacePeriod: Long = 0,
 protected val windowLength: Duration = Seconds(0)) extends Serializable {


  /*
   * Mutable map from population indicator to summary statistics
   * Enables the test to maintain state between successive RDDs provided
   * by the DStream object
   */
  protected val testStats = collection.mutable.Map(true -> summaryFactory.emptySummary(),
    false -> summaryFactory.emptySummary())

  /** Store the number of batches seen so far */
  protected var numBatches: Long = 0

  /** Extract summary statistics from an RDD of (Boolean, Double) pairs */
  protected def summarizeRDD(rdd: RDD[(Boolean, Double)]): RDD[Map[Boolean, SummaryType]] = {
    rdd.combineByKey[SummaryType]((x: Double) => summaryFactory.initializer(x),
      (s1: SummaryType, x: Double) => s1.combine(summaryFactory.initializer(x)),
      (s1: SummaryType, s2: SummaryType) =>
        s1.combine(s2)).map((1,_)).groupByKey.map(
          _._2.toMap.withDefaultValue(summaryFactory.emptySummary()))
  }

  /** Incorporate data from new summary statistics into the current summary statistics */
  protected def updateAndGetSummaryStats(newStats: Map[Boolean, SummaryType]):
  Map[Boolean, SummaryType] = {
    // Incorporate the new summary statistics
    List(true, false).foreach(grp => {
      testStats(grp) = testStats(grp).combine(newStats(grp))
    })

    // Return an RDD containing updated summary statistics
    Map(true -> testStats(true), false -> testStats(false))
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
    tester(testStats(true), testStats(false))
  }

  /**
   * getSummaryStats takes a DStream of (Boolean, Double) pairs. Each pair represents
   * one observation from the experiment, where the Boolean represents the population
   * (treatment or control, probably), and the Double is the outcome.
   *
   * getSummaryStats returns a DStream of (Boolean, SummaryType), one for each batch.
   * getSummaryStats will perform the windowing and will ignore the peace period if these
   * parameters are non-zero.
   *
   * @param data DStream of (Boolean, Double) pairs
   * @return DStream[(Boolean, SummaryType)] containing summary statistics for desired subset
   *         of the data provided by the stream so far, depending on window and peace period
   *         parameters.
   */
  protected def getSummaryStats(data: DStream[(Boolean, Double)]):
  DStream[Map[Boolean, SummaryType]] = {

    if (windowLength.isZero) {
      // Increment the batch counter
      numBatches = numBatches + 1

      // Only update stats if we have passed the peace period
      if (numBatches > peacePeriod) {
        data.transform(rdd => {
          val updatedSummaries = summarizeRDD(rdd).collect.map(updateAndGetSummaryStats(_))
          rdd.sparkContext.parallelize(updatedSummaries)
          /** summarizeRDD(rdd).map(updateAndGetSummaryStats(_)) // Note: this does not work */
        })
      } else {
        data.transform(rdd => {
          rdd.sparkContext.parallelize(
            List(Map(true->summaryFactory.emptySummary(),false->summaryFactory.emptySummary())))
        })
      }
    } else {
      data.window(windowLength).transform(summarizeRDD(_))
    }
  }

  /**
   * getTestResults takes a DStream of (Boolean, Double) pairs. Each pair represents
   * one observation from the experiment, where the Boolean represents the population
   * (treatment or control, probably), and the Double is the outcome.
   *
   * getTestResults returns a DStream of TestResultType, one for each batch. If no windowing
   * or peace period is specified, these test results will incorporate all data seen so far.
   *
   * @param data DStream of (Boolean, Double) pairs
   * @return DStream[TestResultType] containing test results
   */
  def getTestResults(data: DStream[(Boolean, Double)]): DStream[TestResultType] = {
    getSummaryStats(data).transform(_.map(
      summaries => tester(summaries(true), summaries(false))))
  }
}
