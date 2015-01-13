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

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.streaming.dstream.DStream

/**
 * :: DeveloperApi ::
 * Performs online significance testing for a stream of A/B testing results.
 *
 * To address novelty affects, the peacePeriod specifies a set number of initial
 * [[org.apache.spark.rdd.RDD]] batches of the [[DStream]] to be dropped from significance testing.
 *
 * The windowSize sets the number of batches each significance test is to be performed over. The
 * window is sliding with a stride length of 1 batch. Setting windowSize to 0 will perform
 * cumulative processing, using all batches seen so far.
 *
 * Different tests may be used for assessing statistical significance depending on assumptions
 * satisfied by data. For more details, see [[OnlineABTestMethod]]. The testMethod specifies which
 * test will be used.
 *
 * Use a builder pattern to construct an online A/B test in an application, like:
 *
 * val model = new OnlineABTest()
 *   .setPeacePeriod(10)
 *   .setWindowSize(0)
 *   .setTestMethod("welch")
 *   .registerStream(DStream)
 */
@DeveloperApi
class OnlineABTest(
    var peacePeriod: Int = 0,
    var windowSize: Int = 0,
    var testMethod: OnlineABTestMethod = WelchTTest) extends Logging with Serializable {

  /** Set the number of initial batches to ignore. */
  def setPeacePeriod(peacePeriod: Int): this.type = {
    this.peacePeriod = peacePeriod
    this
  }

  /**
   * Set the number of batches to compute significance tests over.
   * A value of 0 will use all batches seen so far.
   */
  def setWindowSize(windowSize: Int): this.type = {
    this.windowSize = windowSize
    this
  }

  /** Set the statistical method used for significance testing. */
  def setTestMethod(method: String): this.type = {
    this.testMethod = OnlineABTestMethodNames.getTestMethodFromName(method)
    this
  }

  /**
   * Register a [[DStream]] of values for significance testing.
   *
   * @param data stream of (key,value) pairs where the key is the group membership (control or
   *             treatment) and the value is the numerical metric to test for significance
   * @return stream of significance testing results
   */
  def registerStream(data: DStream[(Boolean, Double)]): DStream[OnlineABTestResult] = {
    val dataAfterPeacePeriod = dropPeacePeriod(data)
    val summarizedData = summarizeByKeyAndWindow(dataAfterPeacePeriod)
    val pairedSummaries = pairSummaries(summarizedData)
    val testResults = testMethod.doTest(pairedSummaries)

    testResults
  }

  /** Drop all batches inside the peace period. */
  private[stat] def dropPeacePeriod(
      data: DStream[(Boolean, Double)]): DStream[(Boolean, Double)] = {
    data.transform { (rdd, time) =>
      if (time.milliseconds > data.slideDuration.milliseconds * peacePeriod) {
        rdd
      } else {
        rdd.filter(_ => false) // TODO: Is there a better way to drop a RDD from a DStream?
      }
    }
  }

  /** Compute summary statistics over each key and the specified test window size. */
  private[stat] def summarizeByKeyAndWindow(
      data: DStream[(Boolean, Double)]): DStream[(Boolean, MultivariateOnlineSummarizer)] = {
    if (this.windowSize == 0) {
      data.updateStateByKey[MultivariateOnlineSummarizer](
        (newValues: Seq[Double], oldSummary: Option[MultivariateOnlineSummarizer]) => {
          val newSummary = oldSummary.getOrElse(new MultivariateOnlineSummarizer())
          newValues.foreach(value => newSummary.add(Vectors.dense(value)))
          Some(newSummary)
        })
    } else {
      val windowDuration = data.slideDuration * this.windowSize
      data
        .groupByKeyAndWindow(windowDuration)
        .mapValues { values =>
          val summary = new MultivariateOnlineSummarizer()
          values.foreach(value => summary.add(Vectors.dense(value)))
          summary
        }
    }
  }

  /**
   * Transform a stream of summaries into pairs representing summary statistics for group A and
   * group B up to this batch.
   */
  private[stat] def pairSummaries(summarizedData: DStream[(Boolean, MultivariateOnlineSummarizer)])
      : DStream[(MultivariateOnlineSummarizer, MultivariateOnlineSummarizer)] = {
    summarizedData.transform { rdd =>
      val statsA = rdd.filter(_._1).values
      val statsB = rdd.filter(!_._1).values
      statsA.cartesian[MultivariateOnlineSummarizer](statsB)
    }
  }
}
