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

package org.apache.spark.mllib.stat

import org.scalatest.FunSuite
import org.apache.spark.mllib.stat.test.{TTester, TTestResult, TTestSummaryStats, StreamingABTest}
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StreamingContext, TestSuiteBase}
import org.apache.spark.util.random.XORShiftRandom


class StreamingTTestSuite extends FunSuite with TestSuiteBase {

  override def maxWaitTimeMillis = 30000

  test("calculate exact p and t value correctly") {

    val tTester = new StreamingABTest(TTestSummaryStats(), TTester.performTest)

    val numPoints = 200
    val numBatches = 10

    // Generate small populations
    val input = IndexedSeq(IndexedSeq((true, -1.0), (false, 1.0), (true, -2.0), (false, .2)),
      IndexedSeq((true, 0.0), (false, .75), (true, -.5), (false, 1.2), (false, .3)))

    val ssc: StreamingContext = setupStreams(input, (inputDStream: DStream[(Boolean, Double)]) => {
      tTester.getTestResults(inputDStream)
    })

    runStreams(ssc, numBatches, numBatches)

    // Extract test results from tester
    val testResults: TTestResult = tTester.getCurrentTest()

    // Make sure the t-stat, df, and p-value agree with those calculated in R using t.test
    assert(testResults.statistic ~== -3.3374 absTol 1e-4)
    assert(testResults.degreesOfFreedom ~== 4.23 absTol 1e-2)
    assert(testResults.pValue ~== 0.02656 absTol 1e-5)


  }

  test("reject null hypothesis in case of different means") {

    val tTester = new StreamingABTest(TTestSummaryStats(), TTester.performTest)

    val numPoints = 200
    val numBatches = 10

    // Simulate data from two populations with unequal means and equal variances
    val input = StreamingTTestDataGenerator(numPoints, numBatches)

    val ssc: StreamingContext = setupStreams(input, (inputDStream: DStream[(Boolean, Double)]) => {
      tTester.getTestResults(inputDStream)
    })

    runStreams(ssc, numBatches, numBatches)

    // Make sure the null is rejected at the alpha = 0.01 level
    assert(tTester.getCurrentTest().pValue < 0.01)

  }

  test("accept null hypothesis in case of same means, different variances") {

    val tTester = new StreamingABTest(TTestSummaryStats(), TTester.performTest)

    val numPoints = 200
    val numBatches = 10

    // Simulate data from two populations with equal means and unequal variances
    val input = StreamingTTestDataGenerator(numPoints, numBatches, 5, 5, 1, 3)

    val ssc: StreamingContext = setupStreams(input, (inputDStream: DStream[(Boolean, Double)]) => {
      tTester.getTestResults(inputDStream)
    })

    runStreams(ssc, numBatches, numBatches)

    // Make sure the null is not rejected at the alpha = 0.05 level
    assert(tTester.getCurrentTest().pValue > 0.05)

  }

  test("test with huge means and variances when null is true") {

    val tTester = new StreamingABTest(TTestSummaryStats(), TTester.performTest)

    val numPoints = 2000
    val numBatches = 100

    // Simulate data from two populations with equal means and unequal variances
    // Note: this test will fail if means are 1E20, variances are 1E10
    val input = StreamingTTestDataGenerator(numPoints, numBatches, 1E10, 1E10, 1E2, 1E5)

    val ssc: StreamingContext = setupStreams(input, (inputDStream: DStream[(Boolean, Double)]) => {
      tTester.getTestResults(inputDStream)
    })

    runStreams(ssc, numBatches, numBatches)

    // Make sure the null is not rejected at the alpha = 0.05 level
    assert(tTester.getCurrentTest().pValue > 0.05)

  }

  def StreamingTTestDataGenerator(numPoints: Int,
                                  numBatches: Int,
                                  center1: Double = -5,
                                  center2: Double = 5,
                                  sigma1: Double = 1,
                                  sigma2: Double = 1): IndexedSeq[IndexedSeq[(Boolean, Double)]] = {

    val rand = new XORShiftRandom(2014)

    val centers = Map(true -> center1, false -> center2)
    val sigmas = Map(true -> sigma1, false -> sigma2)
    val data = (0 until numBatches).map {
      x => (0 until numPoints).map { y =>
        val treatGroup = rand.nextBoolean()
        (treatGroup, centers(treatGroup) + sigmas(treatGroup) * rand.nextGaussian())
      }
    }

    data
  }
}


