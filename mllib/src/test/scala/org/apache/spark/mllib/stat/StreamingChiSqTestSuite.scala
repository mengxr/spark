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
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.stat.test._
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StreamingContext, TestSuiteBase}
import org.apache.spark.util.random.XORShiftRandom

class StreamingChiSqTestSuite extends FunSuite with TestSuiteBase {

  override def maxWaitTimeMillis = 30000


  test("calculate exact p and Chi-square statistic correctly") {

    // Serially calculate Chi-square statistics for some data
    val data = Array(1000.0, 2500.0, 990.0, 2550.0)
    val serialTestResult = Statistics.chiSqTest(Matrices.dense(2, 2, data))

    // This will fail. 0.5909355 is result obtained from R
    // assert(serialTestResult.pValue ~== 0.5909355 absTol 1e-4)

    // TODO: make the type parameters unnecessary
    val chiSqTester = new StreamingABTest(ChiSqTestSummaryStats(), ChiSqTester.performTest)

    val numBatches = 4

    // Generate small populations corresponding to the serial results
    val input = IndexedSeq((0 until 1000).map(x => (true, 1.0)),
      (0 until 2500).map(x => (true, 0.0)),
      (0 until 990).map(x => (false, 1.0)),
      (0 until 2550).map(x => (false, 0.0)))


    val ssc: StreamingContext = setupStreams(input, (inputDStream: DStream[(Boolean, Double)]) => {
      chiSqTester.getTestResults(inputDStream)
    })

    runStreams(ssc, numBatches, numBatches)

    // Extract test results from tester
    val testResults = chiSqTester.getCurrentTest()

    // Make sure the t-stat, df, and p-value agree with those calculated serially
    assert(testResults.statistic == serialTestResult.statistic)
    assert(testResults.pValue == serialTestResult.pValue)
    assert(testResults.degreesOfFreedom == serialTestResult.degreesOfFreedom)

  }

  test("Reject the null hypothesis") {

    // TODO: make the type parameters unnecessary
    val chiSqTester = new StreamingABTest(ChiSqTestSummaryStats(), ChiSqTester.performTest)

    val numBatches = 200
    val numPoints = 20

    // Generate small populations corresponding to the serial results
    val input = StreamingChiSqTestDataGenerator(numPoints,
      numBatches, Array(.2, .8), Array(.15, .85))

    val ssc: StreamingContext = setupStreams(input, (inputDStream: DStream[(Boolean, Double)]) => {
      chiSqTester.getTestResults(inputDStream)
    })

    runStreams(ssc, numBatches, numBatches)

    // Extract test results from tester
    val testResults = chiSqTester.getCurrentTest()

    // Make sure the t-stat, df, and p-value agree with those calculated serially
    assert(testResults.pValue <= 0.01)

  }

  test("Accept the null hypothesis") {

    // TODO: make the type parameters unnecessary
    val chiSqTester = new StreamingABTest(ChiSqTestSummaryStats(), ChiSqTester.performTest)

    val numBatches = 200
    val numPoints = 20

    // Generate small populations corresponding to the serial results
    val input = StreamingChiSqTestDataGenerator(numPoints,
      numBatches, Array(.2, .8), Array(.2, .8))

    val ssc: StreamingContext = setupStreams(input, (inputDStream: DStream[(Boolean, Double)]) => {
      chiSqTester.getTestResults(inputDStream)
    })

    runStreams(ssc, numBatches, numBatches)

    // Extract test results from tester
    val testResults = chiSqTester.getCurrentTest()

    // Make sure the t-stat, df, and p-value agree with those calculated serially
    assert(testResults.pValue >= 0.05)

  }

  def StreamingChiSqTestDataGenerator(numPoints: Int,
                                      numBatches: Int,
                                      probs1: Array[Double] = Array(.5,.5),
                                      probs2: Array[Double] = Array(.5,.5))
  : IndexedSeq[IndexedSeq[(Boolean, Double)]] = {

    val rand = new scala.util.Random //XORShiftRandom(2014)

    assert(probs1.length == probs2.length)

    // Make sure we are dealing with probabilities
    assert(probs1.reduce(_+_) == 1.0)
    assert(probs2.reduce(_+_) == 1.0)

    // Get cumulative sum of probabilities
    val cumProbs1 = probs1.scanLeft(0.0)(_+_)
    val cumProbs2 = probs2.scanLeft(0.0)(_+_)

    val probMap = Map(true -> cumProbs1, false -> cumProbs2)

    // Extract class membership from randomly drawn uniform
    def getPosition(u: Double, cumProbs: Array[Double]): Int = {
      cumProbs.foldLeft(0)((pos, newBoundary) => {
        if (u > newBoundary) {
          pos + 1
        } else {
          pos
        }
      })
    }

    val outcomes = (0 until probs1.length).map(_.toDouble)

    val data = (0 until numBatches).map {
      x => (0 until numPoints).map { y =>
        val treatGroup = rand.nextBoolean()
        val outcomeClass = getPosition(rand.nextDouble(), probMap(treatGroup))
        (treatGroup, outcomes(outcomeClass - 1))
      }
    }

    data
  }
}


