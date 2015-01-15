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

import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.stat.test.StreamingChiSqTest
import org.apache.spark.streaming.{StreamingContext, TestSuiteBase}
import org.apache.spark.streaming.dstream.DStream
import org.scalatest.FunSuite
import org.apache.spark.mllib.stat.test.ChiSqTest

class StreamingChiSqTestSuite extends FunSuite with TestSuiteBase {

  override def maxWaitTimeMillis = 30000


  test("calculate exact p and Chi-square statistic correctly") {

    // Serially calculate Chi-square statistics for some data
    // Note: these did not agree with results obtained in R
    val serialTestResult = ChiSqTest.chiSquaredMatrix(Matrices.dense(numRows = 2, numCols = 2,
      values = Array(1000, 2500, 990, 2550)))

    val chiSqTester = new StreamingChiSqTest()

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

}


