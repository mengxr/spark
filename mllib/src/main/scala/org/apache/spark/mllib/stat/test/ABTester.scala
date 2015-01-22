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

import scala.math._
import org.apache.commons.math3.distribution.TDistribution
import org.apache.spark.mllib.linalg.Matrices


/**
 * Trait to represent an object that performs an AB test using the summary
 * statistics of two populations.
 *
 * @tparam SummaryType The type of the SummaryStatistics, must implement
 *                     [[org.apache.spark.mllib.stat.test.SummaryStatistics]]
 * @tparam ResultType The type of test results, must implement
 *                    @see [[org.apache.spark.mllib.stat.test.TestResult]]
 *
 */

trait ABTester[SummaryType <: SummaryStatistics[SummaryType], ResultType <: TestResult[_]] {

  /**
   * Perform the statistical test.
   * @param pop1 Summary statistics from first population
   * @param pop2 Summary statistics from second population
   * @return Test results
   */
  def performTest(pop1: SummaryType, pop2: SummaryType): ResultType
}

/**
 * TTester implements Welch's two-sample t-test on the populations.
 */
object TTester extends org.apache.commons.math3.stat.inference.TTest
with ABTester[TTestSummaryStats, TTestResult]
 {

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

      val t = super.t(pop1.mu, pop2.mu,
        sigCorrect1 * pop1.sigma, sigCorrect2 * pop2.sigma,
        pop1.n, pop2.n)

      val df = super.df(sigCorrect1 * pop1.sigma, sigCorrect2 * pop2.sigma,
        pop1.n, pop2.n)

      val p = 2 - 2 * (new TDistribution(df)).cumulativeProbability(abs(t))

      new TTestResult(p, df, t, "Unpaired t-test with unequal variances",
        "Two populations have the same mean")
    }
  }
}

/**
 * ChiSqTester implements Chi-Squared test of independence between population counts
 * and treatment group.
 */
object ChiSqTester extends ABTester[ChiSqTestSummaryStats, ChiSqTestResult] {

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
    val allKeys = (pop1.counts.keys ++ pop2.counts.keys).toArray.distinct

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

