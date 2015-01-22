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
import spire.implicits._

/**
 * SummaryStatistics stores summaries of data necessary for performing a statistical
 * test. Two instantiations of a class that implements SummaryStatistics need to be able
 * to be combined to allow distributed computation of statistics. Also, a method must be
 * provided to instantiate an empty SummaryStatistic
 *
 * Usage example:
 * class TTestSummaryStats extends SummaryStatistics[TTestSummaryStats] { ... }
 *
 * @tparam U Recursive type parameter.  See usage example.
 */
abstract class SummaryStatistics[U] extends Serializable {
  /**
   * Initialize an empty SummaryStatistics
   * @return SummaryStatistics object with no data
   */
  def emptySummary(): U

  /**
   * Initialize a SummaryStatistics from a single observation
   * @param x The observation.
   * @return SummaryStatistics object representing one data point
   */
  def initializer(x: Double): U

  /**
   * Combine two summary statistics. Should maintain the same memory usage of a single
   * summary statistic.
   * @param other SummaryStatistics from another set of observations
   * @return SummaryStatistics object
   */
  def combine(other: U): U
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
case class TTestSummaryStats(mu: Double = 0.0,
                             sigma:Double = 0.0, n:Long = 0)
    extends SummaryStatistics[TTestSummaryStats] {

  def emptySummary = TTestSummaryStats()

  /** A single observation has zero variance */
  def initializer(x: Double) = TTestSummaryStats(x, 0.0, 1)

  /**
   * Combine this set of summary statistics with another.
   * @see [[http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm]]
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


/**
 * Store the summaries of data necessary to perform Chi-square test. This stores the
 * counts of all outcomes seen so far.
 * @param counts Map from outcome (represented as a Double) to count of observations
 */
case class ChiSqTestSummaryStats(val counts: Map[Double, Long]
                                 = Map[Double, Long]().withDefaultValue(0))
    extends SummaryStatistics[ChiSqTestSummaryStats] {

  def emptySummary = ChiSqTestSummaryStats()
  def initializer(x: Double) = ChiSqTestSummaryStats(Map(x -> 1.toLong).withDefaultValue(0))

  /**
   * Combine this set of summary statistics with another. Outer join of two Maps.
   * @param other summary statistics from other population
   * @return new set of combined summary statistics
   */
  def combine(other: ChiSqTestSummaryStats): ChiSqTestSummaryStats = {
    // Use the spire implicits to join two Maps
    ChiSqTestSummaryStats(counts + other.counts)
  }
}
