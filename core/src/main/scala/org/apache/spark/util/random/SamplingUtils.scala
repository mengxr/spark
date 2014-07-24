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

package org.apache.spark.util.random

import scala.reflect.ClassTag
import scala.util.Random

private[spark] object SamplingUtils {

  /**
   * Reservoir sampling implementation that also returns the input size.
   *
   * @param input input size
   * @param k reservoir size
   * @param seed random seed
   * @return (samples, input size)
   */
  def reservoirSampleAndCount[T: ClassTag](
      input: Iterator[T],
      k: Int,
      seed: Long = Random.nextLong())
    : (Array[T], Int) = {
    val reservoir = new Array[T](k)
    // Put the first k elements in the reservoir.
    var i = 0
    while (i < k && input.hasNext) {
      val item = input.next()
      reservoir(i) = item
      i += 1
    }

    // If we have consumed all the elements, return them. Otherwise do the replacement.
    if (i < k) {
      // If input size < k, trim the array to return only an array of input size.
      val trimReservoir = new Array[T](i)
      System.arraycopy(reservoir, 0, trimReservoir, 0, i)
      (trimReservoir, i)
    } else {
      // If input size > k, continue the sampling process.
      val rand = new XORShiftRandom(seed)
      while (input.hasNext) {
        val item = input.next()
        val replacementIndex = rand.nextInt(i)
        if (replacementIndex < k) {
          reservoir(replacementIndex) = item
        }
        i += 1
      }
      (reservoir, i)
    }
  }

  /**
   * Returns a sampling rate that guarantees a sample of size >= sampleSizeLowerBound 99.99% of
   * the time.
   *
   * How the sampling rate is determined:
   * Let p = num / total, where num is the sample size and total is the total number of
   * datapoints in the RDD. We're trying to compute q > p such that
   *   - when sampling with replacement, we're drawing each datapoint with prob_i ~ Pois(q),
   *     where we want to guarantee Pr[s < num] < 0.0001 for s = sum(prob_i for i from 0 to total),
   *     i.e. the failure rate of not having a sufficiently large sample < 0.0001.
   *     Setting q = p + 5 * sqrt(p/total) is sufficient to guarantee 0.9999 success rate for
   *     num > 12, but we need a slightly larger q (9 empirically determined).
   *   - when sampling without replacement, we're drawing each datapoint with prob_i
   *     ~ Binomial(total, fraction) and our choice of q guarantees 1-delta, or 0.9999 success
   *     rate, where success rate is defined the same as in sampling with replacement.
   *
   * The smallest sampling rate supported is 1e-10 (in order to avoid running into the limit of the
   * RNG's resolution).
   *
   * @param sampleSizeLowerBound sample size
   * @param total size of RDD
   * @param withReplacement whether sampling with replacement
   * @return a sampling rate that guarantees sufficient sample size with 99.99% success rate
   */
  def computeFractionForSampleSize(sampleSizeLowerBound: Int, total: Long,
      withReplacement: Boolean): Double = {
    if (withReplacement) {
      PoissonBounds.upper(sampleSizeLowerBound)
    } else {
      BinomialBounds.upper(total, sampleSizeLowerBound)
    }
  }
}

/**
 * Bounds for Poisson distribution.
 */
private[spark] object PoissonBounds {

  val failureRate = 1e-4

  /**
   * Returns lambda such that Pr[X > s] is very small, where X ~ Pois(lambda).
   */
  def lower(s: Double): Double = {
    math.max(s - numStd(s) * math.sqrt(s), 1e-15)
  }

  /**
   * Returns lambda such that Pr[X < s] is very small, where X ~ Pois(lambda).
   */
  def upper(s: Double): Double = {
    s + numStd(s) * math.sqrt(s)
  }

  private def numStd(s: Double): Double = {
    if (s < 6.0) {
      12.0
    } else if (s < 16.0) {
      9.0
    } else {
      6.0
    }
  }
}

/**
 * Bounds for Binomial distribution.
 */
private[spark] object BinomialBounds {

  val failureRate = 1e-4

  /**
   * Returns p such that Pr[X > s] is very small, where X ~ B(n, p).
   */
  def lower(n: Long, s: Long): Double = {
    require(s <= n)
    val q = s.toDouble / n
    val gamma = - (2.0 / 3.0) * math.log(failureRate) / n
    val p = q + gamma - math.sqrt(gamma * gamma + 3.0 * gamma * q)
    math.max(p, 0.0)
  }

  /**
   * Returns p such that Pr[X < s] is very small, where X ~ B(n, p).
   */
  def upper(n: Long, s: Long): Double = {
    require(s <= n)
    val q = s.toDouble / n
    val gamma = - math.log(failureRate) / n
    val p = q + gamma + math.sqrt(gamma * gamma + 2.0 * gamma * q)
    math.min(p, 1.0)
  }
}
