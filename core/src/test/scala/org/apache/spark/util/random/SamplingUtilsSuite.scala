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

import scala.util.Random

import org.apache.commons.math3.distribution.{BinomialDistribution, PoissonDistribution}
import org.scalatest.FunSuite

class SamplingUtilsSuite extends FunSuite {

  test("reservoirSampleAndCount") {
    val input = Seq.fill(100)(Random.nextInt())

    // input size < k
    val (sample1, count1) = SamplingUtils.reservoirSampleAndCount(input.iterator, 150)
    assert(count1 === 100)
    assert(input === sample1.toSeq)

    // input size == k
    val (sample2, count2) = SamplingUtils.reservoirSampleAndCount(input.iterator, 100)
    assert(count2 === 100)
    assert(input === sample2.toSeq)

    // input size > k
    val (sample3, count3) = SamplingUtils.reservoirSampleAndCount(input.iterator, 10)
    assert(count3 === 100)
    assert(sample3.length === 10)
  }

  test("PoissonBounds") {
    val delta = PoissonBounds.failureRate
    ((1 until 15) ++ (4 until 10).map(i => 1 << i)).foreach { s =>
      val lb = PoissonBounds.lower(s)
      assert(new PoissonDistribution(lb).cumulativeProbability(s) > 1.0 - delta)
      val ub = PoissonBounds.upper(s)
      assert(new PoissonDistribution(ub).cumulativeProbability(s) < delta)
    }
  }

  test("BinomialBounds") {
    val delta = BinomialBounds.failureRate
    for (i <- 0 until 10; j <- 0 until i) {
      val n = 1 << i
      val s = 1 << j
      val lb = BinomialBounds.lower(n, s)
      assert(new BinomialDistribution(n, lb).cumulativeProbability(s) > 1.0 - delta)
      val ub = BinomialBounds.upper(n, s)
      assert(new BinomialDistribution(n, ub).cumulativeProbability(s) < delta)
    }
  }
}
