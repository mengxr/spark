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

package org.apache.spark.ml.param

import scala.collection.mutable

import org.scalatest.FunSuite

class ParamsSuite extends FunSuite {

  class Solver extends Params {
    val maxIter = new IntParam(this, "maxIter", "max number of iterations", Some(100))
    def setMaxIter(value: Int): this.type = { set(maxIter, value); this }
    def getMaxIter: Int = get(maxIter)
    val inputCol = new Param[String](this, "inputCol", "input column name")
    def setInputCol(value: String): this.type = { set(inputCol, value); this }
    def getInputCol: String = get(inputCol)
    override def validate(paramMap: ParamMap) = {
      val m = this.paramMap ++ paramMap
      assert(m(maxIter) >= 0)
      assert(m.contains(inputCol))
    }
  }

  val solver = new Solver()
  import solver.{maxIter, inputCol}

  test("param") {
    assert(maxIter.name === "maxIter")
    assert(maxIter.doc === "max number of iterations")
    assert(maxIter.default.get === 100)
    assert(maxIter.parent.eq(solver))
    assert(maxIter.toString === "maxIter: max number of iterations (default: 100)")
    assert(inputCol.default === None)
  }

  test("param pair") {
    val pair0 = maxIter -> 5
    val pair1 = ParamPair(maxIter, 5)
    for (pair <- Seq(pair0, pair1)) {
      assert(pair.param.eq(maxIter))
      assert(pair.value === 5)
    }
  }

  test("param map") {
    val map0 = ParamMap.empty

    assert(!map0.contains(maxIter))
    assert(map0(maxIter) === maxIter.default.get)
    map0.put(maxIter, 10)
    assert(map0.contains(maxIter))
    assert(map0(maxIter) === 10)

    assert(!map0.contains(inputCol))
    intercept[NoSuchElementException] {
      map0(inputCol)
    }
    map0.put(inputCol -> "input")
    assert(map0.contains(inputCol))
    assert(map0(inputCol) === "input")

    val map1 = map0.copy
    val map2 = ParamMap(maxIter -> 10, inputCol -> "input")
    val map3 = new ParamMap()
      .put(maxIter, 10)
      .put(inputCol, "input")
    val map4 = ParamMap.empty ++ map0
    val map5 = ParamMap.empty
    map5 ++= map0

    for (m <- Seq(map1, map2, map3, map4)) {
      assert(m.contains(maxIter))
      assert(m(maxIter) === 10)
      assert(m.contains(inputCol))
      assert(m(inputCol) === "input")
    }
  }

  test("params") {
    val params = solver.params
    assert(params.size === 2)
    assert(params(0).eq(inputCol), "params must be ordered by name")
    assert(params(1).eq(maxIter))
    assert(solver.explainParams() === Seq(inputCol, maxIter).mkString("\n"))
    assert(solver.getParam("inputCol").eq(inputCol))
    assert(solver.getParam("maxIter").eq(maxIter))
    intercept[NoSuchMethodException] {
      solver.getParam("abc")
    }
    assert(!solver.isSet(inputCol))
    intercept[Exception] {
      solver.validate()
    }
    solver.validate(ParamMap(inputCol -> "input"))
    solver.setInputCol("input")
    assert(solver.isSet(inputCol))
    assert(solver.getInputCol === "input")
    solver.validate()
    intercept[Exception] {
      solver.validate(ParamMap(maxIter -> -10))
    }
    solver.setMaxIter(-10)
    intercept[Exception] {
      solver.validate()
    }
  }

  test("param grid builder") {
    def validateGrid(maps: Array[ParamMap], expected: mutable.Set[(Int, String)]): Unit = {
      assert(maps.size === expected.size)
      maps.foreach { m =>
        val tuple = (m(maxIter), m(inputCol))
        assert(expected.contains(tuple))
        expected.remove(tuple)
      }
      assert(expected.isEmpty)
    }

    val maps0 = new ParamGridBuilder()
      .baseOn(maxIter -> 10)
      .addGrid(inputCol, Array("input0", "input1"))
      .build()
    val expected0 = mutable.Set(
      (10, "input0"),
      (10, "input1"))
    validateGrid(maps0, expected0)

    val maps1 = new ParamGridBuilder()
      .baseOn(ParamMap(maxIter -> 5, inputCol -> "input")) // will be overwritten
      .addGrid(maxIter, Array(10, 20))
      .addGrid(inputCol, Array("input0", "input1"))
      .build()
    val expected1 = mutable.Set(
      (10, "input0"),
      (20, "input0"),
      (10, "input1"),
      (20, "input1"))
    validateGrid(maps1, expected1)
  }
}
