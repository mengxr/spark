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

package org.apache.spark.mllib.linalg

import org.scalatest.FunSuite

import org.apache.mahout.math.{
  DenseVector => MahoutDenseVector,
  SequentialAccessSparseVector
}

class VecSuite extends FunSuite {

  val arr = Array(0.1, 0.2, 0.3, 0.4)
  val n = 20
  val indices = Array(0, 3, 5, 10, 13)
  val values = Array(0.1, 0.5, 0.3, -0.8, -1.0)

  test("dense vector construction") {

    val vec = Vec.newDenseVec(values)
    val mahoutVector = new MahoutDenseVector(values)

    assert(vec.toMahout.unwrap() == mahoutVector)
  }

  test("sparse vector construction") {

    val vec = Vec.newSparseVec(n, indices, values)
    val mahoutVector = new SequentialAccessSparseVector(n, indices.length)
    indices.zip(values).foreach { x =>
      mahoutVector.set(x._1, x._2)
    }

    assert(vec.toMahout.unwrap() == mahoutVector)
  }

  test("sparse vector construction with unordered elements") {

    val vec = Vec.newSparseVec(n, indices.zip(values).reverse)
    val mahoutVector = new SequentialAccessSparseVector(n, indices.length)
    indices.zip(values).foreach { x =>
      mahoutVector.set(x._1, x._2)
    }

    assert(vec.toMahout.unwrap() == mahoutVector)
  }
}
