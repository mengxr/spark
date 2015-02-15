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

package org.apache.spark.mllib.fpm

import scala.language.existentials

import org.scalatest.FunSuite

import org.apache.spark.mllib.util.MLlibTestSparkContext

class FPTreeSuite extends FunSuite with MLlibTestSparkContext {

  test("add transaction") {
    val tree = new FPTree()
      .add(Seq(0, 1, 2))
      .add(Seq(0, 1, 3))
      .add(Seq(1))

    assert(tree.root.children.size === 2)
    assert(tree.root.children.contains(0))
    assert(tree.root.children(0).item === 0)
    assert(tree.root.children(0).count === 2)
    assert(tree.root.children.contains(1))
    assert(tree.root.children(1).item === 1)
    assert(tree.root.children(1).count === 1)
    var child = tree.root.children(0)
    assert(child.children.size === 1)
    assert(child.children.contains(1))
    assert(child.children(1).item === 1)
    assert(child.children(1).count === 2)
    child = child.children(1)
    assert(child.children.size === 2)
    assert(child.children.contains(2))
    assert(child.children.contains(3))
    assert(child.children(2).item === 2)
    assert(child.children(3).item === 3)
    assert(child.children(2).count === 1)
    assert(child.children(3).count === 1)
  }

  test("merge tree") {
    val tree1 = new FPTree()
      .add(Seq(0, 1, 2))
      .add(Seq(0, 1, 3))
      .add(Seq(1))

    val tree2 = new FPTree()
      .add(Seq(0, 1))
      .add(Seq(0, 1, 2))
      .add(Seq(0, 1, 2, 4))
      .add(Seq(0, 5))
      .add(Seq(0, 5, 3))
      .add(Seq(2, 6))
      .add(Seq(2, 7))

    val tree3 = tree1.merge(tree2)

    assert(tree3.root.children.size === 3)
    assert(tree3.root.children(0).count === 7)
    assert(tree3.root.children(1).count === 1)
    assert(tree3.root.children(2).count === 2)
    val child1 = tree3.root.children(0)
    assert(child1.children.size === 2)
    assert(child1.children(1).count === 5)
    assert(child1.children(5).count === 2)
    val child2 = child1.children(1)
    assert(child2.children.size === 2)
    assert(child2.children(3).count === 1)
    assert(child2.children(2).count === 3)
    val child3 = child2.children(2)
    assert(child3.children.size === 1)
    assert(child3.children(4).count === 1)
    val child4 = child1.children(5)
    assert(child4.children.size === 1)
    assert(child4.children(3).count === 1)
    val child5 = tree3.root.children(2)
    assert(child5.children.size === 2)
    assert(child5.children(6).count === 1)
    assert(child5.children(7).count === 1)
  }

  test("extract freq itemsets") {
    val tree = new FPTree()
      .add(Seq(0, 1, 2))
      .add(Seq(0, 1, 3))
      .add(Seq(0, 1))
      .add(Seq(0))
      .add(Seq(1))
      .add(Seq(1, 6))

    val freqItemsets = tree.extract(3L).map { case (items, count) =>
      (items.toSet, count)
    }.toSet
    val expected = Set(
      (Set(0), 4L),
      (Set(1), 5L),
      (Set(0, 1), 3L))
    assert(freqItemsets === expected)
  }
}
