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

import scala.collection.mutable.ListBuffer

import org.apache.spark.util.collection.PrimitiveKeyOpenHashMap

/**
 * FP-Tree data structure used in FP-Growth.
 */
private[fpm] class FPTree extends Serializable {

  import FPTree._

  val root: Node = new Node(null)

  private val summaries: PrimitiveKeyOpenHashMap[Int, Summary] = new PrimitiveKeyOpenHashMap()

  /** Adds a transaction with count. */
  def add(t: Iterable[Int], count: Long = 1L): this.type = {
    require(count > 0)
    var curr = root
    curr.count += count
    t.foreach { item =>
      val summary = if (summaries.contains(item)) {
        summaries(item)
      } else {
        val s = new Summary
        summaries.update(item, s)
        s
      }
      summary.count += count
      summaries.update(item, summary)
      val children = curr.children
      val child = if (children.contains(item)) {
        children(item)
      } else {
        val c = new Node(curr)
        c.item = item
        summary.nodes += c
        children.update(item, c)
        c
      }
      child.count += count
      curr.children.update(item, child)
      curr = child
    }
    this
  }

  /** Merges another FP-Tree. */
  def merge(other: FPTree): this.type = {
    other.transactions.foreach { case (t, c) =>
      add(t, c)
    }
    this
  }

  /** Gets a subtree with the suffix. */
  private def project(suffix: Int): FPTree = {
    val tree = new FPTree
    if (summaries.contains(suffix)) {
      val summary = summaries(suffix)
      summary.nodes.foreach { node =>
        var t = List.empty[Int]
        var curr = node.parent
        while (!curr.isRoot) {
          t = curr.item :: t
          curr = curr.parent
        }
        tree.add(t, node.count)
      }
    }
    tree
  }

  /** Returns all transactions in an iterator. */
  def transactions: Iterator[(List[Int], Long)] = getTransactions(root)

  /** Returns all transactions under this node. */
  private def getTransactions(node: Node): Iterator[(List[Int], Long)] = {
    var count = node.count
    node.children.iterator.flatMap { case (item, child) =>
      getTransactions(child).map { case (t, c) =>
        count -= c
        (item :: t, c)
      }
    } ++ {
      if (count > 0) {
        Iterator.single((Nil, count))
      } else {
        Iterator.empty
      }
    }
  }

  /** Extracts all patterns with valid suffix and minimum count. */
  def extract(
      minCount: Long,
      validateSuffix: Int => Boolean = _ => true): Iterator[(List[Int], Long)] = {
    summaries.iterator.flatMap { case (item, summary) =>
      if (validateSuffix(item) && summary.count >= minCount) {
        Iterator.single((item :: Nil, summary.count)) ++
          project(item).extract(minCount).map { case (t, c) =>
            (item :: t, c)
          }
      } else {
        Iterator.empty
      }
    }
  }
}

private[fpm] object FPTree {

  /** Representing a node in an FP-Tree. */
  class Node(val parent: Node) extends Serializable {
    var item: Int = -1
    var count: Long = 0L
    val children: PrimitiveKeyOpenHashMap[Int, Node] = new PrimitiveKeyOpenHashMap()

    def isRoot: Boolean = parent == null
  }

  /** Summary of a item in an FP-Tree. */
  private class Summary extends Serializable {
    var count: Long = 0L
    val nodes: ListBuffer[Node] = ListBuffer.empty
  }
}
