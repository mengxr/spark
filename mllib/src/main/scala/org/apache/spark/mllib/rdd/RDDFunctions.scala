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

package org.apache.spark.mllib.rdd

import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.storage.StorageLevel

/**
 * Machine learning specific RDD functions.
 */
// private[mllib]
class RDDFunctions[T: ClassTag](self: RDD[T]) {

  /**
   * Returns a RDD from grouping items of its parent RDD in fixed size blocks by passing a sliding
   * window over them. The ordering is first based on the partition index and then the ordering of
   * items within each partition. This is similar to sliding in Scala collections, except that it
   * becomes an empty RDD if the window size is greater than the total number of items. It needs to
   * trigger a Spark job if the parent RDD has more than one partitions and the window size is
   * greater than 1.
   */
  def sliding(windowSize: Int): RDD[Seq[T]] = {
    require(windowSize > 0, s"Sliding window size must be positive, but got $windowSize.")
    if (windowSize == 1) {
      self.map(Seq(_))
    } else {
      new SlidingRDD[T](self, windowSize)
    }
  }

  /**
   * Computes the all-reduced RDD of the parent RDD, which has the same number of partitions and
   * locality information as its parent RDD. Each partition contains only one record, which is the
   * same as calling `RDD#reduce` on its parent RDD.
   *
   * @param f reducer
   * @return all-reduced RDD
   */
  def allReduce0(f: (T, T) => T): RDD[T] = {
    val numPartitions = self.partitions.size
    require(numPartitions > 0, "Parent RDD does not have any partitions.")

    var butterfly =
      self.mapPartitions((iter) => Iterator(iter.reduce(f)), preservesPartitioning = true)

    val nextPowerOfTwo = {
      var n = numPartitions - 1
      var i = 0
      while (n > 0) {
        n >>= 1
        i += 1
      }
      1 << i
    }

    if (nextPowerOfTwo > numPartitions) {
      val padding = self.context.parallelize(Seq.empty[T], nextPowerOfTwo - numPartitions)
      butterfly = butterfly.union(padding)
    }

    butterfly.persist(StorageLevel.MEMORY_AND_DISK)

    var rdds = Seq.empty[RDD[T]]
    var offset = nextPowerOfTwo >> 1
    while (offset > 0) {
      rdds :+= butterfly
      butterfly =
        new ButterflyReducedRDD[T](butterfly, f, offset).persist(StorageLevel.MEMORY_AND_DISK)
      offset >>= 1
    }

    butterfly = if (nextPowerOfTwo > numPartitions) {
      PartitionPruningRDD.create(butterfly, (i) => i < numPartitions)
    } else {
      butterfly
    }

    butterfly.count()
    rdds.foreach(rdd => rdd.unpersist(blocking = false))

    butterfly
  }

  def allReduce1(f: (T, T) => T): RDD[T] = {
    val numPartitions = self.partitions.size
    require(numPartitions > 0, "Parent RDD does not have any partitions.")

    var butterfly =
      self.mapPartitions((iter) => Iterator(iter.reduce(f)), preservesPartitioning = true)

    val nextPowerOfTwo = {
      var n = numPartitions - 1
      var i = 0
      while (n > 0) {
        n >>= 1
        i += 1
      }
      1 << i
    }

    if (nextPowerOfTwo > numPartitions) {
      val padding = self.context.parallelize(Seq.empty[T], nextPowerOfTwo - numPartitions)
      butterfly = butterfly.union(padding)
    }

    var offset = nextPowerOfTwo >> 1
    while (offset > 0) {
      val localOffset = offset
      val expanded = butterfly.mapPartitionsWithIndex((i, iter) =>
        if (iter.hasNext) {
          val item = iter.next()
          val outIter = Iterator((i, item), ((i + localOffset) % nextPowerOfTwo, item))
          require(!iter.hasNext, "Must have at most one record per partition.")
          outIter
        } else {
          Iterator.empty
        }, preservesPartitioning = true)
      val shuffled = new ButterflyShuffledRDD[T](expanded, offset)
      butterfly = shuffled.mapPartitions((iter) => Iterator(iter.map(_._2).reduce(f)),
        preservesPartitioning = true)
      offset >>= 1
    }

    if (nextPowerOfTwo > numPartitions) {
      PartitionPruningRDD.create(butterfly, (i) => i < numPartitions)
    } else {
      butterfly
    }
  }

  def allReduce2(f: (T, T) => T): RDD[T] = {
    val reduced = self.context.broadcast(self.reduce(f))
    self.mapPartitions((iter) => Iterator(reduced.value), preservesPartitioning = true)
  }
}

private[mllib]
object RDDFunctions {

  /** Implicit conversion from an RDD to RDDFunctions. */
  implicit def fromRDD[T: ClassTag](rdd: RDD[T]) = new RDDFunctions[T](rdd)
}
