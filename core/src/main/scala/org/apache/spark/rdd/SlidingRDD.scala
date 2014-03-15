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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.{TaskContext, Partition}
import scala.collection.mutable

private[spark]
class SlidingRDDPartition[T](val idx: Int, val partitions: Array[Partition])
  extends Partition with Serializable {
  override val index: Int = idx
}

/**
 * Represents a RDD from grouping items of its parent RDD in fixed size blocks by passing a sliding
 * window over them. The ordering is first based on the partition index and then the ordering of
 * items within each partition. This is similar to sliding in Scala collections, except that it
 * becomes an empty RDD if the window size is greater than the total number of items. It needs to
 * trigger a Spark job if the parent RDD has more than one partitions.
 *
 * @param parent the parent RDD
 * @param windowSize the window size, must be greater than 1
 *
 * @see [[org.apache.spark.rdd.RDD#sliding]]
 */
private[spark]
class SlidingRDD[T: ClassTag](val parent: RDD[T], val windowSize: Int)
  extends RDD[Seq[T]](parent) {

  require(windowSize > 1, s"Window size must be greater than 1, but got $windowSize.")

  override def compute(split: Partition, context: TaskContext): Iterator[Seq[T]] = {
    val part = split.asInstanceOf[SlidingRDDPartition[T]]
    val rdd = firstParent
    val partitions = part.partitions
    val n = partitions.size
    val iter = rdd.iterator(partitions(0), context)
    val tail = mutable.ListBuffer[T]()
    var i = 1
    while ((tail.size < windowSize - 1) && i < n) {
      tail ++= rdd.iterator(partitions(i), context).take(windowSize - 1)
      i += 1
    }
    (iter ++ tail.take(windowSize - 1))
      .sliding(windowSize)
      .withPartial(false)
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    val part = split.asInstanceOf[SlidingRDDPartition[T]]
    val partitions = part.partitions
    val preferred = firstParent.preferredLocations(partitions(0))
    if (partitions.size > 1) {
      val intersect = preferred.intersect(firstParent.preferredLocations(partitions(1)))
      if (!intersect.isEmpty) {
        return intersect
      }
    }
    preferred
  }

  override def getPartitions: Array[Partition] = {
    val parentPartitions = parent.partitions
    val n = parentPartitions.size
    Array.range(0, n).map(i => new SlidingRDDPartition[T](i, parentPartitions.slice(i, n)))
  }

  // TODO: Override methods such as aggregate, which only requires one Spark job.
}
