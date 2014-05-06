package org.apache.spark.mllib.rdd

import org.apache.spark.{Dependency, TaskContext, Partition, NarrowDependency}

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD

/**
 * Represents a binary tree dependency, where partition `i` depends on partitions `2 * i` and
 * `2 * i + 1` (if it exists) of the parent RDD.
 * @param rdd parent RDD
 * @tparam T value type
 */
private class BinaryTreeDependency[T](@transient rdd: RDD[T]) extends NarrowDependency(rdd) {

  val n = rdd.partitions.size

  override def getParents(partitionId: Int): Seq[Int] = {
    val i1 = 2 * partitionId
    val i2 = i1 + 1
    if (i2 < n) {
      Seq(i1, i2)
    } else {
      Seq(i1)
    }
  }
}

private class BinaryTreeNodePartition(
    override val index: Int,
    val left: Partition,
    val right: Option[Partition]) extends Partition {
}

private object BinaryTreeNodePartition {
  def apply(rdd: RDD[_], i: Int): Partition = {
    val n = rdd.partitions.size
    val i1 = 2 * i
    val i2 = i1 + 1
    if (i2 < n) {
      new BinaryTreeNodePartition(i, rdd.partitions(i1), Some(rdd.partitions(i2)))
    } else {
      new BinaryTreeNodePartition(i, rdd.partitions(i1), None)
    }
  }
}

private class TreeNodePartition(
    override val index: Int,
    val parents: Seq[Partition]) extends Partition

private class TreeDependency[T](@transient rdd: RDD[T], degree: Int) extends NarrowDependency(rdd) {

  val numParentPartitions = rdd.partitions.size

  override def getParents(partitionId: Int): Seq[Int] = {
    TreeDependency.getParents(numParentPartitions, degree, partitionId)
  }
}

private object TreeDependency {

  def getNumPartitions(numParentPartitions: Int, degree: Int): Int = {
    require(numParentPartitions > 0)
    require(degree > 1)
    (numParentPartitions - 1) / degree + 1
  }

  def getParents(numParentPartitions: Int, degree: Int, partitionId: Int): Seq[Int] = {
    require(numParentPartitions > 0)
    require(degree > 1)

    require(partitionId < getNumPartitions(numParentPartitions, degree))
    val start = partitionId * degree
    start until math.min(start + degree, numParentPartitions)
  }
}

private[mllib] class SquareRootReducedRDD[T: ClassTag](rdd: RDD[T], f: (T, T) => T)
  extends RDD[T](rdd.context, null) {

  val degree = math.sqrt(rdd.partitions.size).ceil.toInt

  override protected def getDependencies: Seq[Dependency[_]] = {
    Seq(new TreeDependency(rdd, degree))
  }

  override protected def getPartitions: Array[Partition] = {
    val numParentPartitions = rdd.partitions.size
    val numPartitions = TreeDependency.getNumPartitions(numParentPartitions, degree)
    Array.tabulate(numPartitions) { i =>
      val parents = TreeDependency.getParents(numParentPartitions, degree, i)
      new TreeNodePartition(i, parents.map(j => rdd.partitions(j)))
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val p = split.asInstanceOf[TreeNodePartition]
    p.parents.flatMap(rdd.compute(_, context)).reduceLeftOption(f).toIterator
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    val p = split.asInstanceOf[TreeNodePartition]
    rdd.preferredLocations(p.parents.head)
  }
}

private[mllib] class BinaryTreeReducedRDD[T: ClassTag](rdd: RDD[T], f: (T, T) => T)
  extends RDD[T](rdd.context, List(new BinaryTreeDependency(rdd))) {

  override protected def getPartitions: Array[Partition] = {
    Array.tabulate((rdd.partitions.size + 1) / 2)(i => BinaryTreeNodePartition(rdd, i))
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val p = split.asInstanceOf[BinaryTreeNodePartition]
    val iterLeft = rdd.compute(p.left, context)
    val iterRight = if (p.right.isDefined) rdd.compute(p.right.get, context) else Iterator.empty
    val iter = iterLeft ++ iterRight
    if (iter.isEmpty) {
      Iterator.empty
    } else {
      Iterator(iter.reduce(f))
    }
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    val p = split.asInstanceOf[BinaryTreeNodePartition]
    rdd.preferredLocations(p.left)
  }
}
