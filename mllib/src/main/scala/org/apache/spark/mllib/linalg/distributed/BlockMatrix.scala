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

package org.apache.spark.mllib.linalg.distributed

import breeze.linalg.{DenseMatrix => BDM}
import org.apache.spark.util.Utils

import org.apache.spark.{Logging, Partitioner}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * A grid partitioner, which stores every block in a separate partition.
 *
 * @param numRowBlocks Number of blocks that form the rows of the matrix.
 * @param numColBlocks Number of blocks that form the columns of the matrix.
 * @param suggestedNumPartitions Number of partitions to partition the rdd into. The final number
 *                               of partitions will be set to `min(suggestedNumPartitions,
 *                               numRowBlocks * numColBlocks)`, because setting the number of
 *                               partitions greater than the number of sub matrices is not useful.
 */
private[mllib] class GridPartitioner(
    val numRowBlocks: Int,
    val numColBlocks: Int,
    suggestedNumPartitions: Int) extends Partitioner {
  private val totalBlocks = numRowBlocks.toLong * numColBlocks
  // Having the number of partitions greater than the number of sub matrices does not help
  override val numPartitions = math.min(suggestedNumPartitions, totalBlocks).toInt

  private val blockLengthsPerPartition = findOptimalBlockLengths
  // Number of neighboring blocks to take in each row
  private val numRowBlocksPerPartition = blockLengthsPerPartition._1
  // Number of neighboring blocks to take in each column
  private val numColBlocksPerPartition = blockLengthsPerPartition._2
  // Number of rows of partitions
  private val blocksPerRow = math.ceil(numRowBlocks * 1.0 / numRowBlocksPerPartition).toInt

  /**
   * Returns the index of the partition the SubMatrix belongs to. Tries to achieve block wise
   * partitioning.
   *
   * @param key The key for the SubMatrix. Can be its position in the grid (its column major index)
   *            or a tuple of three integers that are the final row index after the multiplication,
   *            the index of the block to multiply with, and the final column index after the
   *            multiplication.
   * @return The index of the partition, which the SubMatrix belongs to.
   */
  override def getPartition(key: Any): Int = {
    key match {
      case (blockRowIndex: Int, blockColIndex: Int) =>
        getPartitionId(blockRowIndex, blockColIndex)
      case (blockRowIndex: Int, innerIndex: Int, blockColIndex: Int) =>
        getPartitionId(blockRowIndex, blockColIndex)
      case _ =>
        throw new IllegalArgumentException(s"Unrecognized key. key: $key")
    }
  }

  /** Partitions sub-matrices as blocks with neighboring sub-matrices. */
  private def getPartitionId(blockRowIndex: Int, blockColIndex: Int): Int = {
    require(0 <= blockRowIndex && blockRowIndex < numRowBlocks, "The blockRowIndex in the key " +
      s"must be in the range 0 <= blockRowIndex < numRowBlocks. blockRowIndex: $blockRowIndex," +
      s"numRowBlocks: $numRowBlocks")
    require(0 <= blockRowIndex && blockColIndex < numColBlocks, "The blockColIndex in the key " +
      s"must be in the range 0 <= blockRowIndex < numColBlocks. blockColIndex: $blockColIndex, " +
      s"numColBlocks: $numColBlocks")
    // Coordinates of the block
    val i = blockRowIndex / numRowBlocksPerPartition
    val j = blockColIndex / numColBlocksPerPartition
    // The mod shouldn't be required but is added as a guarantee for possible corner cases
    Utils.nonNegativeMod(j * blocksPerRow + i, numPartitions)
  }

  /** Tries to calculate the optimal number of blocks that should be in each partition. */
  private def findOptimalBlockLengths: (Int, Int) = {
    // Gives the optimal number of blocks that need to be in each partition
    val targetNumBlocksPerPartition = math.ceil(totalBlocks * 1.0 / numPartitions).toInt
    // Number of neighboring blocks to take in each row
    var m = math.ceil(math.sqrt(targetNumBlocksPerPartition)).toInt
    // Number of neighboring blocks to take in each column
    var n = math.ceil(targetNumBlocksPerPartition * 1.0 / m).toInt
    // Try to make m and n close to each other while making sure that we don't exceed the number
    // of partitions
    var numBlocksForRows = math.ceil(numRowBlocks * 1.0 / m)
    var numBlocksForCols = math.ceil(numColBlocks * 1.0 / n)
    while ((numBlocksForRows * numBlocksForCols > numPartitions) && (m * n != 0)) {
      if (numRowBlocks <= numColBlocks) {
        m += 1
        n = math.ceil(targetNumBlocksPerPartition * 1.0 / m).toInt
      } else {
        n += 1
        m = math.ceil(targetNumBlocksPerPartition * 1.0 / n).toInt
      }
      numBlocksForRows = math.ceil(numRowBlocks * 1.0 / m)
      numBlocksForCols = math.ceil(numColBlocks * 1.0 / n)
    }
    // If a good partitioning scheme couldn't be found, set the side with the smaller dimension to
    // 1 and the other to the number of targetNumBlocksPerPartition
    if (m * n == 0) {
      if (numRowBlocks <= numColBlocks) {
        m = 1
        n = targetNumBlocksPerPartition
      } else {
        n = 1
        m = targetNumBlocksPerPartition
      }
    }
    (m, n)
  }

  /** Checks whether the partitioners have the same characteristics */
  override def equals(obj: Any): Boolean = {
    obj match {
      case r: GridPartitioner =>
        (this.numRowBlocks == r.numRowBlocks) && (this.numColBlocks == r.numColBlocks) &&
          (this.numPartitions == r.numPartitions)
      case _ =>
        false
    }
  }
}

/**
 * Represents a distributed matrix in blocks of local matrices.
 *
 * @param rdd The RDD of SubMatrices (local matrices) that form this matrix
 * @param nRows Number of rows of this matrix. If the supplied value is less than or equal to zero,
 *              the number of rows will be calculated when `numRows` is invoked.
 * @param nCols Number of columns of this matrix. If the supplied value is less than or equal to
 *              zero, the number of columns will be calculated when `numCols` is invoked.
 * @param rowsPerBlock Number of rows that make up each block. The blocks forming the final
 *                     rows are not required to have the given number of rows
 * @param colsPerBlock Number of columns that make up each block. The blocks forming the final
 *                     columns are not required to have the given number of columns
 */
class BlockMatrix(
    val rdd: RDD[((Int, Int), Matrix)],
    private var nRows: Long,
    private var nCols: Long,
    val rowsPerBlock: Int,
    val colsPerBlock: Int) extends DistributedMatrix with Logging {

  private type SubMatrix = ((Int, Int), Matrix) // ((blockRowIndex, blockColIndex), matrix)

  /**
   * Alternate constructor for BlockMatrix without the input of the number of rows and columns.
   *
   * @param rdd The RDD of SubMatrices (local matrices) that form this matrix
   * @param rowsPerBlock Number of rows that make up each block. The blocks forming the final
   *                     rows are not required to have the given number of rows
   * @param colsPerBlock Number of columns that make up each block. The blocks forming the final
   *                     columns are not required to have the given number of columns
   */
  def this(
      rdd: RDD[((Int, Int), Matrix)],
      rowsPerBlock: Int,
      colsPerBlock: Int) = {
    this(rdd, 0L, 0L, rowsPerBlock, colsPerBlock)
  }

  private lazy val dims: (Long, Long) = getDim

  override def numRows(): Long = {
    if (nRows <= 0L) nRows = dims._1
    nRows
  }

  override def numCols(): Long = {
    if (nCols <= 0L) nCols = dims._2
    nCols
  }

  val numRowBlocks = math.ceil(numRows() * 1.0 / rowsPerBlock).toInt
  val numColBlocks = math.ceil(numCols() * 1.0 / colsPerBlock).toInt

  private[mllib] var partitioner: GridPartitioner =
    new GridPartitioner(numRowBlocks, numColBlocks, rdd.partitions.length)

  /** Returns the dimensions of the matrix. */
  private def getDim: (Long, Long) = {
    val (rows, cols) = rdd.map { case ((blockRowIndex, blockColIndex), mat) =>
      (blockRowIndex * rowsPerBlock + mat.numRows, blockColIndex * colsPerBlock + mat.numCols)
    }.reduce((x0, x1) => (math.max(x0._1, x1._1), math.max(x0._2, x1._2)))

    (math.max(rows, nRows), math.max(cols, nCols))
  }

  /** Cache the underlying RDD. */
  def cache(): BlockMatrix = {
    rdd.cache()
    this
  }

  /** Set the storage level for the underlying RDD. */
  def persist(storageLevel: StorageLevel): BlockMatrix = {
    rdd.persist(storageLevel)
    this
  }

  /** Collect the distributed matrix on the driver as a `DenseMatrix`. */
  def toLocalMatrix(): Matrix = {
    require(numRows() < Int.MaxValue, "The number of rows of this matrix should be less than " +
      s"Int.MaxValue. Currently numRows: ${numRows()}")
    require(numCols() < Int.MaxValue, "The number of columns of this matrix should be less than " +
      s"Int.MaxValue. Currently numCols: ${numCols()}")
    require(numRows() * numCols() < Int.MaxValue, "The length of the values array must be " +
      s"less than Int.MaxValue. Currently numRows * numCols: ${numRows() * numCols()}")
    val nRows = numRows().toInt
    val nCols = numCols().toInt
    val mem = nRows * nCols / 125000
    if (mem > 500) logWarning(s"Storing this matrix will require $mem MB of memory!")

    val parts = rdd.collect()
    val values = new Array[Double](nRows * nCols)
    parts.foreach { case ((blockRowIndex, blockColIndex), block) =>
      val rowOffset = blockRowIndex * rowsPerBlock
      val colOffset = blockColIndex * colsPerBlock
      block.foreachActive { (i, j, v) =>
        val indexOffset = (j + colOffset) * nRows + rowOffset + i
        values(indexOffset) = v
      }
    }
    new DenseMatrix(nRows, nCols, values)
  }

  /** Collects data and assembles a local dense breeze matrix (for test only). */
  private[mllib] def toBreeze(): BDM[Double] = {
    val localMat = toLocalMatrix()
    new BDM[Double](localMat.numRows, localMat.numCols, localMat.toArray)
  }
}
