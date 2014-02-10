package org.apache.spark.mllib.linalg

import org.apache.mahout.math.{
  Vector => MahoutVector,
  DenseVector => MahoutDenseVector,
  SequentialAccessSparseVector => MahoutSequantialAccessSparseVector
}

/**
 * Represents a numeric vector, whose index type is Int and value type is Double.
 * Not using the name "Vector" because scala imports its [[scala.Vector]] by default.
 */
trait Vec extends Serializable {

  def size: Int

  /**
   * Converts the instance to a Mahout vector wrapper.
   */
  private[mllib] def toMahout: MahoutVectorWrapper
}

/**
 * Represents a vector with random access to its elements.
 *
 */
trait RandomAccessVec extends Vec {
  // empty
}

/**
 * Factory methods for [[org.apache.spark.mllib.linalg.Vec]].
 */
object Vec {

  /** Creates a dense vector. */
  def newDenseVec(values: Array[Double]): Vec = new DenseVec(values)

  /**
   * Creates a sparse vector providing its index array and value array.
   *
   * @param size vector size.
   * @param indices index array, must be strictly increasing.
   * @param values value array, must have the same length as indices.
   */
  def newSparseVec(size: Int, indices: Array[Int], values: Array[Double]): Vec =
    new SparseVec(size, indices, values)

  /**
   * Create a sparse vector using unordered (index, value) pairs.
   *
   * @param size vector size.
   * @param elements vector elements in (index, value) pairs.
   */
  def newSparseVec(size: Int, elements: Iterable[(Int, Double)]): Vec = {

    require(size > 0)

    val (indices, values) = elements.toArray.sortBy(_._1).unzip
    var prev = -1
    indices.foreach { i =>
      require(prev < i, "Found duplicate indices: " + i)
      prev = i
    }
    require(prev < size)

    new SparseVec(size, indices.toArray, values.toArray)
  }

  private[mllib] def fromMahout(wrapper: MahoutVectorWrapper): Vec = {
    wrapper.unwrap() match {
      case v: MahoutDenseVector => {
        new DenseVec(MahoutVectorHelper.getDenseVectorValues(v))
      }
      case v: MahoutSequantialAccessSparseVector => {
        val mapping = MahoutVectorHelper.getSequentialAccessSparseVectorValues(v)
        val indices = mapping.getIndices
        val values = mapping.getValues
        require(mapping.getNumMappings == indices.length)
        new SparseVec(v.size(), indices, values)
      }
      case v: MahoutVector => {
        sys.error("Unsupported Mahout vector type: " + v.getClass)
      }
    }
  }
}

/**
 * A dense vector represented by a value array.
 *
 * @param values
 */
class DenseVec(var values: Array[Double]) extends RandomAccessVec {

  override def size: Int = values.length

  override def toString = values.mkString("[", ",", "]")


  private[mllib] override def toMahout =
    new MahoutVectorWrapper(new MahoutDenseVector(values, true))
}

/**
 * A sparse vector represented by an index array and an value array.
 *
 * @param n size of the vector.
 * @param indices index array, assume to be strictly increasing.
 * @param values value array, must have the same length as the index array.
 */
class SparseVec(var n: Int, var indices: Array[Int], var values: Array[Double]) extends Vec {

  override def size: Int = n

  override def toString = {
    "(" + n + "," + indices.zip(values).mkString("[", "," ,"]") + ")"
  }

  private[mllib] override def toMahout = new MahoutVectorWrapper(
      MahoutVectorHelper.newSequentialAccessSparseVector(n, indices, values)
  )
}
