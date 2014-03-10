package org.apache.spark.mllib.linalg

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV}
import org.scalatest.FunSuite

/**
 * Test Breeze conversions.
 */
class BreezeVectorConversionSuite extends FunSuite {

  val arr = Array(0.1, 0.2, 0.3, 0.4)
  val n = 20
  val indices = Array(0, 3, 5, 10, 13)
  val values = Array(0.1, 0.5, 0.3, -0.8, -1.0)

  test("dense to breeze") {
    val vec = Vectors.dense(arr)
    assert(vec.toBreeze === new BDV[Double](arr))
  }

  test("sparse to breeze") {
    val vec = Vectors.sparse(n, indices, values)
    assert(vec.toBreeze === new BSV[Double](indices, values, n))
  }

  test("dense breeze to vector") {
    val breeze = new BDV[Double](arr)
    val vec = Vectors.fromBreeze(breeze).asInstanceOf[DenseVector]
    assert(vec.size === arr.length)
    assert(vec.values.eq(arr), "should not copy data")
  }

  test("sparse breeze to vector") {
    val breeze = new BSV[Double](indices, values, n)
    val vec = Vectors.fromBreeze(breeze).asInstanceOf[SparseVector]
    assert(vec.size === n)
    assert(vec.indices.eq(indices), "should not copy data")
    assert(vec.values.eq(values), "should not copy data")
  }
}
