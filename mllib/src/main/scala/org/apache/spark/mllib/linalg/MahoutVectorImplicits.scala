package org.apache.spark.mllib.linalg

import org.apache.mahout.math.Vector

/**
 * Implicit conversions between [[org.apache.spark.mllib.linalg.MahoutVectorWrapper]]
 * and [[org.apache.mahout.math.Vector]].
 */
object MahoutVectorImplicits {

  implicit def wrapperToVector(wrapper: MahoutVectorWrapper): Vector = wrapper.unwrap()

  implicit def vectorToWrapper(vector: Vector) = new MahoutVectorWrapper(vector)
}
