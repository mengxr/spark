package org.apache.spark.mllib.linalg;

import org.apache.commons.lang3.SerializationUtils;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link MahoutVectorWrapper}.
 */
public class MahoutVectorWrapperSuite {

  @Test
  public void testDenseVectorSerialization() {
    DenseVector denseVector = new DenseVector(new double[]{1.0, 0.0, 3.0});
    MahoutVectorWrapper wrapper1 = new MahoutVectorWrapper(denseVector);
    byte[] bytes = SerializationUtils.serialize(wrapper1);
    MahoutVectorWrapper wrapper2 = (MahoutVectorWrapper) SerializationUtils.deserialize(bytes);
    Assert.assertEquals(wrapper2.unwrap(), denseVector);
  }

  @Test
  public void testSequentialAccessSparseVectorSerialization() {
    SequentialAccessSparseVector sparseVector = new SequentialAccessSparseVector(3, 2);
    sparseVector.set(0, 1.0);
    sparseVector.set(2, 3.0);
    MahoutVectorWrapper wrapper1 =
        new MahoutVectorWrapper(sparseVector);
    byte[] bytes = SerializationUtils.serialize(wrapper1);
    MahoutVectorWrapper wrapper2 = (MahoutVectorWrapper) SerializationUtils.deserialize(bytes);
    Assert.assertEquals(wrapper2.unwrap(), sparseVector);
  }

  @Test
  public void testClone() {
    DenseVector denseVector = new DenseVector(new double[]{1.0, 0.0, 3.0});
    MahoutVectorWrapper wrapper1 = new MahoutVectorWrapper(denseVector);
    MahoutVectorWrapper wrapper2 = wrapper1.clone();
    Assert.assertFalse(wrapper2.unwrap() == wrapper1.unwrap());
    Assert.assertEquals(wrapper2.unwrap(), wrapper1.unwrap());
  }
}
