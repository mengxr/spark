package org.apache.spark.mllib.linalg;

import org.apache.commons.lang3.SerializationUtils;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.OrderedIntDoubleMapping;
import org.apache.mahout.math.SequentialAccessSparseVector;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link org.apache.spark.mllib.linalg.MahoutVectorHelper}.
 */
public class MahoutVectorHelperSuite {

  @Test
  public void testGetDenseVectorValues() {
    double[] values = new double[]{1.0, 0.0, 3.0};
    DenseVector denseVector = new DenseVector(values, true);
    Assert.assertTrue(MahoutVectorHelper.getDenseVectorValues(denseVector) == values);
  }

  @Test
  public void testGetSequentialAccessSparseVectorValues() {
    SequentialAccessSparseVector sparseVector = new SequentialAccessSparseVector(3, 2);
    sparseVector.set(0, 1.0);
    sparseVector.set(2, 3.0);
    OrderedIntDoubleMapping mapping =
        MahoutVectorHelper.getSequentialAccessSparseVectorValues(sparseVector);
    int[] indices = mapping.getIndices();
    double[] values = mapping.getValues();
    Assert.assertArrayEquals(indices, new int[]{0, 2});
    Assert.assertArrayEquals(values, new double[]{1.0, 3.0}, 0.0);
  }

  @Test
  public void testNewSequentialAccessSparseVector() {
    SequentialAccessSparseVector sparseVector1 = new SequentialAccessSparseVector(3, 2);
    sparseVector1.set(0, 1.0);
    sparseVector1.set(2, 3.0);
    SequentialAccessSparseVector sparseVector2 =
        MahoutVectorHelper
            .newSequentialAccessSparseVector(3, new int[]{0, 2}, new double[]{1.0, 3.0});
    Assert.assertEquals(sparseVector2, sparseVector1);
  }
}
