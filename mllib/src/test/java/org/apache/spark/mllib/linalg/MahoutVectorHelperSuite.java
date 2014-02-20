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

package org.apache.spark.mllib.linalg;

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
