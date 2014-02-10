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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.OrderedIntDoubleMapping;
import org.apache.mahout.math.SequentialAccessSparseVector;

/**
 * Helper class for Mahout vectors.
 * <p/>
 * Java reflection is used to extract fields from Mahout vectors without copying the data.
 */
public class MahoutVectorHelper {

  private static Constructor<SequentialAccessSparseVector> vectorConstructor;
  private static Constructor<OrderedIntDoubleMapping> mappingConstructor;
  private static Field denseVectorValuesField;
  private static Field sequentialAccessSparseVectorValuesField;

  static {
    try {
      vectorConstructor = SequentialAccessSparseVector.class
          .getDeclaredConstructor(int.class, OrderedIntDoubleMapping.class);
      vectorConstructor.setAccessible(true);
      mappingConstructor = OrderedIntDoubleMapping.class
          .getDeclaredConstructor(int[].class, double[].class, int.class);
      mappingConstructor.setAccessible(true);
      denseVectorValuesField = DenseVector.class.getDeclaredField("values");
      denseVectorValuesField.setAccessible(true);
      sequentialAccessSparseVectorValuesField = SequentialAccessSparseVector.class
          .getDeclaredField("values");
      sequentialAccessSparseVectorValuesField.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates a new {@link org.apache.mahout.math.SequentialAccessSparseVector} instance.
   *
   * @param size vector size
   * @param indices index array, must be strictly increasing
   * @param values value array, must have the same length as the index array
   */
  public static SequentialAccessSparseVector newSequentialAccessSparseVector(
      int size,
      int[] indices,
      double[] values
  ) {
    try {
      OrderedIntDoubleMapping mapping =
          mappingConstructor.newInstance(indices, values, indices.length);
      return vectorConstructor.newInstance(size, mapping);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets the values field (double[]) from a {@link org.apache.mahout.math.DenseVector} instance.
   */
  public static double[] getDenseVectorValues(DenseVector denseVector) {
    try {
      return (double[]) denseVectorValuesField.get(denseVector);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets the values field ({@link org.apache.mahout.math.OrderedIntDoubleMapping}) from a
   * {@link org.apache.mahout.math.SequentialAccessSparseVector} instance.
   */
  public static OrderedIntDoubleMapping getSequentialAccessSparseVectorValues(
      SequentialAccessSparseVector sparseVector
  ) {
    try {
      return (OrderedIntDoubleMapping) sequentialAccessSparseVectorValuesField.get(sparseVector);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
