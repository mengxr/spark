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

import java.io.*;

import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * A serializable wrapper class for Mahout vectors.
 */
public class MahoutVectorWrapper implements Serializable, Cloneable {

  private Vector vector = null;

  /**
   * Wraps a vector.
   */
  public MahoutVectorWrapper(Vector v) {
    vector = v;
  }

  /**
   * Returns the wrapped vector.
   */
  public Vector unwrap() {
    return vector;
  }

  /**
   * Sets the wrapped vector.
   */
  public void wrap(Vector v) {
    vector = v;
  }

  @Override
  public MahoutVectorWrapper clone() {
    return new MahoutVectorWrapper(vector.clone());
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    VectorWritable.writeVector(out, vector);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    vector = VectorWritable.readVector(in);
  }

  private void readObjectNoData() throws ObjectStreamException {
    throw new InvalidObjectException("Stream data required.");
  }
}
