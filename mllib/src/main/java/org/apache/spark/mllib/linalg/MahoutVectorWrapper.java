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
   * Wraps null (for serialization).
   */
  public MahoutVectorWrapper() {
    // empty
  }

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
