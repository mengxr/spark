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

package org.apache.spark.serializer

import java.io._
import java.nio.ByteBuffer

import org.apache.spark.SparkConf
import org.apache.spark.util.ByteBufferInputStream

private[spark] class JavaSerializationStream(out: OutputStream, conf: SparkConf)
  extends SerializationStream {
  val objOut = new ObjectOutputStream(out)
  var counter = 0
  val counterReset = conf.getInt("spark.serializer.objectStreamReset", 10000)

  /**
   * Calling reset to avoid memory leak:
   * http://stackoverflow.com/questions/1281549/memory-leak-traps-in-the-java-standard-api
   * But only call it every 10,000th time to avoid bloated serialization streams (when
   * the stream 'resets' object class descriptions have to be re-written)
   */
  def writeObject[T](t: T): SerializationStream = {
    objOut.writeObject(t)
    if (counterReset > 0 && counter >= counterReset) {
      objOut.reset()
      counter = 0
    } else {
      counter += 1
    }
    this
  }
  def flush() { objOut.flush() }
  def close() { objOut.close() }
}

private[spark] class JavaDeserializationStream(in: InputStream, loader: ClassLoader)
extends DeserializationStream {
  val objIn = new ObjectInputStream(in) {
    override def resolveClass(desc: ObjectStreamClass) =
      Class.forName(desc.getName, false, loader)
  }

  def readObject[T](): T = objIn.readObject().asInstanceOf[T]
  def close() { objIn.close() }
}

private[spark] class JavaSerializerInstance(conf: SparkConf) extends SerializerInstance {
  def serialize[T](t: T): ByteBuffer = {
    val bos = new ByteArrayOutputStream()
    val out = serializeStream(bos)
    out.writeObject(t)
    out.close()
    ByteBuffer.wrap(bos.toByteArray)
  }

  def deserialize[T](bytes: ByteBuffer): T = {
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis)
    in.readObject().asInstanceOf[T]
  }

  def deserialize[T](bytes: ByteBuffer, loader: ClassLoader): T = {
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis, loader)
    in.readObject().asInstanceOf[T]
  }

  def serializeStream(s: OutputStream): SerializationStream = {
    new JavaSerializationStream(s, conf)
  }

  def deserializeStream(s: InputStream): DeserializationStream = {
    new JavaDeserializationStream(s, Thread.currentThread.getContextClassLoader)
  }

  def deserializeStream(s: InputStream, loader: ClassLoader): DeserializationStream = {
    new JavaDeserializationStream(s, loader)
  }
}

/**
 * A Spark serializer that uses Java's built-in serialization.
 */
class JavaSerializer(conf: SparkConf) extends Serializer {
  def newInstance(): SerializerInstance = new JavaSerializerInstance(conf)
}
