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

package org.apache.spark.ml.attribute

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.sql.types.{Metadata, MetadataBuilder, StructField}

/**
 * Attributes that describe a vector ML column.
 *
 * @param name name of the attribute group (the ML column name)
 * @param attrs attributes
 */
class AttributeGroup(val name: String, attrs: Array[Attribute]) extends Serializable {

  val attributes: Array[Attribute] = attrs.view.zipWithIndex.map { case (attr, i) =>
    attr.withIndex(i)
  }.toArray

  private lazy val nameToIndex: Map[String, Int] = {
    attributes.view.flatMap { attr =>
      attr.name.map(_ -> attr.index.get)
    }.toMap
  }

  /** Number of attributes. */
  def size: Int = attributes.length

  /** Test whether this attribute group contains a specific attribute. */
  def hasAttr(attrName: String): Boolean = nameToIndex.contains(attrName)

  /** Index of an attribute specified by name. */
  def indexOf(attrName: String): Int = nameToIndex(attrName)

  /** Gets an attribute by name. */
  def apply(attrName: String): Attribute = {
    attributes(indexOf(attrName))
  }

  def apply(attrIndex: Int): Attribute = attributes(attrIndex)

  def toMetadata: Metadata = {
    import org.apache.spark.ml.attribute.AttributeGroup._
    val numericMetadata = ArrayBuffer.empty[Metadata]
    val nominalMetadata = ArrayBuffer.empty[Metadata]
    val binaryMetadata = ArrayBuffer.empty[Metadata]
    attributes.foreach {
      case numeric: NumericAttribute =>
        // Skip default numeric attributes.
        if (numeric.withoutIndex != NumericAttribute.defaultAttr) {
          numericMetadata += numeric.toMetadata(withType = false)
        }
      case nominal: NominalAttribute =>
        nominalMetadata += nominal.toMetadata(withType = false)
      case binary: BinaryAttribute =>
        binaryMetadata += binary.toMetadata(withType = false)
    }
    val attrBldr = new MetadataBuilder
    if (numericMetadata.nonEmpty) {
      attrBldr.putMetadataArray(AttributeType.Numeric.name, numericMetadata.toArray)
    }
    if (nominalMetadata.nonEmpty) {
      attrBldr.putMetadataArray(AttributeType.Nominal.name, nominalMetadata.toArray)
    }
    if (binaryMetadata.nonEmpty) {
      attrBldr.putMetadataArray(AttributeType.Binary.name, binaryMetadata.toArray)
    }
    new MetadataBuilder()
      .putLong(SIZE, attributes.length)
      .putMetadata(ATTRIBUTES, attrBldr.build())
      .build()
  }

  def toStructField(existingMetadata: Metadata): StructField = {
    val newMetadata = new MetadataBuilder()
      .withMetadata(existingMetadata)
      .putMetadata(AttributeKeys.ML_ATTR, toMetadata)
      .build()
    StructField(name, new VectorUDT, nullable = false, newMetadata)
  }

  def toStructField(): StructField = toStructField(Metadata.empty)

  override def equals(other: Any): Boolean = {
    other match {
      case o: AttributeGroup =>
        (name == o.name) && (attributes.toSeq == o.attributes.toSeq)
      case _ =>
        false
    }
  }

  override def hashCode: Int = {
    var sum = 17
    sum = 37 * sum + name.hashCode
    sum = 37 * sum + attributes.toSeq.hashCode
    sum
  }
}

object AttributeGroup {

  private[attribute] final val SIZE: String = "size"
  private[attribute] final val ATTRIBUTES: String = "attributes"

  def fromMetadata(metadata: Metadata, name: String): AttributeGroup = {
    import org.apache.spark.ml.attribute.AttributeType._
    val size = metadata.getLong(SIZE).toInt
    val attributes = new Array[Attribute](size)
    val attrMetadata = metadata.getMetadata(ATTRIBUTES)
    if (attrMetadata.contains(Numeric.name)) {
      attrMetadata.getMetadataArray(Numeric.name)
        .map(NumericAttribute.fromMetadata)
        .foreach { attr =>
          attributes(attr.index.get) = attr
        }
    }
    if (attrMetadata.contains(Nominal.name)) {
      attrMetadata.getMetadataArray(Nominal.name)
        .map(NominalAttribute.fromMetadata)
        .foreach { attr =>
          attributes(attr.index.get) = attr
        }
    }
    if (attrMetadata.contains(Binary.name)) {
      attrMetadata.getMetadataArray(Binary.name)
        .map(BinaryAttribute.fromMetadata)
        .foreach { attr =>
          attributes(attr.index.get) = attr
        }
    }
    var i = 0
    while (i < size) {
      if (attributes(i) == null) {
        attributes(i) = NumericAttribute.defaultAttr
      }
      i += 1
    }
    new AttributeGroup(name, attributes)
  }

  def fromStructField(field: StructField): AttributeGroup = {
    require(field.dataType == new VectorUDT)
    fromMetadata(field.metadata.getMetadata(AttributeKeys.ML_ATTR), field.name)
  }
}
