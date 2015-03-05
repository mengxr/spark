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

import org.apache.spark.sql.types.{Metadata, MetadataBuilder}

class AttributeGroup(val name: String, val attributes: Array[Attribute]) {

  attributes.view.zipWithIndex.foreach { case (attr, i) =>
    require(attr.index.get == i)
  }

  private lazy val nameToIndex: Map[String, Int] = {
    if (attributes != null) {
      attributes.view.map(_.name).zipWithIndex.flatMap { case (attrName, index) =>
        attrName.map(_ -> index)
      }.toMap
    } else {
      Map.empty
    }
  }

  /** Test whether this attribute group has a name. */
  def hasName: Boolean = name != null

  /** Number of attributes. */
  def size: Int = if (attributes == null) -1 else attributes.length

  /** Index of an attribute specified by name. */
  def indexOf(attrName: String): Int = nameToIndex(attrName)

  /** Gets an attribute by name. */
  def apply(attrName: String): Attribute = {
    attributes(indexOf(attrName))
  }

  def toMetadata: Metadata = {
    import org.apache.spark.ml.attribute.AttributeGroup._
    val numericMetadata = ArrayBuffer.empty[Metadata]
    val nominalMetadata = ArrayBuffer.empty[Metadata]
    val binaryMetadata = ArrayBuffer.empty[Metadata]
    attributes.foreach {
      case numeric: NumericAttribute =>
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
        attributes(i) = NumericAttribute.defaultAttr.withIndex(i)
        i += 1
      }
    }
    new AttributeGroup(name, attributes)
  }
}
