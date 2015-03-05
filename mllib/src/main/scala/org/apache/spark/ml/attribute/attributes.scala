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

import scala.annotation.varargs

import org.apache.spark.sql.types.{Metadata, MetadataBuilder}

sealed abstract class Attribute extends Serializable {

  /** Attribute type. */
  def attrType: AttributeType

  /** Name of the attribute. None if it is not set. */
  def name: Option[String]
  def withName(name: String): Attribute
  def withoutName: Attribute

  /** Index of the attribute. None if it is not set. */
  def index: Option[Int]
  def withIndex(index: Int): Attribute
  def withoutIndex: Attribute

  def isNumeric: Boolean

  def isNominal: Boolean

  /** Convert this attribute to metadata. */
  def toMetadata(withType: Boolean): Metadata

  /** Converts this attribute to Metadata without type info. */
  def toMetadata(): Metadata = toMetadata(withType = true)

  override def toString: String = toMetadata().toString
}

private[attribute] trait AttributeFactory {

  def fromMetadata(metadata: Metadata): Attribute
}

object Attribute extends AttributeFactory {

  override def fromMetadata(metadata: Metadata): Attribute = {
    import org.apache.spark.ml.attribute.AttributeKey._
    val attrType = if (metadata.contains(Type)) {
      metadata.getString(Type)
    } else {
      AttributeType.Numeric.name
    }
    getFactory(attrType).fromMetadata(metadata)
  }

  private def getFactory(attrType: String): AttributeFactory = {
    if (attrType == AttributeType.Numeric.name) {
      NumericAttribute
    } else if (attrType == AttributeType.Nominal.name) {
      NominalAttribute
    } else if (attrType == AttributeType.Binary.name) {
      BinaryAttribute
    } else {
      throw new IllegalArgumentException(s"Cannot recognize type $attrType.")
    }
  }
}

private[attribute] object AttributeKey {
  final val Type: String = "type"
  final val Name: String = "name"
  final val Index: String = "index"
  final val Values: String = "values"
  final val Min: String = "min"
  final val Max: String = "max"
  final val Std: String = "std"
  final val Sparsity: String = "sparsity"
  final val IsOrdinal: String = "isOrdinal"
  final val Cardinality: String = "cardinality"
}

class NumericAttribute private[ml] (
    override val name: Option[String] = None,
    override val index: Option[Int] = None,
    val min: Option[Double] = None,
    val max: Option[Double] = None,
    val std: Option[Double] = None,
    val sparsity: Option[Double] = None)
  extends Attribute {

  override def attrType: AttributeType = AttributeType.Numeric

  override def withName(name: String): NumericAttribute = copy(name = Some(name))
  override def withoutName: NumericAttribute = copy(name = None)

  override def withIndex(index: Int): NumericAttribute = copy(index = Some(index))
  override def withoutIndex: NumericAttribute = copy(index = None)

  def withMin(min: Double): NumericAttribute = copy(min = Some(min))
  def withoutMin: NumericAttribute = copy(min = None)

  def withMax(max: Double): NumericAttribute = copy(max = Some(max))
  def withoutMax: NumericAttribute = copy(max = None)

  def withStd(std: Double): NumericAttribute = copy(std = Some(std))
  def withoutStd: NumericAttribute = copy(std = None)

  def withSparsity(sparsity: Double): NumericAttribute = copy(sparsity = Some(sparsity))
  def withoutSparsity: NumericAttribute = copy(sparsity = None)

  override def isNumeric: Boolean = true

  override def isNominal: Boolean = false

  /** Convert this attribute to metadata. */
  override def toMetadata(withType: Boolean): Metadata = {
    import org.apache.spark.ml.attribute.AttributeKey._
    val bldr = new MetadataBuilder()
    if (withType) bldr.putString(Type, attrType.name)
    name.foreach(bldr.putString(Name, _))
    index.foreach(bldr.putLong(Index, _))
    min.foreach(bldr.putDouble(Min, _))
    max.foreach(bldr.putDouble(Max, _))
    std.foreach(bldr.putDouble(Std, _))
    sparsity.foreach(bldr.putDouble(Sparsity, _))
    bldr.build()
  }

  override def toMetadata(): Metadata = toMetadata(withType = false)

  private def copy(
      name: Option[String] = name,
      index: Option[Int] = index,
      min: Option[Double] = min,
      max: Option[Double] = max,
      std: Option[Double] = std,
      sparsity: Option[Double] = sparsity): NumericAttribute = {
    new NumericAttribute(name, index, min, max, std, sparsity)
  }

  override def equals(other: Any): Boolean = {
    other match {
      case o: NumericAttribute =>
        (name == o.name) &&
          (index == o.index) &&
          (min == o.min) &&
          (max == o.max) &&
          (std == o.std) &&
          (sparsity == o.sparsity)
      case _ =>
        false
    }
  }

  override def hashCode: Int = {
    var sum = 17
    sum = 37 * sum + name.hashCode
    sum = 37 * sum + index.hashCode
    sum = 37 * sum + min.hashCode
    sum = 37 * sum + max.hashCode
    sum = 37 * sum + std.hashCode
    sum = 37 * sum + sparsity.hashCode
    sum
  }
}

object NumericAttribute extends AttributeFactory {

  val defaultAttr: NumericAttribute = new NumericAttribute

  override def fromMetadata(metadata: Metadata): NumericAttribute = {
    import org.apache.spark.ml.attribute.AttributeKey._
    val name = if (metadata.contains(Name)) Some(metadata.getString(Name)) else None
    val index = if (metadata.contains(Index)) Some(metadata.getLong(Index).toInt) else None
    val min = if (metadata.contains(Min)) Some(metadata.getDouble(Min)) else None
    val max = if (metadata.contains(Max)) Some(metadata.getDouble(Max)) else None
    val std = if (metadata.contains(Std)) Some(metadata.getDouble(Std)) else None
    val sparsity = if (metadata.contains(Sparsity)) Some(metadata.getDouble(Sparsity)) else None
    new NumericAttribute(name, index, min, max, std, sparsity)
  }
}

class NominalAttribute private[ml] (
    override val name: Option[String] = None,
    override val index: Option[Int] = None,
    val isOrdinal: Option[Boolean] = None,
    val cardinality: Option[Int] = None,
    val values: Option[Array[String]] = None) extends Attribute {

  override def attrType: AttributeType = AttributeType.Nominal

  override def isNumeric: Boolean = false

  override def isNominal: Boolean = true

  private lazy val valueToIndex: Map[String, Int] = {
    values.map(_.zipWithIndex.toMap).getOrElse(Map.empty)
  }

  /** Index of a specific value. */
  def indexOf(value: String): Int = {
    valueToIndex(value)
  }

  def withValues(values: Array[String]): NominalAttribute = {
    copy(cardinality = None, values = Some(values))
  }

  @varargs
  def withValues(first: String, others: String*): NominalAttribute = {
    copy(cardinality = None, values = Some((first +: others).toArray))
  }

  def withoutValues: NominalAttribute = {
    copy(values = None)
  }

  def withCardinality(cardinality: Int): NominalAttribute = {
    if (values.isDefined) {
      throw new IllegalArgumentException("Cannot copy with cardinality if values are defined.")
    } else {
      copy(cardinality = Some(cardinality))
    }
  }

  private def copy(
      name: Option[String] = name,
      index: Option[Int] = index,
      isOrdinal: Option[Boolean] = isOrdinal,
      cardinality: Option[Int] = cardinality,
      values: Option[Array[String]] = values): NominalAttribute = {
    new NominalAttribute(name, index, isOrdinal, cardinality, values)
  }

  override def withName(name: String): NominalAttribute = copy(name = Some(name))
  override def withoutName: NominalAttribute = copy(name = None)

  override def withIndex(index: Int): NominalAttribute = copy(index = Some(index))
  override def withoutIndex: NominalAttribute = copy(index = None)

  override def toMetadata(withType: Boolean): Metadata = {
    import org.apache.spark.ml.attribute.AttributeKey._
    val bldr = new MetadataBuilder()
    if (withType) bldr.putString(Type, attrType.name)
    name.foreach(bldr.putString(Name, _))
    index.foreach(bldr.putLong(Index, _))
    isOrdinal.foreach(bldr.putBoolean(IsOrdinal, _))
    cardinality.foreach(bldr.putLong(Cardinality, _))
    values.foreach(v => bldr.putStringArray(Values, v))
    bldr.build()
  }

  override def equals(other: Any): Boolean = {
    other match {
      case o: NominalAttribute =>
        (name == o.name) &&
          (index == o.index) &&
          (isOrdinal == o.isOrdinal) &&
          (cardinality == o.cardinality) &&
          (values.map(_.toSeq) == o.values.map(_.toSeq))
      case _ =>
        false
    }
  }

  override def hashCode: Int = {
    var sum = 17
    sum = 37 * sum + name.hashCode
    sum = 37 * sum + index.hashCode
    sum = 37 * sum + isOrdinal.hashCode
    sum = 37 * sum + cardinality.hashCode
    sum = 37 * sum + values.map(_.toSeq).hashCode
    sum
  }
}

object NominalAttribute extends AttributeFactory {

  final val defaultAttr: NominalAttribute = new NominalAttribute

  override def fromMetadata(metadata: Metadata): NominalAttribute = {
    import org.apache.spark.ml.attribute.AttributeKey._
    val name = if (metadata.contains(Name)) Some(metadata.getString(Name)) else None
    val index = if (metadata.contains(Index)) Some(metadata.getLong(Index).toInt) else None
    val isOrdinal = if (metadata.contains(IsOrdinal)) Some(metadata.getBoolean(IsOrdinal)) else None
    val cardinality =
      if (metadata.contains(Cardinality)) Some(metadata.getLong(Cardinality).toInt) else None
    val values =
      if (metadata.contains(Values)) Some(metadata.getStringArray(Values)) else None
    new NominalAttribute(name, index, isOrdinal, cardinality, values)
  }
}

class BinaryAttribute private[ml] (
    override val name: Option[String] = None,
    override val index: Option[Int] = None,
    val values: Option[Array[String]] = None)
  extends Attribute {

  override def attrType: AttributeType = AttributeType.Binary

  override def isNumeric: Boolean = true

  override def isNominal: Boolean = true

  override def withName(name: String): BinaryAttribute = copy(name = Some(name))
  override def withoutName: BinaryAttribute = copy(name = None)

  override def withIndex(index: Int): BinaryAttribute = copy(index = Some(index))
  override def withoutIndex: BinaryAttribute = copy(index = None)

  def withValues(negative: String, positive: String): BinaryAttribute =
    copy(values = Some(Array(negative, positive)))
  def withoutValues: BinaryAttribute = copy(values = None)

  private def copy(
      name: Option[String] = name,
      index: Option[Int] = index,
      values: Option[Array[String]] = values): BinaryAttribute = {
    new BinaryAttribute(name, index, values)
  }

  override def toMetadata(withType: Boolean): Metadata = {
    import org.apache.spark.ml.attribute.AttributeKey._
    val bldr = new MetadataBuilder
    if (withType) bldr.putString(Type, attrType.name)
    name.foreach(bldr.putString(Name, _))
    index.foreach(bldr.putLong(Index, _))
    values.foreach(v => bldr.putStringArray(Values, v))
    bldr.build()
  }

  override def equals(other: Any): Boolean = {
    other match {
      case o: BinaryAttribute =>
        (name == o.name) &&
          (index == o.index) &&
          (values.map(_.toSeq) == o.values.map(_.toSeq))
      case _ =>
        false
    }
  }

  override def hashCode: Int = {
    var sum = 17
    sum = 37 * sum + name.hashCode
    sum = 37 * sum + index.hashCode
    sum = 37 * sum + values.map(_.toSeq).hashCode
    sum
  }
}

object BinaryAttribute extends AttributeFactory {

  final val defaultAttr: BinaryAttribute = new BinaryAttribute

  override def fromMetadata(metadata: Metadata): BinaryAttribute = {
    import org.apache.spark.ml.attribute.AttributeKey._
    val name = if (metadata.contains(Name)) Some(metadata.getString(Name)) else None
    val index = if (metadata.contains(Index)) Some(metadata.getLong(Index).toInt) else None
    val values =
      if (metadata.contains(Values)) Some(metadata.getStringArray(Values)) else None
    new BinaryAttribute(name, index, values)
  }
}
