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

import org.apache.spark.sql.types.{MetadataBuilder, Metadata}

sealed abstract class Attribute extends Serializable {

  /** Attribute type: {0: numeric, 1: nominal, 2: binary}. */
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

  override def toString: String = toMetadata.toString
}

private[attribute] trait AttributeFactory {

  def fromMetadata(metadata: Metadata): Attribute
}

object Attribute extends AttributeFactory {

  override def fromMetadata(metadata: Metadata): Attribute = {
    import AttributeKey._
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

case class NumericAttribute private[ml] (
    override val name: Option[String] = None,
    override val index: Option[Int] = None,
    min: Option[Double] = None,
    max: Option[Double] = None,
    std: Option[Double] = None,
    sparsity: Option[Double] = None)
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
    import AttributeKey._
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
}

object NumericAttribute extends AttributeFactory {

  val defaultAttr: NumericAttribute = new NumericAttribute

  override def fromMetadata(metadata: Metadata): NumericAttribute = {
    import AttributeKey._
    val name = if (metadata.contains(Name)) Some(metadata.getString(Name)) else None
    val index = if (metadata.contains(Index)) Some(metadata.getLong(Index).toInt) else None
    val min = if (metadata.contains(Min)) Some(metadata.getDouble(Min)) else None
    val max = if (metadata.contains(Max)) Some(metadata.getDouble(Max)) else None
    val std = if (metadata.contains(Std)) Some(metadata.getDouble(Std)) else None
    val sparsity = if (metadata.contains(Sparsity)) Some(metadata.getDouble(Sparsity)) else None
    NumericAttribute(name, index, min, max, std, sparsity)
  }
}

case class NominalAttribute private[ml] (
    override val name: Option[String] = None,
    override val index: Option[Int] = None,
    isOrdinal: Option[Boolean] = None,
    cardinality: Option[Int] = None,
    values: Option[Seq[String]] = None) extends Attribute {

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

  def withValues(values: Seq[String]): NominalAttribute = {
    copy(cardinality = None, values = Some(values))
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

  override def withName(name: String): NominalAttribute = copy(name = Some(name))
  override def withoutName: NominalAttribute = copy(name = None)

  override def withIndex(index: Int): NominalAttribute = copy(index = Some(index))
  override def withoutIndex: NominalAttribute = copy(index = None)

  override def toMetadata(withType: Boolean): Metadata = {
    import AttributeKey._
    val bldr = new MetadataBuilder()
    if (withType) bldr.putString(Type, attrType.name)
    name.foreach(bldr.putString(Name, _))
    index.foreach(bldr.putLong(Index, _))
    isOrdinal.foreach(bldr.putBoolean(IsOrdinal, _))
    cardinality.foreach(bldr.putLong(Cardinality, _))
    values.foreach(v => bldr.putStringArray(Values, v.toArray))
    bldr.build()
  }
}

object NominalAttribute extends AttributeFactory {

  final val defaultAttr: NominalAttribute = new NominalAttribute

  override def fromMetadata(metadata: Metadata): NominalAttribute = {
    import AttributeKey._
    val name = if (metadata.contains(Name)) Some(metadata.getString(Name)) else None
    val index = if (metadata.contains(Index)) Some(metadata.getLong(Index).toInt) else None
    val isOrdinal = if (metadata.contains(IsOrdinal)) Some(metadata.getBoolean(IsOrdinal)) else None
    val cardinality =
      if (metadata.contains(Cardinality)) Some(metadata.getLong(Cardinality).toInt) else None
    val values =
      if (metadata.contains(Values)) Some(metadata.getStringArray(Values).toSeq) else None
    NominalAttribute(name, index, isOrdinal, cardinality, values)
  }
}

case class BinaryAttribute private[ml] (
    override val name: Option[String] = None,
    override val index: Option[Int] = None,
    values: Option[Seq[String]] = None)
  extends Attribute {

  override def attrType: AttributeType = AttributeType.Binary

  override def isNumeric: Boolean = true

  override def isNominal: Boolean = true

  override def withName(name: String): BinaryAttribute = copy(name = Some(name))
  override def withoutName: BinaryAttribute = copy(name = None)

  override def withIndex(index: Int): BinaryAttribute = copy(index = Some(index))
  override def withoutIndex: BinaryAttribute = copy(index = None)

  def withValues(values: Seq[String]): BinaryAttribute = copy(values = Some(values))
  def withoutValues: BinaryAttribute = copy(values = None)

  override def toMetadata(withType: Boolean): Metadata = {
    import AttributeKey._
    val bldr = new MetadataBuilder
    if (withType) bldr.putString(Type, attrType.name)
    name.foreach(bldr.putString(Name, _))
    index.foreach(bldr.putLong(Index, _))
    values.foreach(v => bldr.putStringArray(Values, v.toArray))
    bldr.build()
  }
}

object BinaryAttribute extends AttributeFactory {

  final val defaultAttr: BinaryAttribute = new BinaryAttribute

  override def fromMetadata(metadata: Metadata): BinaryAttribute = {
    import AttributeKey._
    val name = if (metadata.contains(Name)) Some(metadata.getString(Name)) else None
    val index = if (metadata.contains(Index)) Some(metadata.getLong(Index).toInt) else None
    val values =
      if (metadata.contains(Values)) Some(metadata.getStringArray(Values).toSeq) else None
    BinaryAttribute(name, index, values)
  }
}
