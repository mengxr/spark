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

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write

import org.apache.spark.sql.types.Metadata

sealed trait Attribute extends Serializable {

  /** Attribute type: {0: numeric, 1: nominal, 2: binary}. */
  def attrType: AttributeType

  /** Name of the attribute. None if it is not set. */
  def name: Option[String] = None

  def withName(name: String): Attribute = copyWithName(Some(name))
  def withoutName: Attribute = copyWithName(None)
  private[ml] def copyWithName(name: Option[String]): Attribute

  /** Index of the attribute. None if it is not set. */
  def index: Option[Int] = None

  def withIndex(index: Int): Attribute = copyWithIndex(Some(index))
  def withoutIndex: Attribute = copyWithIndex(None)
  private[ml] def copyWithIndex(index: Option[Int]): Attribute

  def isNumeric: Boolean

  def isNominal: Boolean

  /** Get the JSON representation of this attribute. */
  def toJson: String = compact(render(jsonValue))

  private[ml] def jsonValue: JValue

  /** Convert this attribute to metadata. */
  def toMetadata: Metadata = null
}

private[attribute] trait AttributeFactory {

  private[ml] def fromJsonValue(json: JValue): Attribute

  def fromJson(json: String): Attribute = fromJsonValue(parse(json))

  def fromMetadata(metadata: Metadata): Attribute = null
}

object Attribute extends AttributeFactory {

  private[ml] override def fromJsonValue(json: JValue): Attribute = {
    implicit val formats = DefaultFormats
    val attrType = (json \ AttributeKey.Type).extractOrElse(AttributeType.Numeric.name)
    val factory: AttributeFactory =
      if (attrType == AttributeType.Numeric.name) {
        NumericAttribute
      } else if (attrType == AttributeType.Nominal.name) {
        NominalAttribute
      } else if (attrType == AttributeType.Binary.name) {
        BinaryAttribute
      } else {
        throw new IllegalArgumentException(s"Cannot recognize type $attrType.")
      }
    factory.fromJsonValue(json)
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

  override private[ml] def jsonValue: JValue = {
    import AttributeKey._
    (Name -> name) ~
      (Index -> index) ~
      (Min -> min) ~
      (Max -> max) ~
      (Std -> std) ~
      (Sparsity -> sparsity)
  }

  override def copyWithName(name: Option[String]) = {
    copyWith(name = name)
  }

  override def copyWithIndex(index: Option[Int]) = {
    copyWith(index = index)
  }

  private[ml] def copyWith(
    name: Option[String] = name,
    index: Option[Int] = index,
    min: Option[Double] = min,
    max: Option[Double] = max,
    std: Option[Double] = std,
    support: Option[Double] = sparsity) = {
    new NumericAttribute(name, index, min, max, std, support)
  }

  override def isNumeric: Boolean = true

  override def isNominal: Boolean = false
}

object NumericAttribute extends AttributeFactory {

  val defaultAttr: NumericAttribute = new NumericAttribute

  override def fromJsonValue(json: JValue): NumericAttribute = {
    implicit val formats = DefaultFormats
    json.extract[NumericAttribute]
  }
}

case class NominalAttribute private[ml] (
    override val name: Option[String] = None,
    override val index: Option[Int] = None,
    isOrdinal: Option[Boolean] = None,
    cardinality: Option[Int] = None,
    values: Option[Array[String]] = None) extends Attribute {

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

  override private[ml] def jsonValue: JValue = {
    implicit val formats = DefaultFormats
    write(this)
  }

  override private[ml] def copyWithName(name: Option[String]): Attribute = copyWith(name = name)

  override private[ml] def copyWithIndex(index: Option[Int]): Attribute = copyWith(index = index)

  private[ml] def copyWith(
      name: Option[String] = None,
      index: Option[Int] = None,
      cardinality: Option[Int] = None,
      values: Option[Array[String]] = None,
      isOrdinal: Option[Boolean] = None): NominalAttribute = {
    new NominalAttribute(name, index, isOrdinal, cardinality, values)
  }
}

object NominalAttribute extends AttributeFactory {

  final val defaultAttr: NominalAttribute = new NominalAttribute

  override def fromJsonValue(json: JValue): NominalAttribute = {
    implicit val formats = DefaultFormats
    json.extract[NominalAttribute]
  }
}

case class BinaryAttribute private[ml] (
    override val name: Option[String] = None,
    override val index: Option[Int] = None,
    values: Option[Array[String]] = None)
  extends Attribute {

  override def attrType: AttributeType = AttributeType.Binary

  override def isNumeric: Boolean = true

  override def isNominal: Boolean = true

  private[ml] override def copyWithName(name: Option[String]): Attribute = copyWith(name = name)

  private[ml] override def copyWithIndex(index: Option[Int]): Attribute = copyWith(index = index)

  private def copyWith(
    name: Option[String] = name,
    index: Option[Int] = index,
    values: Option[Array[String]] = values): Attribute = {
    new BinaryAttribute(name, index, values)
  }

  override private[ml] def jsonValue: JValue = {
    implicit val formats = DefaultFormats
    write(this)
  }
}

object BinaryAttribute extends AttributeFactory {

  final val defaultAttr: BinaryAttribute = new BinaryAttribute

  override private[ml] def fromJsonValue(json: JValue): Attribute = {
    implicit val formats = DefaultFormats
    json.extract[BinaryAttribute]
  }
}
