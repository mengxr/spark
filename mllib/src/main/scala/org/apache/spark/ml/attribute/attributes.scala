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

import org.apache.spark.sql.types.Metadata

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

  override private[ml] def jsonValue: JValue = {
    import AttributeKey._
    (Name -> name) ~
      (Index -> index) ~
      (Min -> min) ~
      (Max -> max) ~
      (Std -> std) ~
      (Sparsity -> sparsity)
  }

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
}

object NumericAttribute extends AttributeFactory {

  val defaultAttr: NumericAttribute = new NumericAttribute

  override def fromJsonValue(json: JValue): NumericAttribute = {
    implicit val formats = DefaultFormats
    import AttributeKey._
    val name = (json \ Name).extractOpt[String]
    val index = (json \ Index).extractOpt[Int]
    val min = (json \ Min).extractOpt[Double]
    val max = (json \ Max).extractOpt[Double]
    val std = (json \ Std).extractOpt[Double]
    val sparsity = (json \ Sparsity).extractOpt[Double]
    new NumericAttribute(name, index, min, max, std, sparsity)
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

  override private[ml] def jsonValue: JValue = {
    import AttributeKey._
    val json = (Type -> attrType.name) ~
      (Name -> name) ~
      (Index -> index) ~
      (IsOrdinal -> isOrdinal) ~
      (Cardinality -> cardinality)
    if (values.isDefined) {
      json ~ (Values -> values.get)
    } else {
      json
    }
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
}

object NominalAttribute extends AttributeFactory {

  final val defaultAttr: NominalAttribute = new NominalAttribute

  override def fromJsonValue(json: JValue): NominalAttribute = {
    implicit val formats = DefaultFormats
    import AttributeKey._
    val name = (json \ Name).extractOpt[String]
    val index = (json \ Index).extractOpt[Int]
    val isOrdinal = (json \ IsOrdinal).extractOpt[Boolean]
    val cardinality = (json \ Cardinality).extractOpt[Int]
    val values = (json \ Values).toOption.map(_.extract[Seq[String]])
    new NominalAttribute(name, index, isOrdinal, cardinality, values)
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

  override private[ml] def jsonValue: JValue = {
    import AttributeKey._
    val json = (Type -> attrType.name) ~
      (Name -> name) ~
      (Index -> index)
    if (values.isDefined) {
      json ~ (Values -> values.get)
    } else {
      json
    }
  }

  override def withName(name: String): Attribute = copy(name = Some(name))
  override def withoutName: Attribute = copy(name = None)

  override def withIndex(index: Int): Attribute = copy(index= Some(index))
  override def withoutIndex: Attribute = copy(index = None)
}

object BinaryAttribute extends AttributeFactory {

  final val defaultAttr: BinaryAttribute = new BinaryAttribute

  override private[ml] def fromJsonValue(json: JValue): Attribute = {
    implicit val formats = DefaultFormats
    import AttributeKey._
    val name = (json \ Name).extractOpt[String]
    val index = (json \ Index).extractOpt[Int]
    val values = (json \ Values).toOption.map(_.extract[Seq[String]])
    BinaryAttribute(name, index, values)
  }
}
