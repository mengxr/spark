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

import org.scalatest.FunSuite

import org.apache.spark.sql.types.Metadata

class AttributeSuite extends FunSuite {

  test("default numeric attribute") {
    val attr: NumericAttribute = NumericAttribute.defaultAttr
    val metadata = Metadata.fromJson("{}")
    val metadataWithType = Metadata.fromJson("""{"type":"numeric"}""")
    assert(attr.attrType === AttributeType.Numeric)
    assert(attr.isNumeric)
    assert(!attr.isNominal)
    assert(attr.name.isEmpty)
    assert(attr.index.isEmpty)
    assert(attr.min.isEmpty)
    assert(attr.max.isEmpty)
    assert(attr.std.isEmpty)
    assert(attr.sparsity.isEmpty)
    assert(attr.toMetadata() === metadata)
    assert(attr.toMetadata(withType = false) === metadata)
    assert(attr.toMetadata(withType = true) === metadataWithType)
    assert(attr === Attribute.fromMetadata(metadata))
    assert(attr === Attribute.fromMetadata(metadataWithType))
  }

  test("customized numeric attribute") {
    val name = "age"
    val index = 0
    val metadata = Metadata.fromJson("""{"name":"age","idx":0}""")
    val metadataWithType = Metadata.fromJson("""{"type":"numeric","name":"age","idx":0}""")
    val attr: NumericAttribute = NumericAttribute.defaultAttr
      .withName(name)
      .withIndex(index)
    assert(attr.attrType == AttributeType.Numeric)
    assert(attr.isNumeric)
    assert(!attr.isNominal)
    assert(attr.name === Some(name))
    assert(attr.index === Some(index))
    assert(attr.toMetadata() === metadata)
    assert(attr.toMetadata(withType = false) === metadata)
    assert(attr.toMetadata(withType = true) === metadataWithType)
    assert(attr === Attribute.fromMetadata(metadata))
    assert(attr === Attribute.fromMetadata(metadataWithType))
    val attr2 =
      attr.withoutName.withoutIndex.withMin(0.0).withMax(1.0).withStd(0.5).withSparsity(0.3)
    assert(attr2.name.isEmpty)
    assert(attr2.index.isEmpty)
    assert(attr2.min === Some(0.0))
    assert(attr2.max === Some(1.0))
    assert(attr2.std === Some(0.5))
    assert(attr2.sparsity === Some(0.3))
    assert(attr2 === Attribute.fromMetadata(attr2.toMetadata()))
  }

  test("default nominal attribute") {
    val attr: NominalAttribute = NominalAttribute.defaultAttr
    val metadata = Metadata.fromJson("""{"type":"nominal"}""")
    val metadataWithoutType = Metadata.fromJson("{}")
    assert(attr.attrType === AttributeType.Nominal)
    assert(!attr.isNumeric)
    assert(attr.isNominal)
    assert(attr.name.isEmpty)
    assert(attr.index.isEmpty)
    assert(attr.values.isEmpty)
    assert(attr.cardinality.isEmpty)
    assert(attr.isOrdinal.isEmpty)
    assert(attr.toMetadata() === metadata)
    assert(attr.toMetadata(withType = true) === metadata)
    assert(attr.toMetadata(withType = false) === metadataWithoutType)
    assert(attr === Attribute.fromMetadata(metadata))
    assert(attr === NominalAttribute.fromMetadata(metadataWithoutType))
  }

  test("customized nominal attribute") {
    val name = "size"
    val index = 1
    val values = Array("small", "medium", "large")
    val metadata = Metadata.fromJson(
      """{"type":"nominal","name":"size","idx":1,"vals":["small","medium","large"]}""")
    val metadataWithoutType = Metadata.fromJson(
      """{"name":"size","idx":1,"vals":["small","medium","large"]}""")
    val attr: NominalAttribute = NominalAttribute.defaultAttr
      .withName(name)
      .withIndex(index)
      .withValues(values)
    assert(attr.attrType === AttributeType.Nominal)
    assert(!attr.isNumeric)
    assert(attr.isNominal)
    assert(attr.name === Some(name))
    assert(attr.index === Some(index))
    assert(attr.values === Some(values))
    assert(attr.indexOf("medium") === 1)
    assert(attr.toMetadata() === metadata)
    assert(attr.toMetadata(withType = true) === metadata)
    assert(attr.toMetadata(withType = false) === metadataWithoutType)
    assert(attr === Attribute.fromMetadata(metadata))
    assert(attr === NominalAttribute.fromMetadata(metadataWithoutType))
    val attr2 = attr.withoutName.withoutIndex.withValues(attr.values.get :+ "x-large")
    assert(attr2.name.isEmpty)
    assert(attr2.index.isEmpty)
    assert(attr2.values.get === Array("small", "medium", "large", "x-large"))
    assert(attr2.indexOf("x-large") === 3)
    assert(attr2 === Attribute.fromMetadata(attr2.toMetadata()))
    assert(attr2 === NominalAttribute.fromMetadata(attr2.toMetadata(withType = false)))
  }

  test("default binary attribute") {
    val attr = BinaryAttribute.defaultAttr
    val metadata = Metadata.fromJson("""{"type":"binary"}""")
    val metadataWithoutType = Metadata.fromJson("{}")
    assert(attr.attrType === AttributeType.Binary)
    assert(attr.isNumeric)
    assert(attr.isNominal)
    assert(attr.name.isEmpty)
    assert(attr.index.isEmpty)
    assert(attr.values.isEmpty)
    assert(attr.toMetadata() === metadata)
    assert(attr.toMetadata(withType = true) === metadata)
    assert(attr.toMetadata(withType = false) === metadataWithoutType)
    assert(attr === Attribute.fromMetadata(metadata))
    assert(attr === BinaryAttribute.fromMetadata(metadataWithoutType))
  }

  test("customized binary attribute") {
    val name = "clicked"
    val index = 2
    val values = Array("no", "yes")
    val metadata = Metadata.fromJson(
      """{"type":"binary","name":"clicked","idx":2,"vals":["no","yes"]}""")
    val metadataWithoutType = Metadata.fromJson(
      """{"name":"clicked","idx":2,"vals":["no","yes"]}""")
    val attr = BinaryAttribute.defaultAttr
      .withName(name)
      .withIndex(index)
      .withValues(values(0), values(1))
    assert(attr.attrType === AttributeType.Binary)
    assert(attr.isNumeric)
    assert(attr.isNominal)
    assert(attr.name === Some(name))
    assert(attr.index === Some(index))
    assert(attr.values.get === values)
    assert(attr.toMetadata() === metadata)
    assert(attr.toMetadata(withType = true) === metadata)
    assert(attr.toMetadata(withType = false) === metadataWithoutType)
    assert(attr === Attribute.fromMetadata(metadata))
    assert(attr === BinaryAttribute.fromMetadata(metadataWithoutType))
  }
}
