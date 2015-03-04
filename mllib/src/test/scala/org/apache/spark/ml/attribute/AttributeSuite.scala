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

import org.apache.spark.sql.types.Metadata
import org.scalatest.FunSuite

class AttributeSuite extends FunSuite {
  test("default numeric attribute") {
    val attr: NumericAttribute = NumericAttribute.defaultAttr
    val json = "{}"
    assert(attr.attrType === AttributeType.Numeric)
    assert(attr.isNumeric)
    assert(!attr.isNominal)
    assert(attr.name.isEmpty)
    assert(attr.index.isEmpty)
    assert(attr.min.isEmpty)
    assert(attr.max.isEmpty)
    assert(attr.std.isEmpty)
    assert(attr.sparsity.isEmpty)
    assert(attr.toString === json)
    assert(attr === Attribute.fromMetadata(Metadata.fromJson(json)))
  }

  test("customized numeric attribute") {
    val name = "age"
    val index = 0
    val json = """{"name":"age","index":0}"""
    val attr: NumericAttribute = NumericAttribute.defaultAttr
      .withName(name)
      .withIndex(index)
    assert(attr.attrType == AttributeType.Numeric)
    assert(attr.isNumeric)
    assert(!attr.isNominal)
    assert(attr.name === Some(name))
    assert(attr.index === Some(index))
    assert(attr.toString === json)
    assert(attr === Attribute.fromMetadata(Metadata.fromJson(json)))
  }

  test("default nominal attribute") {
    val attr: NominalAttribute = NominalAttribute.defaultAttr
    val json = """{"type":"nominal"}"""
    assert(attr.attrType === AttributeType.Nominal)
    assert(!attr.isNumeric)
    assert(attr.isNominal)
    assert(attr.name.isEmpty)
    assert(attr.index.isEmpty)
    assert(attr.values.isEmpty)
    assert(attr.cardinality.isEmpty)
    assert(attr.isOrdinal.isEmpty)
    assert(attr.toString === json)
    assert(attr === Attribute.fromMetadata(Metadata.fromJson(json)))
  }

  test("customized nominal attribute") {
    val name = "size"
    val index = 1
    val values = Seq("small", "medium", "large")
    val json = """{"type":"nominal","name":"size","index":1,"values":["small","medium","large"]}"""
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
    assert(attr.toString === json)
    assert(attr === Attribute.fromMetadata(Metadata.fromJson(json)))
  }
}
