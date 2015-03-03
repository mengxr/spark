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

class AttributeSuite extends FunSuite {
  test("default numeric attribute") {
    val default = NumericAttribute.defaultAttr
    assert(default.attrType === AttributeType.Numeric)
    assert(default.name.isEmpty)
    assert(default.index.isEmpty)
    assert(default.min.isEmpty)
    assert(default.max.isEmpty)
    assert(default.std.isEmpty)
    assert(default.sparsity.isEmpty)
    assert(default.isNumeric === true)
    assert(default.isNominal === false)
    assert(default.toJson === "{}")
    assert(default === Attribute.fromJson("{}"))
  }

  test("non-default numeric attribute") {
    val name = "age"
    val index = 0
    val json = """{"name":"age","index":0}"""
    val attr = NumericAttribute.defaultAttr
      .withName(name)
      .withIndex(index)
    assert(attr.attrType == AttributeType.Numeric)
    assert(attr.name.get === name)
    assert(attr.index.get === index)
    assert(attr.toJson === json)
    assert(attr === Attribute.fromJson(json))
  }
}
