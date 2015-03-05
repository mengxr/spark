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

class AttributeGroupSuite extends FunSuite {

  test("attribute group") {
    val attrs = Array(
      NumericAttribute.defaultAttr,
      NominalAttribute.defaultAttr,
      BinaryAttribute.defaultAttr,
      NumericAttribute.defaultAttr.withName("age").withSparsity(0.8),
      NominalAttribute.defaultAttr.withName("size").withValues("small", "medium", "large"),
      BinaryAttribute.defaultAttr.withName("clicked").withValues("no", "yes"),
      NumericAttribute.defaultAttr,
      NumericAttribute.defaultAttr)
    val group = new AttributeGroup("user", attrs)
    assert(group.size === 8)
    assert(group.name === "user")
    assert(group.indexOf("age") === 3)
    assert(group.indexOf("size") === 4)
    assert(group.indexOf("clicked") === 5)
    assert(!group.hasAttr("abc"))
    intercept[NoSuchElementException] {
      group("abc")
    }
    assert(group === AttributeGroup.fromMetadata(group.toMetadata, group.name))
  }
}
