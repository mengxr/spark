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

/**
 * An enum-like type for attribute types: [[AttributeType$#Numeric]], [[AttributeType$#Nominal]],
 * and [[AttributeType$#Binary]].
 */
sealed abstract class AttributeType(val name: String)

object AttributeType {

  private case object _Numeric extends AttributeType("numeric")
  private case object _Nominal extends AttributeType("nominal")
  private case object _Binary extends AttributeType("binary")

  /** Numeric type. */
  final val Numeric: AttributeType = _Numeric

  /** Nominal type. */
  final val Nominal: AttributeType = _Nominal

  /** Binary type. */
  final val Binary: AttributeType = _Binary

  /**
   * Gets the [[AttributeType]] object from its name.
   * @param name attribute type name: "numeric", "nominal", or "binary"
   */
  def fromName(name: String): AttributeType = {
    if (name == Numeric.name) {
      Numeric
    } else if (name == Nominal.name) {
      Nominal
    } else if (name == Binary.name) {
      Binary
    } else {
      throw new IllegalArgumentException(s"Cannot recognize type $name.")
    }
  }
}
