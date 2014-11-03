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

package org.apache.spark.ml

import java.lang.reflect.Modifier

import scala.language.implicitConversions
import scala.collection.mutable

/**
 * Describes a parameter key.
 * @param parent parent id
 * @param name parameter name
 * @param doc documentation
 * @param defaultValue default value
 * @tparam T parameter value type
 */
class Param[+T](
    val parent: Identifiable,
    val name: String,
    val doc: String,
    val defaultValue: Option[T]) extends Serializable {

  override def toString: String = s"$name: $doc (default: $defaultValue)"

  def ->[V >: T](value: V): ParamPair[V] = ParamPair(this, value)
}

case class ParamPair[+T](param: Param[T], value: T)

/**
 * Trait for components with parameters.
 */
trait Params {

  /** Returns all parameters. */
  def params: Array[Param[_]] = {
    val methods = this.getClass.getMethods
    methods.filter { m =>
      Modifier.isPublic(m.getModifiers) &&
        classOf[Param[_]].isAssignableFrom(m.getReturnType) &&
        m.getParameterTypes.isEmpty
    }.map(m => m.invoke(this).asInstanceOf[Param[_]])
  }

  /** Gets a param by its name. */
  def getParam(paramName: String): Param[_] = {
    val m = this.getClass.getMethod(paramName, null)
    assert(Modifier.isPublic(m.getModifiers) &&
      classOf[Param[_]].isAssignableFrom(m.getReturnType))
    m.invoke(this).asInstanceOf[Param[_]]
  }

  /** Validate parameters specified by the input parameter map. */
  def validateParams(paramMap: ParamMap): Unit = {}
}

object Params {
  val empty: Params = new Params {
    override def params: Array[Param[_]] = Array.empty
  }
}

/**
 * A param -> value map.
 */
class ParamMap private (params: mutable.Map[Identifiable, mutable.Map[String, Any]]) extends Serializable {

  def this() = this(mutable.Map.empty[Identifiable, mutable.Map[String, Any]])

  /**
   * Puts a new (param, value) pair.
   */
  def put[T](param: Param[T], value: T): this.type = {
    if (!params.contains(param.parent)) {
      params(param.parent) = mutable.HashMap.empty[String, Any]
    }
    params(param.parent) += param.name -> value
    this
  }

  def put[T](paramPair: ParamPair[T]): this.type = {
    put(paramPair.param, paramPair.value)
    this
  }

  /**
   * Gets the value of the input parameter or the default value if it doesn't exist.
   */
  implicit def getOrDefault[T](param: Param[T]): T = {
    params.get(param.parent)
      .flatMap(m => m.get(param.name))
      .getOrElse(param.defaultValue.get)
      .asInstanceOf[T]
  }

  /**
   * Filter this parameter map for the given parent.
   */
  def filter(parent: Identifiable): ParamMap = {
    val map = params.getOrElse(parent, mutable.Map.empty[String, Any])
    new ParamMap(mutable.Map(parent -> map))
  }

  /**
   * Make a deep copy of this parameter set.
   */
  def copy: ParamMap = new ParamMap(params.clone())

  override def toString: String = {
    params.map { case (parentId, map) =>
      s"{$parentId: $map}"
    }.mkString("{", ",", "}")
  }
}

object ParamMap {
  def empty: ParamMap = new ParamMap()
}

/**
 * Builder for a param grid used in grid search.
 */
class ParamGridBuilder {
  val paramGrid = mutable.Map.empty[Param[_], Iterable[_]]

  def add[T](param: Param[T], value: T): this.type = {
    paramGrid.put(param, Seq(value))
    this
  }

  def addMulti[T](param: Param[T], values: Iterable[T]): this.type = {
    paramGrid.put(param, values)
    this
  }

  def build(): Array[ParamMap] = {
    var paramSets = Array(new ParamMap)
    paramGrid.foreach { case (param, values) =>
      val newParamSets = values.flatMap { v =>
        paramSets.map(_.copy.put(param, v))
      }
      paramSets = newParamSets.toArray
    }
    paramSets
  }
}
