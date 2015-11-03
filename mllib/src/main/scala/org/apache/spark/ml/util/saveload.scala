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

package org.apache.spark.ml.util

import java.io.IOException
import java.{util => ju}

import scala.annotation.varargs
import scala.collection.mutable
import scala.collection.JavaConverters._

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.annotation.{Since, Experimental}
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.ml.param.{Param, ParamPair, Params}
import org.apache.spark.sql.SQLContext

/**
 * Trait for [[Saver]] and [[Loader]].
 */
private[util] sealed trait BaseSaveLoad {
  private var optionSQLContext: Option[SQLContext] = None

  /**
   * User-specified options.
   */
  protected final var options: mutable.Map[String, String] = mutable.Map.empty

  /**
   * Java-friendly version of [[options]].
   */
  protected final def javaOptions: ju.Map[String, String] = options.asJava

  /**
   * Sets the SQL context to use for saving/loading.
   */
  def context(sqlContext: SQLContext): this.type = {
    optionSQLContext = Option(sqlContext)
    this
  }

  /**
   * Returns the user-specified SQL context or the default.
   */
  protected final def sqlContext: SQLContext = optionSQLContext.getOrElse {
    SQLContext.getOrCreate(SparkContext.getOrCreate())
  }

  /**
   * Adds one or more options as (key, value) pairs.
   */
  def options(first: (String, String), others: (String, String)*): this.type = {
    options += first
    options ++= others
    this
  }

  /**
   * Adds one or more options with alternating key and value strings.
   * @param k1 first key
   * @param v1 first value
   * @param others other options, must be paired
   */
  @varargs
  def options(k1: String, v1: String, others: String*): this.type = {
    options += k1 -> v1
    require(others.length % 2 == 0,
      s"Options must be specified in pairs but got: ${others.mkString(",")}.")
    others.grouped(2).foreach { case Seq(k, v) =>
      options += k -> v
    }
    this
  }

  /**
   * Adds options as a Scala map.
   * @return
   */
  def options(options: Map[String, String]): this.type = {
    this.options ++= options
    this
  }

  /**
   * Adds options as a Java map.
   */
  def options(options: ju.Map[String, String]): this.type = {
    this.options ++= options.asScala
    this
  }
}

/**
 * Abstract class for utility classes that can save ML instances.
 */
@Experimental
@Since("1.6.0")
abstract class Saver extends BaseSaveLoad {

  /**
   * Saves the ML instance to the input path.
   */
  def to(path: String): Unit
}

/**
 * Trait for classes that provide [[Saver]].
 */
@Since("1.6.0")
trait Saveable {

  /**
   * Returns a [[Saver]] instance for this class.
   */
  def save: Saver
}

/**
 * Abstract class for utility classes that can load ML instances.
 * @tparam T ML instance type
 */
@Experimental
@Since("1.6.0")
abstract class Loader[T] extends BaseSaveLoad {

  /**
   * Loads the ML component from the input path.
   */
  def from(path: String): T
}

/**
 * Default [[Saver]] implementation for non-meta transformers and estimators.
 * @param instance object to save
 */
private[ml] class DefaultParamsSaver(instance: Params) extends Saver with Logging {

  options("overwrite" -> "false")

  /**
   * Saves the ML component to the input path.
   */
  override def to(path: String): Unit = {
    val sc = sqlContext.sparkContext

    val hadoopConf = sc.hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)
    val p = new Path(path)
    if (fs.exists(p)) {
      if (options("overwrite").toBoolean) {
        logInfo(s"Path $path already exists. It will be overwritten.")
        fs.delete(p, true)
      } else {
        throw new IOException(
          s"Path $path already exists. Please set overwrite=true to overwrite it.")
      }
    }

    val uid = instance.uid
    val cls = instance.getClass.getName
    val params = instance.params.asInstanceOf[Array[Param[Any]]]
      .flatMap(p => instance.get(p).map(v => p -> v))
    val jsonParams = params.map { case ParamPair(p, v) =>
      p.name -> parse(p.jsonEncode(v))
    }.toList
    val metadata = ("class" -> cls) ~
      ("timestamp" -> System.currentTimeMillis()) ~
      ("uid" -> uid) ~
      ("params" -> jsonParams)
    val metadataPath = new Path(path, "metadata").toString
    val metadataJson = compact(render(metadata))
    sc.parallelize(Seq(metadataJson), 1).saveAsTextFile(metadataPath)
  }
}

/**
 * Default [[Loader]] implementation for non-meta transformers and estimators.
 * @tparam T ML instance type
 */
private[ml] class DefaultParamsLoader[T] extends Loader[T] {

  /**
   * Loads the ML component from the input path.
   */
  override def from(path: String): T = {
    implicit val format = DefaultFormats
    val sc = sqlContext.sparkContext
    val metadataPath = new Path(path, "metadata").toString
    val metadataStr = sc.textFile(metadataPath, 1).first()
    val metadata = parse(metadataStr)
    val cls = Class.forName((metadata \ "class").extract[String])
    val uid = (metadata \ "uid").extract[String]
    val instance = cls.getConstructor(classOf[String]).newInstance(uid).asInstanceOf[Params]
    (metadata \ "params") match {
      case JObject(pairs) =>
        pairs.foreach { case (paramName, jsonValue) =>
          val param = instance.getParam(paramName)
          val value = param.jsonDecode(compact(render(jsonValue)))
          instance.set(param, value)
        }
      case _ =>
        throw new IllegalArgumentException(s"Cannot recognize JSON metadata: $metadataStr.")
    }
    instance.asInstanceOf[T]
  }
}
