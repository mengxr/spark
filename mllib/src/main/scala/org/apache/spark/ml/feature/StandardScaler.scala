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

package org.apache.spark.ml.feature

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Params for [[StandardScaler]] and [[StandardScalerModel]].
 */
private[feature] trait StandardScalerParams extends Params with HasInputCol with HasOutputCol {

  /**
   * Centers the data with mean before scaling.
   * It will build a dense output, so this does not work on sparse input
   * and will raise an exception.
   * Default: false
   * @group param
   */
  val withMean: BooleanParam = new BooleanParam(this, "withMean", "Center data with mean")

  /**
   * Scales the data to unit standard deviation.
   * Default: true
   * @group param
   */
  val withStd: BooleanParam = new BooleanParam(this, "withStd", "Scale to unit standard deviation")
}

/**
 * :: Experimental ::
 * Standardizes features by removing the mean and scaling to unit variance using column summary
 * statistics on the samples in the training set.
 */
@Experimental
class StandardScaler(override val uid: String) extends Estimator[StandardScalerModel]
  with StandardScalerParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("stdScal"))

  setDefault(withMean -> false, withStd -> true)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setWithMean(value: Boolean): this.type = set(withMean, value)

  /** @group setParam */
  def setWithStd(value: Boolean): this.type = set(withStd, value)

  override def fit(dataset: DataFrame): StandardScalerModel = {
    transformSchema(dataset.schema, logging = true)
    val input = dataset.select($(inputCol)).map { case Row(v: Vector) => v }
    val scaler = new feature.StandardScaler(withMean = $(withMean), withStd = $(withStd))
    val scalerModel = scaler.fit(input)
    copyValues(new StandardScalerModel(uid, scalerModel).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.isInstanceOf[VectorUDT],
      s"Input column ${$(inputCol)} must be a vector column")
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), new VectorUDT, false)
    StructType(outputFields)
  }

  override def copy(extra: ParamMap): StandardScaler = defaultCopy(extra)
}

@Since("1.6.0")
object StandardScaler extends DefaultParamsReadable[StandardScaler] {

  @Since("1.6.0")
  override def load(path: String): StandardScaler = super.load(path)
}

/**
 * :: Experimental ::
 * Model fitted by [[StandardScaler]].
 */
@Experimental
class StandardScalerModel private[ml] (
    override val uid: String,
    scaler: feature.StandardScalerModel)
  extends Model[StandardScalerModel] with StandardScalerParams with Writable {

  import StandardScalerModel._

  /** Standard deviation of the StandardScalerModel */
  val std: Vector = scaler.std

  /** Mean of the StandardScalerModel */
  val mean: Vector = scaler.mean

  /** Whether to scale to unit standard deviation. */
  @Since("1.6.0")
  def getWithStd: Boolean = scaler.withStd

  /** Whether to center data with mean. */
  @Since("1.6.0")
  def getWithMean: Boolean = scaler.withMean

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val scale = udf { scaler.transform _ }
    dataset.withColumn($(outputCol), scale(col($(inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.isInstanceOf[VectorUDT],
      s"Input column ${$(inputCol)} must be a vector column")
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), new VectorUDT, false)
    StructType(outputFields)
  }

  override def copy(extra: ParamMap): StandardScalerModel = {
    val copied = new StandardScalerModel(uid, scaler)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("1.6.0")
  override def write: Writer = new StandardScalerModelWriter(this)
}

@Since("1.6.0")
object StandardScalerModel extends Readable[StandardScalerModel] {

  private[StandardScalerModel]
  class StandardScalerModelWriter(instance: StandardScalerModel) extends Writer {

    private case class Data(std: Vector, mean: Vector, withStd: Boolean, withMean: Boolean)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.std, instance.mean, instance.getWithStd, instance.getWithMean)
      val dataPath = new Path(path, "data").toString
      sqlContext.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class StandardScalerModelReader extends Reader[StandardScalerModel] {

    private val className = "org.apache.spark.ml.feature.StandardScalerModel"

    override def load(path: String): StandardScalerModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val Row(std: Vector, mean: Vector, withStd: Boolean, withMean: Boolean) =
        sqlContext.read.parquet(dataPath)
          .select("std", "mean", "withStd", "withMean")
          .head()
      // This is very likely to change in the future because withStd and withMean should be params.
      val oldModel = new feature.StandardScalerModel(std, mean, withStd, withMean)
      val model = new StandardScalerModel(metadata.uid, oldModel)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  @Since("1.6.0")
  override def read: Reader[StandardScalerModel] = new StandardScalerModelReader

  @Since("1.6.0")
  override def load(path: String): StandardScalerModel = super.load(path)
}
