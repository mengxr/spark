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

package org.apache.spark.ml.classification

import org.apache.spark.ml._
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.linalg.{VectorUDT, BLAS, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.analysis.Star
import org.apache.spark.sql.catalyst.dsl._
import org.apache.spark.sql.catalyst.expressions.Row

class LogisticRegression(override val id: String) extends Estimator {

  def this() = this("lr-" + Identifiable.randomId())

  val maxIter: Param[Int] = new Param(this, "maxIter", "max number of iterations", Some(100))

  val regParam: Param[Double] = new Param(this, "regParam", "regularization constant", Some(0.1))

  val labelCol: Param[String] = new Param(this, "labelCol", "label column name", Some("label"))

  val featuresCol: Param[String] =
    new Param(this, "featuresCol", "features column name", Some("features"))

  override def fit(dataset: SchemaRDD, paramMap: ParamMap): LogisticRegressionModel = {
    import dataset.sqlContext._
    import paramMap._
    val instances = dataset.select((labelCol: String).attr, (featuresCol: String).attr)
      .map { case Row(label: Double, features: Vector) =>
        LabeledPoint(label, features)
      }.cache()
    val lr = new LogisticRegressionWithLBFGS
    lr.optimizer
      .setRegParam(regParam)
      .setNumIterations(maxIter)
    val model = lr.run(instances)
    new LogisticRegressionModel(id + ".model", model.weights)
  }
}

class LogisticRegressionModel(override val id: String, weights: Vector) extends Transformer {
  override def transform(dataset: SchemaRDD, paramMap: ParamMap): SchemaRDD = {
    val udt = new VectorUDT()
    val score: Any => Double = (datum) => {
      val v = udt.deserialize(datum)
      val margin = BLAS.dot(v, weights)
      1.0 / (1.0 + math.exp(-margin))
    }
    import dataset.sqlContext._
    dataset.select(Star(None), score.call('features) as 'score)
  }
}
