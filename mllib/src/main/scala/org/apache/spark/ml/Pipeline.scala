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

import org.apache.spark.sql.SchemaRDD

import scala.collection.mutable.ListBuffer

trait PipelineStage extends Identifiable

class Pipeline(override val id: String) extends Estimator {

  def this() = this("Pipeline-" + Identifiable.randomId())

  val stages: Param[Array[PipelineStage]] =
    new Param[Array[PipelineStage]](this, "stages", "stages of the pipeline", None)

  override def fit(dataset: SchemaRDD, paramMap: ParamMap): Transformer = {
    val theStages = paramMap.getOrDefault(stages)
    // Search for last estimator.
    var lastIndexOfEstimator = -1
    theStages.view.zipWithIndex.foreach { case (stage, index) =>
      stage match {
        case _: Estimator =>
          lastIndexOfEstimator = index
        case _ =>
      }
    }
    var curDataset = dataset
    val transformers = ListBuffer.empty[Transformer]
    theStages.view.zipWithIndex.foreach { case (stage, index) =>
      stage match {
        case estimator: Estimator =>
          val transformer = estimator.fit(dataset, paramMap)
          if (index < lastIndexOfEstimator) {
            curDataset = transformer.transform(curDataset, paramMap)
          }
          transformers += transformer
        case transformer: Transformer =>
          if (index < lastIndexOfEstimator) {
            curDataset = transformer.transform(curDataset, paramMap)
          }
          transformers += transformer
        case _ =>
          throw new IllegalArgumentException
      }
    }

    new Pipeline.Model(transformers.toArray)
  }

  override def params: Array[Param[_]] = Array.empty
}

object Pipeline {

  class Model(override val id: String, val transformers: Array[Transformer]) extends Transformer {

    def this(transformers: Array[Transformer]) = this("Pipeline.Model-" + Identifiable.randomId(), transformers)

    override def transform(dataset: SchemaRDD, paramMap: ParamMap): SchemaRDD = {
      transformers.foldLeft(dataset) { (dataset, transformer) =>
        transformer.transform(dataset, paramMap)
      }
    }
  }
}

