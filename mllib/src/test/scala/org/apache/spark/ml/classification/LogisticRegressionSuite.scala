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

import org.apache.spark.sql.Row
import org.scalatest.FunSuite

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.test.TestSQLContext._

class LogisticRegressionSuite extends FunSuite {

  test("logistic regression") {
    val dataset = MLUtils.loadLibSVMFile(sparkContext, "../data/mllib/sample_libsvm_data.txt")
    val lr = new LogisticRegression()
    val model = {
      import lr._
      fit(dataset, maxIter -> 10, regParam -> 1.0)
    }
    val predictions = model.transform(dataset)
    predictions.select('label, 'score).collect()
      .foreach { case Row(label: Double, score: Double) =>
        println(label, score)
      }
  }
}
