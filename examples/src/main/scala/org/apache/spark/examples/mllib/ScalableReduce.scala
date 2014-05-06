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

package org.apache.spark.examples.mllib

import org.apache.log4j.{Level, Logger}
import breeze.linalg._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.rdd.RDDFunctions._

object ScalableReduce {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Scalable reduce")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)
    val numPartitions = args(0).toInt
    val index = sc.parallelize(0 until numPartitions, numPartitions)
    for (n <- Seq(100, 1000, 10000, 100000, 1000000, 10000000)) {
      println("n: " + n)

      val vectors = index.map(i => DenseVector.rand[Double](n)).cache()
      vectors.count()

      var start = System.nanoTime()
      vectors.reduce(_ + _)
      println("reduce: " + (System.nanoTime() - start) / 1e9)

      start = System.nanoTime()
      vectors.treeReduce0(_ + _)
      println("treeReduce0: " + (System.nanoTime() - start) / 1e9)

      start = System.nanoTime()
      vectors.treeReduce1(_ + _)
      println("treeReduce1: " + (System.nanoTime() - start) / 1e9)

      start = System.nanoTime()
      vectors.binaryTreeReduce0(_ + _)
      println("binaryTreeReduce0: " + (System.nanoTime() - start) / 1e9)

      start = System.nanoTime()
      vectors.binaryTreeReduce1(_ + _)
      println("binaryTreeReduce1: " + (System.nanoTime() - start) / 1e9)

      vectors.unpersist()
    }

    sc.stop()
  }
}
