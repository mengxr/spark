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

package org.apache.spark.mllib.util

import java.io.File

import scala.io.Source
import scala.math

import org.scalatest.FunSuite

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, norm => breezeNorm,
  squaredDistance => breezeSquaredDistance}
import com.google.common.base.Charsets
import com.google.common.io.Files

import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils._

class MLUtilsSuite extends FunSuite with LocalSparkContext {

  test("epsilon computation") {
    assert(1.0 + EPSILON > 1.0, s"EPSILON is too small: $EPSILON.")
    assert(1.0 + EPSILON / 2.0 === 1.0, s"EPSILON is too big: $EPSILON.")
  }

  test("fast squared distance") {
    val a = (30 to 0 by -1).map(math.pow(2.0, _)).toArray
    val n = a.length
    val v1 = new BDV[Double](a)
    val norm1 = breezeNorm(v1, 2.0)
    val precision = 1e-6
    for (m <- 0 until n) {
      val indices = (0 to m).toArray
      val values = indices.map(i => a(i))
      val v2 = new BSV[Double](indices, values, n)
      val norm2 = breezeNorm(v2, 2.0)
      val squaredDist = breezeSquaredDistance(v1, v2)
      val fastSquaredDist1 = fastSquaredDistance(v1, norm1, v2, norm2, precision)
      assert((fastSquaredDist1 - squaredDist) <= precision * squaredDist, s"failed with m = $m")
      val fastSquaredDist2 = fastSquaredDistance(v1, norm1, v2.toDenseVector, norm2, precision)
      assert((fastSquaredDist2 - squaredDist) <= precision * squaredDist, s"failed with m = $m")
    }
  }

  test("loadLibSVMFile") {
    val lines =
      """
        |+1 1:1.0 3:2.0 5:3.0
        |-1
        |-1 2:4.0 4:5.0 6:6.0
      """.stripMargin
    val tempDir = Files.createTempDir()
    val file = new File(tempDir.getPath, "part-00000")
    Files.write(lines, file, Charsets.US_ASCII)
    val path = tempDir.toURI.toString

    val pointsWithNumFeatures = loadLibSVMFile(sc, path, multiclass = false, 6).collect()
    val pointsWithoutNumFeatures = loadLibSVMFile(sc, path).collect()

    for (points <- Seq(pointsWithNumFeatures, pointsWithoutNumFeatures)) {
      assert(points.length === 3)
      assert(points(0).label === 1.0)
      assert(points(0).features === Vectors.sparse(6, Seq((0, 1.0), (2, 2.0), (4, 3.0))))
      assert(points(1).label == 0.0)
      assert(points(1).features == Vectors.sparse(6, Seq()))
      assert(points(2).label === 0.0)
      assert(points(2).features === Vectors.sparse(6, Seq((1, 4.0), (3, 5.0), (5, 6.0))))
    }

    val multiclassPoints = loadLibSVMFile(sc, path, multiclass = true).collect()
    assert(multiclassPoints.length === 3)
    assert(multiclassPoints(0).label === 1.0)
    assert(multiclassPoints(1).label === -1.0)
    assert(multiclassPoints(2).label === -1.0)

    deleteQuietly(tempDir)
  }

  test("saveAsLibSVMFile") {
    val examples = sc.parallelize(Seq(
      LabeledPoint(1.1, Vectors.sparse(3, Seq((0, 1.23), (2, 4.56)))),
      LabeledPoint(0.0, Vectors.dense(1.01, 2.02, 3.03))
    ), 2)
    val tempDir = Files.createTempDir()
    val outputDir = new File(tempDir, "output")
    MLUtils.saveAsLibSVMFile(examples, outputDir.toURI.toString)
    val lines = outputDir.listFiles()
      .filter(_.getName.startsWith("part-"))
      .flatMap(Source.fromFile(_).getLines())
      .toSet
    val expected = Set("1.1 1:1.23 3:4.56", "0.0 1:1.01 2:2.02 3:3.03")
    assert(lines === expected)
    deleteQuietly(tempDir)
  }

  test("appendBias") {
    val sv = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))
    val sv1 = appendBias(sv).asInstanceOf[SparseVector]
    assert(sv1.size === 4)
    assert(sv1.indices === Array(0, 2, 3))
    assert(sv1.values === Array(1.0, 3.0, 1.0))

    val dv = Vectors.dense(1.0, 0.0, 3.0)
    val dv1 = appendBias(dv).asInstanceOf[DenseVector]
    assert(dv1.size === 4)
    assert(dv1.values === Array(1.0, 0.0, 3.0, 1.0))
  }

  test("kFold") {
    val data = sc.parallelize(1 to 100, 2)
    val collectedData = data.collect().sorted
    val twoFoldedRdd = kFold(data, 2, 1)
    assert(twoFoldedRdd(0)._1.collect().sorted === twoFoldedRdd(1)._2.collect().sorted)
    assert(twoFoldedRdd(0)._2.collect().sorted === twoFoldedRdd(1)._1.collect().sorted)
    for (folds <- 2 to 10) {
      for (seed <- 1 to 5) {
        val foldedRdds = kFold(data, folds, seed)
        assert(foldedRdds.size === folds)
        foldedRdds.map { case (training, validation) =>
          val result = validation.union(training).collect().sorted
          val validationSize = validation.collect().size.toFloat
          assert(validationSize > 0, "empty validation data")
          val p = 1 / folds.toFloat
          // Within 3 standard deviations of the mean
          val range = 3 * math.sqrt(100 * p * (1 - p))
          val expected = 100 * p
          val lowerBound = expected - range
          val upperBound = expected + range
          assert(validationSize > lowerBound,
            s"Validation data ($validationSize) smaller than expected ($lowerBound)" )
          assert(validationSize < upperBound,
            s"Validation data ($validationSize) larger than expected ($upperBound)" )
          assert(training.collect().size > 0, "empty training data")
          assert(result ===  collectedData,
            "Each training+validation set combined should contain all of the data.")
        }
        // K fold cross validation should only have each element in the validation set exactly once
        assert(foldedRdds.map(_._2).reduce((x,y) => x.union(y)).collect().sorted ===
          data.collect().sorted)
      }
    }
  }

  test("loadVectors") {
    val vectors = sc.parallelize(Seq(
      Vectors.dense(1.0, 2.0),
      Vectors.sparse(2, Array(1), Array(-1.0)),
      Vectors.dense(0.0, 1.0)
    ), 2)
    val tempDir = Files.createTempDir()
    val outputDir = new File(tempDir, "vectors")
    val path = outputDir.toURI.toString
    vectors.saveAsTextFile(path)
    val loaded = loadVectors(sc, outputDir.toURI.toString)
    assert(vectors.collect().toSet === loaded.collect().toSet)
    deleteQuietly(tempDir)
  }

  test("loadLabeledPoints") {
    val points = sc.parallelize(Seq(
      LabeledPoint(1.0, Vectors.dense(1.0, 2.0)),
      LabeledPoint(0.0, Vectors.sparse(2, Array(1), Array(-1.0))),
      LabeledPoint(1.0, Vectors.dense(0.0, 1.0))
    ), 2)
    val tempDir = Files.createTempDir()
    val outputDir = new File(tempDir, "points")
    val path = outputDir.toURI.toString
    points.saveAsTextFile(path)
    val loaded = loadLabeledPoints(sc, path)
    assert(points.collect().toSet === loaded.collect().toSet)
    deleteQuietly(tempDir)
  }

  /** Delete a file/directory quietly. */
  def deleteQuietly(f: File) {
    if (f.isDirectory) {
      f.listFiles().foreach(deleteQuietly)
    }
    try {
      f.delete()
    } catch {
      case _: Throwable =>
    }
  }
}

