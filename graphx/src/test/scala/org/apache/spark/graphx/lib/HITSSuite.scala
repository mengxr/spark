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

package org.apache.spark.graphx.lib

import org.apache.spark.SparkFunSuite
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD


object GridHITS {
  def apply(nRows: Int, nCols: Int, nIter: Int): Seq[(VertexId, (Double, Double))] = {
    val inNbrs = Array.fill(nRows * nCols)(collection.mutable.MutableList.empty[Int])
    val outNbrs = Array.fill(nRows * nCols)(collection.mutable.MutableList.empty[Int])

    // Convert row column address into vertex ids (row major order)
    def sub2ind(r: Int, c: Int): Int = r * nCols + c
    // Make the grid graph
    for (r <- 0 until nRows; c <- 0 until nCols) {
      val ind = sub2ind(r, c)
      if (r + 1 < nRows) {
        outNbrs(ind) += sub2ind(r + 1, c)
        inNbrs(sub2ind(r + 1, c)) += ind
      }
      if (c + 1 < nCols) {
        outNbrs(ind) += sub2ind(r, c + 1)
        inNbrs(sub2ind(r, c + 1)) += ind
      }
    }

    def normalize(a: Array[Double]): Unit = {
      val norm = math.sqrt(a.map( x => x * x).reduce(_ + _))
      a.zipWithIndex.foreach { case (element, index) =>
        a(index) = element / norm
      }
    }

    // compute the authority and hub scores
    val authorityScores = Array.fill(nRows * nCols)(1.0)
    val hubScores = Array.fill(nRows * nCols)(1.0)
    for (iter <- 0 until nIter) {
      // update authority scores
      for (ind <- 0 until (nRows * nCols)) {
        authorityScores(ind) = inNbrs(ind).map( nbr => hubScores(nbr)).sum
      }
      // update hub scores
      for (ind <- 0 until (nRows * nCols)) {
        hubScores(ind) = outNbrs(ind).map( nbr => authorityScores(nbr)).sum
      }
      // normalize scores
      normalize(authorityScores)
      normalize(hubScores)
    }
    (0L until (nRows * nCols)).zip(
        authorityScores.zip(hubScores))
  }

}


class HITSSuite extends SparkFunSuite with LocalSparkContext {

  test("Star HITS") {
    withSpark { sc =>
      val nVertices = 100
      val inwardStarGraph = GraphGenerators.starGraph(sc, nVertices).cache()
      val outerVertexScore = 1.0 / math.sqrt(nVertices - 1)
      val errorTol = 1.0e-5

      val inwardScores1: VertexRDD[(Double, Double)] = inwardStarGraph
        .authoritiesAndHubs(numIter = 1).vertices
      val inwardScores2: VertexRDD[(Double, Double)] = inwardStarGraph
        .authoritiesAndHubs(numIter = 2).vertices.cache()

      // Just like PageRank, HITS should only take 2 iterations to converge
      val inwardNotMatching = inwardScores1
        .innerZipJoin(inwardScores2) { (vid, scores1, scores2) =>
          (scores1, scores2)
        }.collect().foreach { case (vid, (scores1, scores2)) =>
          assert(math.abs(scores1._1 - scores2._1) < errorTol)
          assert(math.abs(scores1._2 - scores2._2) < errorTol)
        }

      inwardScores2.collect()
        .foreach { case (vid, (authorityScore, hubScore)) =>
          if (vid > 0) {
            assert(authorityScore < errorTol)
            assert(math.abs(hubScore - outerVertexScore) < errorTol)
          }
          else if (vid == 0L) {
            assert(math.abs(authorityScore - 1.0) < errorTol)
            assert(hubScore < errorTol)
          }
        }

      val outwardStarGraph = inwardStarGraph.reverse

      val outwardScores1: VertexRDD[(Double, Double)] = outwardStarGraph
        .authoritiesAndHubs(numIter = 1).vertices
      val outwardScores2: VertexRDD[(Double, Double)] = outwardStarGraph
        .authoritiesAndHubs(numIter = 2).vertices.cache()

      val outwardNotMatching = outwardScores1
        .innerZipJoin(outwardScores2) { (vid, scores1, scores2) =>
          (scores1, scores2)
        }.collect().foreach { case (vid, (scores1, scores2)) =>
          assert(math.abs(scores1._1 - scores2._1) < errorTol)
          assert(math.abs(scores1._2 - scores2._2) < errorTol)
        }

      outwardScores2.collect()
        .foreach { case (vid, (authorityScore, hubScore)) =>
          if (vid > 0) {
            assert(math.abs(authorityScore - outerVertexScore) < errorTol)
            assert(hubScore < errorTol)
          }
          else if (vid == 0L) {
            assert(authorityScore < errorTol)
            assert(math.abs(hubScore - 1.0) < errorTol)
          }
        }

     }
  } // end of test Star HITS

  test("Circular Graph HITS") {
    withSpark { sc =>
      // Test that the normalizing the scores does not result in division by zero errors
      // for a graph with no edges
      val nVertices = 100
      val expectedScore = 1.0 / math.sqrt(nVertices)
      val errorTol = 1.0e-5

      val edges: RDD[(VertexId, VertexId)] = sc.parallelize(0 until nVertices).map { vid =>
        (vid, (vid + 1) % nVertices)
      }

      val circularGraph: Graph[Int, Int] = Graph.fromEdgeTuples(edges, 1)

      // Just like in the star graph case, HITS should only take 2 iterations to converge
      val circularScores: VertexRDD[(Double, Double)] = circularGraph
        .authoritiesAndHubs(numIter = 1).vertices

      circularScores.collect()
        .foreach { case (vid, (authorityScore, hubScore)) =>
          assert(math.abs(authorityScore - expectedScore) < errorTol)
          assert(math.abs(hubScore - expectedScore) < errorTol)
        }

     }
  } // end of test Circular Graph HITS

  test("Empty Graph HITS") {
    withSpark { sc =>
      // Test that the normalizing the scores does not result in division by zero errors
      // for a graph with no edges
      val nVertices = 100

      val vertices: RDD[(VertexId, Unit)] = sc.parallelize(0 until nVertices).map { vid =>
        (vid, Unit)
      }

      val emptyGraph: Graph[Unit, Unit] = Graph(vertices, sc.emptyRDD)

      val emptyScores: VertexRDD[(Double, Double)] = emptyGraph
        .authoritiesAndHubs(numIter = 1).vertices

      val emptyErrors = emptyScores.map { case (vid, (authorityScore, hubScore)) =>
        val correct = authorityScore == 0.0 && hubScore == 0.0
        if (!correct) 1 else 0
      }

      assert(emptyErrors.sum === 0)
     }
  } // end of test Empty Graph HITS

  test("Grid HITS") {
    withSpark { sc =>
      val rows = 10
      val cols = 10
      val numIter = 50
      val errorTol = 1.0e-5
      val gridGraph = GraphGenerators.gridGraph(sc, rows, cols)

      val computedScores = gridGraph
        .authoritiesAndHubs(numIter).vertices.collect()
        .sorted.map { case (vid, scores) => scores }
      val referenceScores = GridHITS(rows, cols, numIter)
        .sorted.map { case (vid, scores) => scores }

      computedScores.zip(referenceScores)
        .foreach { case ((authority, hub), (referenceAuthority, referenceHub)) =>
          assert(math.abs(authority - referenceAuthority) < errorTol)
          assert(math.abs(hub - referenceHub) < errorTol)
        }
    }
  } // end of Grid HITS

}
