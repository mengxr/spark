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

class HITSSuite extends SparkFunSuite with LocalSparkContext {

  test("Unit Norm") {
    withSpark { sc =>
      val nVertices = 100
      val logGraph = GraphGenerators.logNormalGraph(sc, nVertices).cache()
      val errorTol = 1.0e-5

      for (i <- 1 to 10) {
        val HITS1 = logGraph.hits(numIter = i, normFreq = 5).vertices
        // Each step, the authority scores and hub scores should
        // have norm 1 respectively across the graph
        val authNormDiff = math.abs(1.0 - HITS1.map(vtx => vtx._2._1 * vtx._2._1).sum)
        val hubNormDiff = math.abs(1.0 - HITS1.map(vtx => vtx._2._2 * vtx._2._2).sum)
        assert(authNormDiff <= errorTol && hubNormDiff <= errorTol)
      }
    }
  }

  test("Star HITS") {
    withSpark { sc =>
      val nVertices = 100
      val starGraph = GraphGenerators.starGraph(sc, nVertices).cache()

      val HITS1 = starGraph.hits(numIter = 1, normFreq = 5).vertices
      val HITS2 = starGraph.hits(numIter = 2, normFreq = 5).vertices.cache()

      // For star graph, HITS algorithm converges after 2 iterations and
      // the weights can be computed explicitly
      val notMatchingIter = HITS1.innerZipJoin(HITS2) { (vid, ah1, ah2) =>
        if (ah1._1 != ah2._1 || ah1._2 != ah2._2) 1 else 0
      }.map { case (vid, test) => test }.sum()
      assert(notMatchingIter == 0)

      val notMatchingScore = HITS2.map{
        vtx => vtx._1 match {
          case 0 => if (vtx._2._1 != 1.0 || vtx._2._2 != 0.0) 1 else 0
          case _ => if (vtx._2._1 != 0.0 || vtx._2._2 != 1.0/math.sqrt(99)) 1 else 0
        }
      }.sum()
      assert(notMatchingScore == 0)
    }
  }

  test("Complete HITS") {
    withSpark { sc =>
      val n = 30

      // Construct complete graph, in which each pair of vertices are connected
      var va = new Array[(VertexId, Int)](n)
      for (i <- 0 to (n - 1)) {
        va(i) = (i, 1)
      }
      val vertices: RDD[(VertexId, Int)] = sc.parallelize(va)

      var ea = new Array[Edge[Int]](n * (n-1))
      for (i <- 0 to (n-1)) {
        var k = 0
        for (j <- 0 to (n-1)) {
          if (i != j) {
            ea(i * (n-1) + k) = Edge(i, j, 1)
            k = k + 1
          }
        }
      }
      val edges: RDD[Edge[Int]] = sc.parallelize(ea)
      val connectedGraph = Graph(vertices, edges)

      val HITS1 = connectedGraph.hits(numIter = 1, normFreq = 10).vertices
      val HITS2 = connectedGraph.hits(numIter = 2, normFreq = 10).vertices.cache()

      // For complete graph, HITS algorithm converges after 2 iterations and
      // the weights can be computed explicitly
      val notMatchingIter = HITS1.innerZipJoin(HITS2) { (vid, ah1, ah2) =>
        if (math.abs(ah1._1 - ah2._1) > 1e-5 || math.abs(ah1._2 - ah2._2) > 1e-5) 1 else 0
      }.map { case (vid, test) => test }.sum()
      assert(notMatchingIter == 0)

      val notMatchingScore = HITS2.map{
        case (vid, (auth, hub)) => {
          val correct = ((math.abs(auth - 1.0/math.sqrt(30)) < 1e-5) &&
              (math.abs(hub - 1.0/math.sqrt(30)) < 1e-5))
          if (!correct) 1 else 0
        }
      }.sum()
      assert(notMatchingScore == 0)
    }
  }

  test("Three HITS") {
    withSpark { sc =>
      // Construct a graph with 3 vertices (1, 2, 3) and with edges
      // 1 -> 1, 1 -> 2, 1 -> 3
      // 2 -> 1, 2 -> 3
      // 3 -> 2
      val vertices: RDD[(VertexId, Int)] = sc.parallelize(
          Array((1L, 1), (2L, 1), (3L, 1))
      )
      val edges: RDD[Edge[Int]] = sc.parallelize(
          Array(Edge(1L, 1L, 1), Edge(1L, 2L, 1), Edge(1L, 3L, 1),
              Edge(2L, 1L, 1), Edge(2L, 3L, 1),
              Edge(3L, 2L, 1)
          )
      )
      val threeGraph = Graph(vertices, edges)

      val HITS1 = threeGraph.hits(numIter = 40, normFreq = 10).vertices.cache()

      // We compare the solution with theoretical one, i.e. principal eigenvectors
      val notMatchingScore = HITS1.map{
        case (vid, (auth, hub)) => {
          val correct = vid match {
            case 1L => (math.abs(auth - 0.627963) > 1e-5) ||
              (math.abs(hub - 0.788675) > 1e-5)
            case 2L => (math.abs(auth - 0.459701) > 1e-5) ||
              (math.abs(hub - 0.577350) > 1e-5)
            case 3L => (math.abs(auth - 0.627963) > 1e-5) ||
              (math.abs(hub - 0.211325) > 1e-5)
          }
          if (!correct) 1 else 0
        }
      }.sum()

      assert(notMatchingScore == 0)
    }
  }

  test("Four HITS") {
    withSpark { sc =>
      // Construct a graph with 4 vertices (1, 2, 3, 4) and with edges
      // 1 -> 2, 1 -> 4
      // 2 -> 1, 2 -> 3
      // 3 -> 2, 3 -> 4
      // 4 -> 1, 4 -> 3
      val vertices: RDD[(VertexId, Int)] = sc.parallelize(
          Array((1L, 1), (2L, 1), (3L, 1), (4L, 1))
      )
      val edges: RDD[Edge[Int]] = sc.parallelize(
          Array(Edge(1L, 2L, 1), Edge(1L, 4L, 1),
              Edge(2L, 1L, 1), Edge(2L, 3L, 1),
              Edge(3L, 2L, 1), Edge(3L, 4L, 1),
              Edge(4L, 1L, 1), Edge(4L, 3L, 1)
          )
      )
      val fourGraph = Graph(vertices, edges)
      val HITS1 = fourGraph.hits(numIter = 10, normFreq = 10).vertices.cache()

      // The solution is uniform for both authority and hub scores
      val notMatchingScore = HITS1.map{
        case (vid, (auth, hub)) => {
          val correct = ((math.abs(auth - 0.5) < 1e-5) &&
              (math.abs(hub - 0.5) < 1e-5))
          if (!correct) 1 else 0
        }
      }.sum()
      assert(notMatchingScore == 0)
    }
  }
}
