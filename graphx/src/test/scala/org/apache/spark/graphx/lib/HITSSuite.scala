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
        val HITS1 = logGraph.hits(numIter = i).vertices
        // Each step, the authority scores and hub scores should
        // have norm 1 respectively across the graph
        val authNormDiff = math.abs(1.0 - HITS1.map(vtx => vtx._2._1 * vtx._2._1).sum())
        val hubNormDiff = math.abs(1.0 - HITS1.map(vtx => vtx._2._2 * vtx._2._2).sum())
        assert(authNormDiff <= errorTol && hubNormDiff <= errorTol)
      }
    }
  }

  test("Star HITS") {
    withSpark { sc =>
      val nVertices = 100
      val starGraph = GraphGenerators.starGraph(sc, nVertices).cache()

      val HITS1 = starGraph.hits(numIter = 1).vertices.cache()

      // For star graph, HITS algorithm converges in 1 step and
      // the weights can be computed explicitly
      val notMatchingScore = HITS1.collect().foreach{
        vtx => vtx._1 match {
          case 0 => assert(math.abs(vtx._2._1 - 1.0) < 1e-5 && math.abs(vtx._2._2) < 1e-5)
          case _ => assert(math.abs(vtx._2._1) < 1e-5 &&
              math.abs(vtx._2._2 - 1.0/math.sqrt(99)) < 1e-5)
        }
      }
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

      val HITS1 = connectedGraph.hits(numIter = 1).vertices.cache()

      // For complete graph, HITS algorithm converges in 1 step
      // the weights can be computed explicitly
      val notMatchingScore = HITS1.collect().foreach{
        case (vid, (auth, hub)) => {
          assert((math.abs(auth - 1.0/math.sqrt(30)) < 1e-5) &&
              (math.abs(hub - 1.0/math.sqrt(30)) < 1e-5))
        }
      }
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

      val HITS1 = threeGraph.hits(numIter = 10).vertices.cache()

      // We compare with the theoretical solution using principal eigenvectors
      val notMatchingScore = HITS1.collect().foreach{
        case (vid, (auth, hub)) => {
          val correct = vid match {
            case 1L => assert((math.abs(auth - 0.627963) <= 1e-5) &&
              (math.abs(hub - 0.788675) <= 1e-5))
            case 2L => assert((math.abs(auth - 0.459701) <= 1e-5) &&
              (math.abs(hub - 0.577350) <= 1e-5))
            case 3L => assert((math.abs(auth - 0.627963) <= 1e-5) &&
              (math.abs(hub - 0.211325) <= 1e-5))
          }
        }
      }
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
      val HITS1 = fourGraph.hits(numIter = 10).vertices.cache()

      // The solution is uniform for both authority and hub scores
      val notMatchingScore = HITS1.collect().foreach{
        case (vid, (auth, hub)) => {
          assert((math.abs(auth - 0.5) < 1e-5) &&
              (math.abs(hub - 0.5) < 1e-5))
        }
      }
    }
  }
}
