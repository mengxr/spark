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
        val nbrIndex = sub2ind(r + 1, c)
        inNbrs(nbrIndex) += ind
        outNbrs(ind) += nbrIndex
      }
      if (c + 1 < nCols) {
        val nbrIndex = sub2ind(r, c + 1)
        inNbrs(nbrIndex) += ind
        outNbrs(ind) += nbrIndex          
      }
    }

    // Compute the hub and auth scores of the generated grid graph
    var hub = Array.fill(nRows * nCols)(1.0)
    var auth : Array[Double] = new Array[Double](nRows * nCols)

    // The bottom-right (last) point in the grid should
    // have a hub score of 0, as it has no outgoing edges
    hub(nRows * nCols - 1) = 0

    // Test the grid graph by manually simulating auth/hub score
    // updates nIter times
    for (iter <- 0 until nIter) {
      
      // Update the auth scores based on incoming-edge hub scores
      var authNormalizer = 0.0      
      for (ind <- 0 until (nRows * nCols)) {
        auth(ind) = inNbrs(ind).map( nbr => hub(nbr) ).sum
        authNormalizer += auth(ind) * auth(ind) 
      }

      // Normalize auth scores
      auth = auth.map( num => num / math.sqrt(authNormalizer) )

      // Update the hub scores based on outgoing-edge auth scores
      var hubNormalizer = 0.0
      for (ind <- 0 until (nRows * nCols)) {
        hub(ind) = outNbrs(ind).map( nbr => auth(nbr) ).sum
        hubNormalizer += hub(ind) * hub(ind)
      }
      hub = hub.map( num => num / math.sqrt(hubNormalizer) )
    }
    
    (0L until (nRows * nCols)).zip(auth.zip(hub))    
  }

}


class HITSSuite extends SparkFunSuite with LocalSparkContext {

  def compareScores(expected: VertexRDD[(Double, Double)], actual: VertexRDD[(Double, Double)]): Double = {
    expected.innerZipJoin(actual) { case (id, a, b) => (a._1 - b._1) * (a._1 - b._1) + (a._2 - b._2) * (a._2 - b._2) }
      .map { case (id, error) => error }.sum()
  }

  def getStarHITSResult(nVertices : Int) : Seq[(VertexId, (Double, Double))] = {
    val actualScores = Array.fill(nVertices)(0.0, 1.0 / math.sqrt(nVertices - 1))
    actualScores(0) = (1.0, 0)
    (0L until nVertices).zip(actualScores)
  }

  def getChainHITSResult(nVertices : Int) : Seq[(VertexId, (Double, Double))] = {
    val commonScore = 1.0 / math.sqrt(nVertices - 1)
    val actualScores = Array.fill(nVertices)(commonScore, commonScore)
    actualScores(0) = (0, commonScore)
    actualScores(nVertices - 1) = (commonScore, 0)
    (0L until nVertices).zip(actualScores)
  }


  // test("Edgeless HITS") {
  //   withSpark { sc => 
  //     // val nVertices = 1
  //     // val singletonGraph = GraphGenerators.starGraph(sc, nVertices).cache()

  //     // val scores = HITS.run(graph = singletonGraph, numIter = 1).vertices.cache()
  //     // TODO assert that hub and auth scores are 0
  //     // scores.unpersist(false)

  //   }
  // } // end of test edgeless HITS


  // test("Cycle HITS") {
  //   withSpark { sc => 
  //
  //   }    
  // }

  test("Star HITS") {
    withSpark { sc =>
      val nVertices = 1000
      val starGraph = GraphGenerators.starGraph(sc, nVertices).cache()
      val errorTol = 1.0e-5

      val staticScores1 = HITS.run(graph = starGraph, numIter = 1).vertices
      val staticScores2 = HITS.run(graph = starGraph, numIter = 10).vertices.cache()     

      // HITS should only take 1 iteration to converge on a star
      // graph
      val notMatching = staticScores1.innerZipJoin(staticScores2) { (vid, pr1, pr2) =>
        if (pr1 != pr2) 1 else 0
      }.map { case (vid, test) => test }.sum()
      assert(notMatching === 0)

      // For a star graph on n vertices, the middle vertex (vertex 0) has n - 1 incoming edges
      // and 0 outgoing edges => a hub score of 0 and an auth score of 1

      // Similarly, the outer vertices each have one outgoing edge and no incoming
      // edges => a hub score of 1 / sqrt(n - 1) and an auth score of 0
      val actualScores = VertexRDD(sc.parallelize(getStarHITSResult(nVertices)))
      assert(compareScores(actualScores, staticScores1) < errorTol)
    }
  } // end of test Star HITS


  test("Grid HITS") {
    withSpark { sc =>
      val rows = 150
      val cols = 150
      val numIter = 5
      val errorTol = 1.0e-5
      val gridGraph = GraphGenerators.gridGraph(sc, rows, cols).cache()

      val staticScores = HITS.run(graph = gridGraph, numIter = numIter).vertices.cache()
      val referenceScores = VertexRDD(sc.parallelize(GridHITS(rows, cols, numIter))).cache()


      assert(compareScores(referenceScores, staticScores) < errorTol)
    }
  } // end of test Grid HITS

  test("Chain HITS") {
    withSpark { sc =>
      val nVertices = 200
      val chainSequence = (0 until nVertices - 1).map(x => (x, x + 1))
      val rawEdges = sc.parallelize(chainSequence, 1).map { case (s, d) => (s.toLong, d.toLong) }
      val chain = Graph.fromEdgeTuples(rawEdges, 1.0).cache()

      val nIter = 10

      val oneIterationScores = HITS.run(graph = chain, numIter = 1).vertices
      val manyIterationScores = HITS.run(graph = chain, numIter = nIter).vertices.cache() 
      val errorTol = 1.0e-5


      // HITS should only take 1 iteration to converge on a chain
      // graph, so the results after 1 and nIter > 1 iterations should be the same
      assert(compareScores(oneIterationScores, manyIterationScores) < errorTol)


      // When run on an n-vertex chain graph rooted at vertex 0,
      // vertices 1 through n - 1 should have auth scores of 1 / sqrt(n - 1), and vertex
      // 0 should have an auth score of 0.
      // Similarly, vertex n - 1 should should have an hub score of 0 while vertices
      // 0 through n - 2 should have hub scores of 1 / sqrt(n - 1).
      val actualScores = VertexRDD(sc.parallelize(getChainHITSResult(nVertices)))
      assert(compareScores(oneIterationScores, actualScores) < errorTol)
    }
  } // end of test Chain HITS

}
