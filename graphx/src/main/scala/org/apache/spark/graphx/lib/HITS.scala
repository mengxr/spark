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

import scala.math
import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD



/**
 * HITS algorithm implementation. Our implementation is based on Jon
 * Kleinberg's paper (http://www.cs.cornell.edu/home/kleinber/auth.pdf)
 * and works as follows:
 *
 * 1) Initialize the hub and authority scores of all vertices to 0.0,
 *    with the exception of vertices with >= 1 outgoing edge,
 *    whose hub scores are initialized to 1.
 *
 * 2) Until we have reached the desired number of iterations...
 *    a) Set the auth score of each vertex to the sum of the hub
 *       scores of neighbors with outgoing edges to the current vertex
 *    b) Set the hub score of each vertex to the sum of the auth
 *       scores of neighbors with incoming edges from the current vertex
 *
 * 3) Normalize the hub and auth scores so that the vectors of all hub/auth
 *    scores have unit norm.
 *
 * Our algorithm also periodically normalizes the hub/auth scores
 * during step 2 to avoid Double overflow, if necessary.
 */
object HITS extends Logging {


  /**
   * Run HITS for a fixed number of iterations returning a graph
   * with vertex attributes containing auth and hub scores as
   * (Double, Double) tuples.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to run the HITS algorithm
   * @param numIter the number of iterations of HITS to run
   *
   * @return the graph with each vertex containing
   *         a tuple of its normalized auth and hub scores (in that order)
   *         along with edges with Unit attributes.
   */

  def run[VD: ClassTag, ED: ClassTag](
    graph: Graph[VD, ED], numIter: Int): Graph[(Double, Double), Unit] =
  {

    // Helper function that normalizes the auth and hub scores of a graph,
    // returning a copy of the graph with normalized vertex attributes
    def normalizeGraph(
      graph : Graph[(Double, Double), Unit]): Graph[(Double, Double), Unit] =
    {
        // Get the squareroot of the sum of the squares of our vertices' auth
        // scores
        val authNormalizer = math.sqrt(graph.vertices.map
          { case (vid, (auth, hub)) => auth * auth }.sum()
        )

        // Get the squareroot of the sum of the squares of our vertices' auth
        // scores
        val hubNormalizer = math.sqrt(graph.vertices.map
          { case (vid, (auth, hub)) => hub * hub }.sum()
        )

        val result = graph.mapVertices[(Double, Double)](
          (id, prop) => (prop._1 / authNormalizer, prop._2 / hubNormalizer)
        )
        result
    }


    // Helper function that returns an upper bound on the max number of HITS
    // iterations that can be run on the current graph without incurring Double
    // overflow.
    def getMaxIters[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED]): Int =
    {
      // Handle the case of an edgeless graph - auth and hub scores will be 0 in such
      // a graph, so we can iterate forever without encountering Double overflow
      if (graph.outDegrees.count() == 0) {
        return Int.MaxValue
      }

      // Define a reduce operation to compute the highest degree vertex
      def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
        if (a._2 > b._2) a else b
      }

      // Compute the max indegree or outdegree of any vertex
      val maxInDegree: (VertexId, Int) = graph.inDegrees.reduce(max)
      val maxOutDegree: (VertexId, Int) = graph.outDegrees.reduce(max)
      val maxDegree = math.max(maxInDegree._2, maxOutDegree._2)

      var nVertices: Double = graph.vertices.count()

      // Estimate an upper bound for the number of iterations of HITS we can
      // run without overflow. To do this, we note that the growth rate per
      // iteration of the unnormalized auth/hub scores of a vertex v is non-decreasing
      // with respect to the addition of edges adjacent to v.
      //
      // To bound the growth rate of a graph G on n vertices, we can therefore consider the case
      // in which each vertex has indegree and outdegree equal to maxDegree
      //
      // In such a graph, each vertex's hub and auth scores grow
      // grow exponentially as maxDegree^(2t) where t is the number of
      // iterations that have passed.
      //
      // The squared norm of the hub/auth scores after t iterations is therefore bounded by
      // n * (maxDegree^(2t) ^ 2) = n * maxDegree ^ (4t)
      //
      // Since we need to be able to compute the squared norm of the hub/auth scores without
      // overflow, we need Double.MaxValue >= n * maxDegree ^ (4t) - we can solve for
      // t resulting in the expression below
      //
      // Note that due to the exponential growth of unnormalized hub/auth scores,
      // this added complexity is necessary - for example, a complete graph on just 10
      // vertices will begin to experience overflow errors after ~80 iterations.

      val allowedIters = math.log(Double.MaxValue / nVertices) / (4 * math.log(maxDegree))

      // Round our upper bound down to the nearest integer but ensure
      // that it is at least 1.
      math.max(1, math.floor(allowedIters).toInt)
    }

    // Initialize the HITS graph so that each vertex v has an auth
    // score of 1.0 and a hub score of 1.0 or 0.0 if v has/doesn't have
    // outgoing edges, respectively.
    var hitsGraph: Graph[(Double, Double), Unit] = graph.mapEdges((edge) => ())
      // Vertices with no outgoing edges should have hub scores of 0.0 - all other vertices
      // should have hub scores of 1.0
      .outerJoinVertices(graph.outDegrees)
        { (vid, vdata, deg) => (0.0, math.min(deg.getOrElse(0), 1.0)) }.cache()

    // If our graph has no edges, all hub and auth scores will be 0.0
    // so we can return the HITS graph as is
    if (graph.outDegrees.count() == 0) {
      return hitsGraph
    }

    // Get an upper bound on the maximum number of iterations of HITS we can
    // perform without incurring Double overflow in either our hub or auth scores
    val itersBeforeNorm = getMaxIters(graph)

    var prevHitsGraph: Graph[(Double, Double), Unit] = null
    var iteration = 0
    while (iteration < numIter) {

      // Compute auth scores for each vertex v by summing the
      // hub scores of all vertices u with edges to v.
      val authUpdates = hitsGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr._2), _ + _, TripletFields.Src).cache()

      // Get a reference to our current graph so that we can
      // unpersist it later on
      prevHitsGraph = hitsGraph

      // Assign hitsGraph to a copy of our current graph with
      // normalized auth scores.
      hitsGraph = hitsGraph.joinVertices(authUpdates) {
        (id, oldProperties, newAuthScore) => (newAuthScore, oldProperties._2)
      }.cache()

      // Unpersist the old HITS graph's vertices (we don't modify the edges
      // so we can leave them in the cache/reuse them each iteration)
      prevHitsGraph.unpersistVertices(false)

      // Compute new hub scores for each vertex u by summing the
      // auth scores of all vertices v with incoming edges from u.
      val hubUpdates = hitsGraph.aggregateMessages[Double](
        ctx => ctx.sendToSrc(ctx.dstAttr._1), _ + _, TripletFields.Dst).cache()

      // Get reference to old graph
      prevHitsGraph = hitsGraph

      // Assign new hub scores
      hitsGraph = hitsGraph.joinVertices(hubUpdates) {
        (id, oldProperties, newHubScore) => (oldProperties._1, newHubScore)
      }.cache()

      // Uncache old graph's vertices
      prevHitsGraph.unpersistVertices(false)

      iteration += 1

      // Normalize the hub and auth scores to avoid overflow, if necessary
      if (iteration % itersBeforeNorm == 0) {
        hitsGraph = normalizeGraph(hitsGraph).cache()
      }

    }

    // Normalize and return the final hub and auth scores
    normalizeGraph(hitsGraph)
  }

}
