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

import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.graphx._

/**
 * HITS (Hyperlink-Induced Topic Search) algorithm implementation.
 * Reference: Kleinberg, Jon M. "Authoritative sources in a hyperlinked environment."
 *            Journal of the ACM (JACM) 46.5 (1999): 604-632.
 *
 * The implementation uses the standalone [[Graph]] interface and runs HITS
 * for a fixed number of iterations:
 * {{{
 * var HITS = Array.fill(n, 2)( (1.0, 1.0) )
 * for( iter <- 0 until numIter ) {
 *   var norm = 0.0
 *   for( i <- 0 until n ) {
 *     HITS[i][0] = inNbrs[i].map(j => HITS[j][1]).sum
 *     norm += HITS[i][0] * HITS[i][0]
 *   }
 *   norm = sqrt(norm)
 *   for( i <- 0 until n) {
 *     HITS[i][0] /= norm
 *   }
 *   norm = 0.0
 *   for (i <- 0 until n ) {
 *     HITS[i][1] = outNbrs[i].map(j => HITS[j][0]).sum
 *     norm += HITS[i][1] * HITS[i][1]
 *   }
 *   norm = sqrt(norm)
 *   for( i <- 0 until n) {
 *     HITS[i][1] /= norm
 *   }
 * }
 * }}}
 *
 * `inNbrs[i]` is the set of neighbors whick link to `i` and `outNbrs[i]` is the set of neighbors
 * which link from `i`.
 *
 */
object HITS extends Logging {

  /**
   * Run HITS algorithm for a fixed number of iterations returning a graph
   * with vertex attributes containing authority and hub scores
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute authority and hub scores
   * @param numIter the number of iterations of HITS to run
   * @param normFreq the number of iterations between two normalizations
   *
   * @return the graph with each vertex containing the authority and hub
   *      scores as a tuple
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
      numIter: Int, normFreq: Int): Graph[(Double, Double), ED] =
  {
    // Initialize the HITS graph with each vertex having
    // attribute (authority, hub) = (1.0, 1.0)
    var hitsGraph: Graph[(Double, Double), ED] = graph
    // Set the vertex attributes to the initial authority and hub scores
        .mapVertices((id, attr) => (1.0, 1.0))

    var iteration = 0
    var prevHits: Graph[(Double, Double), ED] = null

    while (iteration < numIter) {
      hitsGraph.cache()
      // Compute the contribution of each vertex's hub score to the authority scores
      // of the neighbors, perform local preaggregation, and do the final aggregation
      // at the receiving vertices. Requires a shuffle for aggregation.
      val authUpdates = hitsGraph.aggregateMessages[(Double)](
      ctx => ctx.sendToDst(ctx.srcAttr._2), _ + _, TripletFields.Src)
      // Normalize the authority scores across the graph every certain iterations.
      // After normalization, the authority scores have unit Euclidean norm.
      var authNormUpdates = authUpdates
      if (((iteration + 1) % normFreq == 0) || (iteration == numIter - 1)) {
        val normAuth = math.sqrt(authUpdates.map(auth => auth._2 * auth._2).sum())
        authNormUpdates = authUpdates.mapValues(attr => attr / normAuth)
      }
      prevHits = hitsGraph
      // Apply the authority updates to get the new authority scores, using outerjoin to set 0 to
      // the authority score of vertices that didn't receive a message.
      // Requires a shuffle for broadcasting updated authority scores to the edge partitions.
      hitsGraph = hitsGraph.outerJoinVertices(authNormUpdates) {
          (id, attr, msg) => msg match {
            case Some(m) => (m, attr._2)
            case None => (0.0, attr._2)
          }
      }

      // Update the hub scores in a similar way as the update of authority scores above.
      val hubUpdates = hitsGraph.aggregateMessages[Double](
        ctx => ctx.sendToSrc(ctx.dstAttr._1), _ + _, TripletFields.Dst)
      // Normalize the hub scores every certain iterations
      var hubNormUpdates = hubUpdates
      if (((iteration + 1) % normFreq == 0) || (iteration == numIter - 1)) {
        val normHub = math.sqrt(hubUpdates.map(hub => hub._2 * hub._2).sum())
        hubNormUpdates = hubUpdates.mapValues(attr => attr / normHub)
      }

      hitsGraph = hitsGraph.outerJoinVertices(hubNormUpdates) {
        (id, attr, msg) => msg match {
          case Some(m) => (attr._1, m)
          case None => (attr._1, 0.0)
          }
      }.cache()

      hitsGraph.vertices.foreachPartition(x => {})
      logInfo(s"HITS finished iteration $iteration.")
      prevHits.unpersist()

      iteration += 1
    }

    hitsGraph
  }
}
