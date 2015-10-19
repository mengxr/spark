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
import scala.language.postfixOps

import org.apache.spark.Logging
import org.apache.spark.graphx._

/**
 * The hyperlink-induced topic search (HITS) algorithm implementation.
 *
 * This implementation uses the standalone [[Graph]] interface and runs HITS
 * for a fixed number of iterations:
 * {{{
 * var authorityScores = Array.fill(n)( 1.0 )
 * var hubScores = Array.fill(n)( 1.0 )
 * val oldAuthorityScores = Array.fill(n)( 1.0 )
 * val oldHubScores = Array.fill(n)( 1.0 )
 * for( iter <- 0 until numIter ) {
 *   swap(oldAuthorityScores, authorityScores)
 *   swap(oldHubScores, hubScores)
 *   for( i <- 0 until n ) {
 *     authorityScores[i] = inNbrs[i].map(j => oldHubScores[j]).sum
 *     hubScores[i] = outNbrs[i].map(j => oldAuthorityScores[j]).sum
 *   }
 * }
 * }}}
 * `inNbrs[i]` is the set of neighbors which link to `i` and
 * `outNbrs[i]` is the set of neighbors which `i` links to
 */
object HITS extends Logging {

  /**
   * Run HITS for a fixed number of iterations returning a graph
   * with vertex attributes containing the authority and the hub score
   * and edge attributes equal to 1
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute authority and hub scores
   * @param numIter the number of iterations of HITS to run
   *
   * @return the graph in which each vertex contains the pair
   *         (authority score, hub score) and each edge contains 1
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                      numIter: Int): Graph[(Double, Double), Int] =
  {
    // Initialize the HITS graph with the authority score and the hub score
    // of each edge both equal to 1.0.
    var scoresGraph: Graph[(Double, Double), Int] = graph
      // Set the vertex attributes to the initial authority score and hub score values
      .mapVertices( (id, attr) => (1.0, 1.0))
      // Discard the edge attributes
      .mapEdges( e => 1)

    var iteration = 0
    var prevScoresGraph: Graph[(Double, Double), Int] = null
    while (iteration < numIter) {
      scoresGraph.cache()
      // Compute new authority scores:
      // Perform an aggregation of old hub scores at the receiving (destination) vertices.
      // Requires a shuffle for aggregation.
      val authorityScoresUpdates = scoresGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr._2), _ + _, TripletFields.Src)

      // Compute new hub scores:
      // Perform an aggregation of old authority scores at the receiving (source) vertices.
      // Requires a shuffle for aggregation.
      val hubScoresUpdates = scoresGraph.aggregateMessages[Double](
        ctx => ctx.sendToSrc(ctx.dstAttr._1), _ + _, TripletFields.Dst)

      prevScoresGraph = scoresGraph

      // Apply the updated scores, using outer join to assign a score of zero to the vertices
      // that didn't receive a message. Each of the two outer joins requires a shuffle for
      // broadcasting updated scores to the edge partitions.
      scoresGraph = scoresGraph
        .outerJoinVertices(authorityScoresUpdates) {
          (id, oldScores, authorityScoreOption) => (authorityScoreOption.getOrElse(0.0), 0.0)
        }
        .outerJoinVertices(hubScoresUpdates) {
          (id, scores, hubScoreOption) => (scores._1, hubScoreOption.getOrElse(0.0))
        }
        .cache()

      // Calculate the norms.
      val (authorityNormSquared, hubNormSquared): (Double, Double) = scoresGraph.vertices
        .map { case (id, (authorityScore, hubScore)) =>
          (authorityScore * authorityScore, hubScore * hubScore)
        }.reduce { case ((x1, y1), (x2, y2)) =>
          (x1 + x2, y1 + y2)
        }

      val authorityNorm = Math.sqrt(authorityNormSquared)
      val hubScoresNorm = Math.sqrt(hubNormSquared)

      // If the norms are not zero, normalize the scores.
      if (authorityNorm != 0.0 && hubScoresNorm != 0.0) {
        scoresGraph = scoresGraph
          .mapVertices { case (id, (authorityScore, hubScore)) =>
            (authorityScore / authorityNorm, hubScore / hubScoresNorm)
          }
      }

      logInfo(s"HITS finished iteration $iteration.")

      prevScoresGraph.vertices.unpersist(false)
      prevScoresGraph.edges.unpersist(false)

      iteration += 1
    }
    scoresGraph
  }

}
