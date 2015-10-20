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
 * The hyperlink-induced topic search (HITS) algorithm implementation based on
 * "Authoritative sources in a hyperlinked environment" available at
 * [[http://www.cs.cornell.edu/home/kleinber/auth.pdf]].
 *
 * This implementation uses the standalone [[Graph]] interface and runs HITS
 * for a fixed number of iterations:
 * {{{
 * var authorityScores = Array.fill(n)( 1.0 )
 * var hubScores = Array.fill(n)( 1.0 )
 * val oldHubScores = Array.fill(n)( 1.0 )
 * for( iter <- 0 until numIter ) {
 *   swap(oldHubScores, hubScores)
 *   for( i <- 0 until n ) {
 *     authorityScores[i] = inNbrs[i].map(j => oldHubScores[j]).sum
 *     hubScores[i] = outNbrs[i].map(j => authorityScores[j]).sum
 *   }
 *   authorityScores = normalize(authorityScores)
 *   hubScores = normalize(hubScores)
 * }
 * }}}
 * `inNbrs[i]` is the set of neighbors which link to `i` and
 * `outNbrs[i]` is the set of neighbors which `i` links to
 */
object HITS extends Logging {

  /**
   * Run HITS for a fixed number of iterations returning a graph
   * with vertex attributes containing the authority and the hub score
   * and edge attributes containing Unit
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute authority and hub scores
   * @param numIter the number of iterations of HITS to run
   *
   * @return the graph in which each vertex contains the pair
   *         (authority score, hub score) and each edge contains Unit
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
      numIter: Int): Graph[(Double, Double), Unit] =
  {
    // Initialize the HITS graph with the authority score and the hub score
    // of each edge both equal to 1.0.
    var scoresGraph: Graph[(Double, Double), Unit] = graph
      // Set the vertex attributes to the initial authority score and hub score values
      .mapVertices( (id, attr) => (1.0, 1.0))
      // Discard the edge attributes
      .mapEdges( e => Unit)

    var iteration = 0
    while (iteration < numIter) {
      scoresGraph.cache()

      // Step 1: Compute new authority scores
      // Perform an aggregation of old hub scores at the receiving (destination) vertices.
      // Requires a shuffle for aggregation.
      val authorityScoresUpdates = scoresGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr._2), _ + _, TripletFields.Src)

      // Apply the updated authority scores, using outer join to assign a score of zero to the
      // vertices that didn't receive a message. The outer join requires a shuffle for
      // broadcasting updated scores to the edge partitions.
      scoresGraph = scoresGraph
        .outerJoinVertices(authorityScoresUpdates) {
          (id, scores, authorityScoreOption) => (authorityScoreOption.getOrElse(0.0), scores._2)
        }.cache()

      // Step 2: Compute new hub scores
      // Perform an aggregation of old authority scores at the receiving (source) vertices.
      // Requires a shuffle for aggregation.
      val hubScoresUpdates = scoresGraph.aggregateMessages[Double](
        ctx => ctx.sendToSrc(ctx.dstAttr._1), _ + _, TripletFields.Dst)

      scoresGraph = scoresGraph
        .outerJoinVertices(hubScoresUpdates) {
          (id, scores, hubScoreOption) => (scores._1, hubScoreOption.getOrElse(0.0))
        }
        .cache()

      // Step 3: Normalize the scores
      // Normalize the scores so that the scores will converge after many iterations of the
      // algorithm.
      // Calculating the score norms forces computation of scoresGraph.
      val (authorityNormSquared, hubNormSquared): (Double, Double) = scoresGraph.vertices
        .map { case (id, (authorityScore, hubScore)) =>
          (authorityScore * authorityScore, hubScore * hubScore)
        }.reduce { case ((x1, y1), (x2, y2)) =>
          (x1 + x2, y1 + y2)
        }

      val authorityNorm = math.sqrt(authorityNormSquared)
      val hubScoresNorm = math.sqrt(hubNormSquared)

      // If the norms are not zero, normalize the scores.
      if (authorityNorm != 0.0 && hubScoresNorm != 0.0) {
        scoresGraph = scoresGraph
          .mapVertices { case (id, (authorityScore, hubScore)) =>
            (authorityScore / authorityNorm, hubScore / hubScoresNorm)
          }
      }

      logInfo(s"HITS finished iteration $iteration.")

      iteration += 1
    }
    scoresGraph
  }

}
