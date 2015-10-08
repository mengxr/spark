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
import scala.math


/**
 * HITS algorithm implementation. 
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

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], numIter: Int
    ): Graph[(Double, Double), Unit] =
  {

    // Initialize the HITS graph so that each vertex v has an auth 
    // score of 1.0 and a hub score of 1.0 or 0.0 if v has/doesn't have
    // outgoing edges, respectively.
    var hitsGraph: Graph[(Double, Double), Unit] = graph.mapEdges((edge) => ())
      // Vertices with no outgoing edges should have hub scores of 0.0 - all other vertices
      // should have hub scores of 1.0
      .outerJoinVertices(graph.outDegrees)
        { (vid, vdata, deg) => (0.0, math.min(deg.getOrElse(0), 1.0)) }.cache()


    var iteration = 0
    var prevHitsGraph: Graph[(Double, Double), Unit] = null

    // Helper function that returns the square of the second element
    // of a pair
    def square(pair: (VertexId, Double)) : Double = {
      pair._2 * pair._2
    }

    while (iteration < numIter) {

      // Compute auth scores for each vertex v by summing the
      // hub scores of all vertices u with edges to v.
      val authUpdates = hitsGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr._2), _ + _, TripletFields.Src).cache()


      // Get a reference to our current graph so that we can
      // unpersist it later on
      prevHitsGraph = hitsGraph      

      // Get the sum of the squares of our vertices' new auth scores
      val authNormalizer = math.sqrt(authUpdates.map(square).sum)

      // Assign hitsGraph to a copy of our current graph with 
      // normalized auth scores.
      hitsGraph = hitsGraph.joinVertices(authUpdates) {
        (id, oldProperties, newAuthScore) => (newAuthScore / authNormalizer, oldProperties._2)
      }.cache()

      // Unpersist auth updates RDD
      // authUpdates.unpersist(false)

      // Unpersist our old hits graph
      prevHitsGraph.unpersist(false)

      // Compute new hub scores for each vertex u by summing the
      // auth scores of all vertices v with incoming edges from u.
      val hubUpdates = hitsGraph.aggregateMessages[Double](
        ctx => ctx.sendToSrc(ctx.dstAttr._1), _ + _, TripletFields.Dst).cache()

      // Get the sum of squares of new hub values 
      val hubNormalizer = math.sqrt(hubUpdates.map(square).sum())

      // Get reference to old graph
      prevHitsGraph = hitsGraph

      // Normalize hub scores
      hitsGraph = hitsGraph.joinVertices(hubUpdates) {
        (id, oldProperties, newHubScore) => (oldProperties._1, newHubScore / hubNormalizer)
      }.cache()

      // Unpersist hub updates RDD
      // hubUpdates.unpersist(false)      

      // Uncache old graph
      prevHitsGraph.unpersist(false)   
      iteration += 1
    }

    // println("Done!")

    // println("============Final output=============")
    // hitsGraph.vertices.foreach(println)

    // val otherResult = GridHITS2(3, 3, 1)
    // println("===========Test output================")
    // otherResult.foreach(println)

    hitsGraph
  }

}
