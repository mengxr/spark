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

package org.apache.spark.mllib.tree

import scala.util.control.Breaks._

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.tree.model._
import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.Split
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.configuration.QuantileStrategy._
import org.apache.spark.mllib.tree.configuration.FeatureType._
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.{Variance, Entropy, Gini, Impurity}
import java.util.Random
import org.apache.spark.util.random.XORShiftRandom

/**
 * A class that implements a decision tree algorithm for classification and regression. It
 * supports both continuous and categorical features.
 * @param strategy The configuration parameters for the tree algorithm which specify the type
 *                 of algorithm (classification, regression, etc.), feature type (continuous,
 *                 categorical),
 * depth of the tree, quantile calculation strategy, etc.
  */
class DecisionTree private(val strategy: Strategy) extends Serializable with Logging {

  /**
   * Method to train a decision tree model over an RDD
   * @param input RDD of [[org.apache.spark.mllib.regression.LabeledPoint]] used as training data
   *              for DecisionTree
   * @return a DecisionTreeModel that can be used for prediction
   */
  def train(input: RDD[LabeledPoint]): DecisionTreeModel = {

    // Cache input RDD for speedup during multiple passes
    input.cache()
    logDebug("algo = " + strategy.algo)

    // Finding the splits and the corresponding bins (interval between the splits) using a sample
    // of the input data.
    val (splits, bins) = DecisionTree.findSplitsBins(input, strategy)
    logDebug("numSplits = " + bins(0).length)

    // Noting numBins for the input data
    strategy.numBins = bins(0).length

    // The depth of the decision tree
    val maxDepth = strategy.maxDepth
    // The max number of nodes possible given the depth of the tree
    val maxNumNodes = scala.math.pow(2, maxDepth).toInt - 1
    // Initalizing an array to hold filters applied to points for each node
    val filters = new Array[List[Filter]](maxNumNodes)
    // The filter at the top node is an empty list
    filters(0) = List()
    // Initializing an array to hold parent impurity calculations for each node
    val parentImpurities = new Array[Double](maxNumNodes)
    // Dummy value for top node (updated during first split calculation)
    val nodes = new Array[Node](maxNumNodes)

    // The main-idea here is to perform level-wise training of the decision tree nodes thus
    // reducing the passes over the data from l to log2(l) where l is the total number of nodes.
    // Each data sample is checked for validity w.r.t to each node at a given level -- i.e.,
    // the sample is only used for the split calculation at the node if the sampled would have
    // still survived the filters of the parent nodes.
    breakable {
      for (level <- 0 until maxDepth) {

        logDebug("#####################################")
        logDebug("level = " + level)
        logDebug("#####################################")

        // Find best split for all nodes at a level
        val splitsStatsForLevel = DecisionTree.findBestSplits(input, parentImpurities, strategy,
          level, filters, splits, bins)

        for ((nodeSplitStats, index) <- splitsStatsForLevel.view.zipWithIndex) {
          // Extract info for nodes at the current level
          extractNodeInfo(nodeSplitStats, level, index, nodes)
          // Extract info for nodes at the next lower level
          extractInfoForLowerLevels(level, index, maxDepth, nodeSplitStats, parentImpurities,
            filters)
          logDebug("final best split = " + nodeSplitStats._1)

        }
        require(scala.math.pow(2, level) == splitsStatsForLevel.length)
        // Check whether all the nodes at the current level at leaves
        val allLeaf = splitsStatsForLevel.forall(_._2.gain <= 0)
        logDebug("all leaf = " + allLeaf)
        if (allLeaf) break  //no more tree construction

      }
    }

    // Initialize the top or root node of the tree
    val topNode = nodes(0)
    // Build the full tree using the node info calculated in the level-wise best split calculations
    topNode.build(nodes)

    // Return a decision tree model
    return new DecisionTreeModel(topNode, strategy.algo)
  }

  /**
   * Extract the decision tree node information for th given tree level and node index
   */
  private def extractNodeInfo(
      nodeSplitStats: (Split, InformationGainStats),
      level: Int,
      index: Int,
      nodes: Array[Node])
    : Unit = {

    val split = nodeSplitStats._1
    val stats = nodeSplitStats._2
    val nodeIndex = scala.math.pow(2, level).toInt - 1 + index
    val isLeaf = (stats.gain <= 0) || (level == strategy.maxDepth - 1)
    val node = new Node(nodeIndex, stats.predict, isLeaf, Some(split), None, None, Some(stats))
    logDebug("Node = " + node)
    nodes(nodeIndex) = node
  }

  /**
   *  Extract the decision tree node information for the children of the node
   */
  private def extractInfoForLowerLevels(
      level: Int,
      index: Int,
      maxDepth: Int,
      nodeSplitStats: (Split, InformationGainStats),
      parentImpurities: Array[Double],
      filters: Array[List[Filter]])
    : Unit = {

    // 0 corresponds to the left child node and 1 corresponds to the right child node.
    for (i <- 0 to 1) {
     // Calculating the index of the node from the node level and the index at the current level
      val nodeIndex = scala.math.pow(2, level + 1).toInt - 1 + 2 * index + i
      if (level < maxDepth - 1) {
        val impurity = if (i == 0) {
          nodeSplitStats._2.leftImpurity
        } else {
          nodeSplitStats._2.rightImpurity
        }
        logDebug("nodeIndex = " + nodeIndex + ", impurity = " + impurity)
        // noting the parent impurities
        parentImpurities(nodeIndex) = impurity
        // noting the parents filters for the child nodes
        val childFilter = new Filter(nodeSplitStats._1, if (i == 0) -1 else 1)
        filters(nodeIndex) = childFilter :: filters((nodeIndex - 1) / 2)
        for (filter <- filters(nodeIndex)) {
          logDebug("Filter = " + filter)
        }
      }
    }
  }
}

object DecisionTree extends Serializable with Logging {

  /**
   * Method to train a decision tree model over an RDD
   * @param input RDD of [[org.apache.spark.mllib.regression.LabeledPoint]] used as training data
   *              for DecisionTree
   * @param strategy The configuration parameters for the tree algorithm which specify the type
   *                 of algoritm (classification, regression, etc.), feature type (continuous,
   *                 categorical), depth of the tree, quantile calculation strategy, etc.
   * @return a DecisionTreeModel that can be used for prediction
  */
  def train(input: RDD[LabeledPoint], strategy: Strategy): DecisionTreeModel = {
    new DecisionTree(strategy).train(input: RDD[LabeledPoint])
  }

  /**
   * Method to train a decision tree model over an RDD
   * @param input input RDD of [[org.apache.spark.mllib.regression.LabeledPoint]] used as
   *              training data
   * @param algo algo classification or regression
   * @param impurity impurity criterion used for information gain calculation
   * @param maxDepth maxDepth maximum depth of the tree
   * @return a DecisionTreeModel that can be used for prediction
   */
  def train(
      input: RDD[LabeledPoint],
      algo: Algo,
      impurity: Impurity,
      maxDepth: Int)
    : DecisionTreeModel = {
    val strategy = new Strategy(algo,impurity,maxDepth)
    new DecisionTree(strategy).train(input: RDD[LabeledPoint])
  }


  /**
   * Method to train a decision tree model over an RDD
    * @param input input RDD of [[org.apache.spark.mllib.regression.LabeledPoint]] used as
   *              training data for DecisionTree
   * @param algo classification or regression
   * @param impurity criterion used for information gain calculation
   * @param maxDepth  maximum depth of the tree
   * @param maxBins maximum number of bins used for splitting features
   * @param quantileCalculationStrategy  algorithm for calculating quantiles
   * @param categoricalFeaturesInfo A map storing information about the categorical variables and
   *                                the number of discrete values they take. For example,
   *                                an entry (n -> k) implies the feature n is categorical with k
   *                                categories 0, 1, 2, ... , k-1. It's important to note that
   *                                features are zero-indexed.
   * @return a DecisionTreeModel that can be used for prediction
   */
  def train(
      input: RDD[LabeledPoint],
      algo: Algo,
      impurity: Impurity,
      maxDepth: Int,
      maxBins: Int,
      quantileCalculationStrategy: QuantileStrategy,
      categoricalFeaturesInfo: Map[Int,Int])
    : DecisionTreeModel = {
    val strategy = new Strategy(algo, impurity, maxDepth, maxBins, quantileCalculationStrategy,
      categoricalFeaturesInfo)
    new DecisionTree(strategy).train(input: RDD[LabeledPoint])
  }

  val InvalidBinIndex = -1

  /**
   * Returns an array of optimal splits for all nodes at a given level
   * @param input RDD of [[org.apache.spark.mllib.regression.LabeledPoint]] used as training data
   *              for DecisionTree
   * @param parentImpurities Impurities for all parent nodes for the current level
   * @param strategy [[org.apache.spark.mllib.tree.configuration.Strategy]] instance containing
   *                parameters for construction the DecisionTree
   * @param level Level of the tree
   * @param filters Filters for all nodes at a given level
   * @param splits possible splits for all features
   * @param bins possible bins for all features
   * @return array of splits with best splits for all nodes at a given level.
   */
  def findBestSplits(
      input: RDD[LabeledPoint],
      parentImpurities: Array[Double],
      strategy: Strategy,
      level: Int,
      filters: Array[List[Filter]],
      splits: Array[Array[Split]],
      bins: Array[Array[Bin]])
    : Array[(Split, InformationGainStats)] = {

    // Common calculations for multiple nested methods
    val numNodes = scala.math.pow(2, level).toInt
    logDebug("numNodes = " + numNodes)
    // Find the number of features by looking at the first sample
    val numFeatures = input.first().features.length
    logDebug("numFeatures = " + numFeatures)
    val numBins = strategy.numBins
    logDebug("numBins = " + numBins)

    /** Find the filters used before reaching the current code */
    def findParentFilters(nodeIndex: Int): List[Filter] = {
      if (level == 0) {
        List[Filter]()
      } else {
        val nodeFilterIndex = scala.math.pow(2, level).toInt - 1 + nodeIndex
        filters(nodeFilterIndex)
      }
    }

    /**
     * Find whether the sample is valid input for the current node. In other words,
     * does it pass through all the filters for the current node.
    */
    def isSampleValid(parentFilters: List[Filter], labeledPoint: LabeledPoint): Boolean = {

      // Leaf
      if ((level > 0) & (parentFilters.length == 0) ){
        return false
      }

      for (filter <- parentFilters) {
        val features = labeledPoint.features
        val featureIndex = filter.split.feature
        val threshold = filter.split.threshold
        val comparison = filter.comparison
        val categories = filter.split.categories
        val isFeatureContinuous = filter.split.featureType == Continuous
        val feature =  features(featureIndex)
        if (isFeatureContinuous){
          comparison match {
            case(-1) => if (feature > threshold) return false
            case(1) => if (feature <= threshold) return false
          }
        } else {
          val containsFeature = categories.contains(feature)
          comparison match {
            case(-1) =>  if (!containsFeature) return false
            case(1) =>  if (containsFeature) return false
          }

        }
      }
      true
    }

    /**
     * Finds the right bin for the given feature
     */
    def findBin(
        featureIndex: Int,
        labeledPoint: LabeledPoint,
        isFeatureContinuous: Boolean)
      : Int = {

      if (isFeatureContinuous){
        for (binIndex <- 0 until strategy.numBins) {
          val bin = bins(featureIndex)(binIndex)
          val lowThreshold = bin.lowSplit.threshold
          val highThreshold = bin.highSplit.threshold
          val features = labeledPoint.features
          if ((lowThreshold < features(featureIndex)) & (highThreshold >= features(featureIndex))) {
            return binIndex
          }
        }
        throw new UnknownError("no bin was found for continuous variable.")
      } else {
        val numCategoricalBins = strategy.categoricalFeaturesInfo(featureIndex)
        for (binIndex <- 0 until numCategoricalBins) {
          val bin = bins(featureIndex)(binIndex)
          val category = bin.category
          val features = labeledPoint.features
          if (category == features(featureIndex)) {
            return binIndex
          }
        }
        throw new UnknownError("no bin was found for categorical variable.")

      }

    }

    /**
     * Finds bins for all nodes (and all features) at a given level k features,
     * l nodes (level = log2(l)).
     * Storage label, b11, b12, b13, .., b1k,
     * b21, b22, .. , b2k,
     * bl1, bl2, .. , blk
     * Denotes invalid sample for tree by noting bin for feature 1 as -1
     */
    def findBinsForLevel(labeledPoint: LabeledPoint): Array[Double] = {

      // calculating bin index and label per feature per node
      val arr = new Array[Double](1 + (numFeatures * numNodes))
      arr(0) = labeledPoint.label
      for (nodeIndex <- 0 until numNodes) {
        val parentFilters = findParentFilters(nodeIndex)
        // Find out whether the sample qualifies for the particular node
        val sampleValid = isSampleValid(parentFilters, labeledPoint)
        val shift = 1 + numFeatures * nodeIndex
        if (!sampleValid) {
          // marking one bin as -1 is sufficient
          arr(shift) = InvalidBinIndex
        } else {
          var featureIndex = 0
          while (featureIndex < numFeatures){
            val isFeatureContinuous = strategy.categoricalFeaturesInfo.get(featureIndex).isEmpty
            arr(shift + featureIndex) = findBin(featureIndex, labeledPoint,isFeatureContinuous)
            featureIndex += 1
          }
        }
      }
      arr
    }

    /**
     * Performs a sequential aggregation over a partition for classification.
     *
     * for p bins, k features, l nodes (level = log2(l)) storage is of the form:
     * b111_left_count,b111_right_count, .... , ..
     * .. bpk1_left_count, bpk1_right_count, .... , ..
     * .. bpkl_left_count, bpkl_right_count
     *
     * @param agg Array[Double] storing aggregate calculation of size
     * 2*numSplits*numFeatures*numNodes for classification
     * @param arr Array[Double] of size 1+(numFeatures*numNodes)
     * @return Array[Double] storing aggregate calculation of size 2*numSplits*numFeatures*numNodes
     * for classification
     */
    def classificationBinSeqOp(arr: Array[Double], agg: Array[Double]) {
      for (nodeIndex <- 0 until numNodes) {
        val validSignalIndex = 1 + numFeatures * nodeIndex
        val isSampleValidForNode = arr(validSignalIndex) != InvalidBinIndex
        if (isSampleValidForNode) {
          val label = arr(0)
          for (featureIndex <- 0 until numFeatures) {
            val arrShift = 1 + numFeatures * nodeIndex
            val aggShift = 2 * numBins * numFeatures * nodeIndex
            val arrIndex = arrShift + featureIndex
            val aggIndex = aggShift + 2 * featureIndex * numBins + arr(arrIndex).toInt * 2
            label match {
              case (0.0) => agg(aggIndex) = agg(aggIndex) + 1
              case (1.0) => agg(aggIndex + 1) = agg(aggIndex + 1) + 1
            }
          }
        }
      }
    }

    /**
     * Performs a sequential aggregation over a partition for regression.
     *
     * for p bins, k features, l nodes (level = log2(l)) storage is of the form:
     * b111_count,b111_sum, b111_sum_squares .... , ..
     * .. bpk1_count, bpk1_sum, bpk1_sum_squares, .... , ..
     * .. bpkl_count, bpkl_sum, bpkl_sum_squares
     *
     * @param agg Array[Double] storing aggregate calculation of size
     *            3*numSplits*numFeatures*numNodes for classification
     * @param arr Array[Double] of size 1+(numFeatures*numNodes)
     * @return Array[Double] storing aggregate calculation of size
     *         3*numSplits*numFeatures*numNodes for regression
     */
    def regressionBinSeqOp(arr: Array[Double], agg: Array[Double]) {
      for (nodeIndex <- 0 until numNodes) {
        val validSignalIndex = 1 + numFeatures * nodeIndex
        val isSampleValidForNode = arr(validSignalIndex) != InvalidBinIndex
        if (isSampleValidForNode) {
          val label = arr(0)
          for (feature <- 0 until numFeatures) {
            val arrShift = 1 + numFeatures * nodeIndex
            val aggShift = 3 * numBins * numFeatures * nodeIndex
            val arrIndex = arrShift + feature
            val aggIndex = aggShift + 3 * feature * numBins + arr(arrIndex).toInt * 3
            //count, sum, sum^2
            agg(aggIndex) = agg(aggIndex) + 1
            agg(aggIndex + 1) = agg(aggIndex + 1) + label
            agg(aggIndex + 2) = agg(aggIndex + 2) + label*label
          }
        }
      }
    }

    /**
     * Performs a sequential aggregation over a partition.
     * for p bins, k features, l nodes (level = log2(l)) storage is of the form:
     * b111_left_count,b111_right_count, .... , ....
     * bpk1_left_count, bpk1_right_count, .... , ...., bpkl_left_count, bpkl_right_count
     * @param agg Array[Double] storing aggregate calculation of size
     *            2*numSplits*numFeatures*numNodes for classification and
     *            3*numSplits*numFeatures*numNodes for regression
     * @param arr Array[Double] of size 1+(numFeatures*numNodes)
     * @return Array[Double] storing aggregate calculation of size
     *         2*numSplits*numFeatures*numNodes for classification and
     *         3*numSplits*numFeatures*numNodes for regression
     */
    def binSeqOp(agg: Array[Double], arr: Array[Double]): Array[Double] = {
      strategy.algo match {
        case Classification => classificationBinSeqOp(arr, agg)
        case Regression => regressionBinSeqOp(arr, agg)
      }
      agg
    }

    val binAggregateLength = strategy.algo match {
      case Classification => 2*numBins * numFeatures * numNodes
      case Regression =>  3*numBins * numFeatures * numNodes
    }
    logDebug("binAggregateLength = " + binAggregateLength)

    /**
     * Combines the aggregates from partitions
     * @param agg1 Array containing aggregates from one or more partitions
     * @param agg2 Array containing aggregates from one or more partitions
     * @return Combined aggregate from agg1 and agg2
     */
    def binCombOp(agg1: Array[Double], agg2: Array[Double]): Array[Double] = {
      strategy.algo match {
        case Classification => {
          val combinedAggregate = new Array[Double](binAggregateLength)
          for (index <- 0 until binAggregateLength){
            combinedAggregate(index) = agg1(index) + agg2(index)
          }
          combinedAggregate
        }
        case Regression => {
          val combinedAggregate = new Array[Double](binAggregateLength)
          for (index <- 0 until binAggregateLength){
            combinedAggregate(index) = agg1(index) + agg2(index)
          }
          combinedAggregate
        }
      }
    }

    logDebug("input = " + input.count)
    val binMappedRDD = input.map(x => findBinsForLevel(x))
    logDebug("binMappedRDD.count = " + binMappedRDD.count)

    // calculate bin aggregates
    val binAggregates = {
      binMappedRDD.aggregate(Array.fill[Double](binAggregateLength)(0))(binSeqOp,binCombOp)
    }
    logDebug("binAggregates.length = " + binAggregates.length)

    /**
     * Calculates the information gain for all splits
     * @param leftNodeAgg left node aggregates
     * @param featureIndex feature index
     * @param splitIndex split index
     * @param rightNodeAgg right node aggregate
     * @param topImpurity impurity of the parent node
     * @return information gain and statistics for all splits
     */
    def calculateGainForSplit(
        leftNodeAgg: Array[Array[Double]],
        featureIndex: Int,
        splitIndex: Int,
        rightNodeAgg: Array[Array[Double]],
        topImpurity: Double)
      : InformationGainStats = {

      strategy.algo match {
        case Classification => {

          val left0Count = leftNodeAgg(featureIndex)(2 * splitIndex)
          val left1Count = leftNodeAgg(featureIndex)(2 * splitIndex + 1)
          val leftCount = left0Count + left1Count

          val right0Count = rightNodeAgg(featureIndex)(2 * splitIndex)
          val right1Count = rightNodeAgg(featureIndex)(2 * splitIndex + 1)
          val rightCount = right0Count + right1Count

          val impurity = {
            if (level > 0) {
              topImpurity
            } else {
              strategy.impurity.calculate(left0Count + right0Count, left1Count + right1Count)
            }
          }

          if (leftCount == 0) {
            return new InformationGainStats(0, topImpurity, Double.MinValue, topImpurity,1)
          }
          if (rightCount == 0) {
            return new InformationGainStats(0, topImpurity, topImpurity, Double.MinValue,0)
          }

          val leftImpurity = strategy.impurity.calculate(left0Count, left1Count)
          val rightImpurity = strategy.impurity.calculate(right0Count, right1Count)

          val leftWeight = leftCount.toDouble / (leftCount + rightCount)
          val rightWeight = rightCount.toDouble / (leftCount + rightCount)

          val gain = {
            if (level > 0) {
              impurity - leftWeight * leftImpurity - rightWeight * rightImpurity
            } else {
              impurity - leftWeight * leftImpurity - rightWeight * rightImpurity
            }
          }

          val predict = (left1Count + right1Count) / (leftCount + rightCount)

          new InformationGainStats(gain, impurity, leftImpurity, rightImpurity, predict)
        }
        case Regression => {
          val leftCount = leftNodeAgg(featureIndex)(3 * splitIndex)
          val leftSum = leftNodeAgg(featureIndex)(3 * splitIndex + 1)
          val leftSumSquares = leftNodeAgg(featureIndex)(3 * splitIndex + 2)

          val rightCount = rightNodeAgg(featureIndex)(3 * splitIndex)
          val rightSum = rightNodeAgg(featureIndex)(3 * splitIndex + 1)
          val rightSumSquares = rightNodeAgg(featureIndex)(3 * splitIndex + 2)

          val impurity = {
            if (level > 0) {
              topImpurity
            } else {
              val count = leftCount + rightCount
              val sum = leftSum + rightSum
              val sumSquares = leftSumSquares + rightSumSquares
              strategy.impurity.calculate(count, sum, sumSquares)
            }
          }

          if (leftCount == 0) {
            return new InformationGainStats(0, topImpurity, Double.MinValue, topImpurity,
              rightSum/rightCount)
          }
          if (rightCount == 0) {
            return new InformationGainStats(0, topImpurity ,topImpurity,
              Double.MinValue, leftSum/leftCount)
          }

          val leftImpurity = strategy.impurity.calculate(leftCount, leftSum, leftSumSquares)
          val rightImpurity = strategy.impurity.calculate(rightCount, rightSum, rightSumSquares)

          val leftWeight = leftCount.toDouble / (leftCount + rightCount)
          val rightWeight = rightCount.toDouble / (leftCount + rightCount)

          val gain = {
            if (level > 0) {
              impurity - leftWeight * leftImpurity - rightWeight * rightImpurity
            } else {
              impurity - leftWeight * leftImpurity - rightWeight * rightImpurity
            }
          }

          val predict = (leftSum + rightSum)/(leftCount + rightCount)
          new InformationGainStats(gain, impurity, leftImpurity, rightImpurity, predict)

        }
      }
    }

   /**
     * Extracts left and right split aggregates
     * @param binData  Array[Double] of size 2*numFeatures*numSplits
     * @return (leftNodeAgg, rightNodeAgg) tuple of type (Array[Double],
     *         Array[Double]) where each array is of size(numFeature,2*(numSplits-1))
     */
    def extractLeftRightNodeAggregates(
        binData: Array[Double])
      : (Array[Array[Double]], Array[Array[Double]]) = {

      strategy.algo match {
        case Classification => {

          val leftNodeAgg = Array.ofDim[Double](numFeatures, 2 * (numBins - 1))
          val rightNodeAgg = Array.ofDim[Double](numFeatures, 2 * (numBins - 1))
          for (featureIndex <- 0 until numFeatures) {
            val shift = 2*featureIndex*numBins
            leftNodeAgg(featureIndex)(0) = binData(shift + 0)
            leftNodeAgg(featureIndex)(1) = binData(shift + 1)
            rightNodeAgg(featureIndex)(2 * (numBins - 2))
              = binData(shift + (2 * (numBins - 1)))
            rightNodeAgg(featureIndex)(2 * (numBins - 2) + 1)
              = binData(shift + (2 * (numBins - 1)) + 1)
            for (splitIndex <- 1 until numBins - 1) {
              leftNodeAgg(featureIndex)(2 * splitIndex) = binData(shift + 2*splitIndex) +
                leftNodeAgg(featureIndex)(2 * splitIndex - 2)
              leftNodeAgg(featureIndex)(2 * splitIndex + 1) = binData(shift + 2*splitIndex + 1) +
                leftNodeAgg(featureIndex)(2 * splitIndex - 2 + 1)
              rightNodeAgg(featureIndex)(2 * (numBins - 2 - splitIndex)) =
                binData(shift + (2 *(numBins - 2 - splitIndex))) +
                rightNodeAgg(featureIndex)(2 * (numBins - 1 - splitIndex))
              rightNodeAgg(featureIndex)(2 * (numBins - 2 - splitIndex) + 1) =
                binData(shift + (2* (numBins - 2 - splitIndex) + 1)) +
                  rightNodeAgg(featureIndex)(2 * (numBins - 1 - splitIndex) + 1)
            }
          }
          (leftNodeAgg, rightNodeAgg)
        }

        case Regression => {

          val leftNodeAgg = Array.ofDim[Double](numFeatures, 3 * (numBins - 1))
          val rightNodeAgg = Array.ofDim[Double](numFeatures, 3 * (numBins - 1))
          for (featureIndex <- 0 until numFeatures) {
            val shift = 3*featureIndex*numBins
            leftNodeAgg(featureIndex)(0) = binData(shift + 0)
            leftNodeAgg(featureIndex)(1) = binData(shift + 1)
            leftNodeAgg(featureIndex)(2) = binData(shift + 2)
            rightNodeAgg(featureIndex)(3 * (numBins - 2)) =
              binData(shift + (3 * (numBins - 1)))
            rightNodeAgg(featureIndex)(3 * (numBins - 2) + 1) =
              binData(shift + (3 * (numBins - 1)) + 1)
            rightNodeAgg(featureIndex)(3 * (numBins - 2) + 2) =
              binData(shift + (3 * (numBins - 1)) + 2)
            for (splitIndex <- 1 until numBins - 1) {
              leftNodeAgg(featureIndex)(3 * splitIndex)
                = binData(shift + 3*splitIndex) +
                leftNodeAgg(featureIndex)(3 * splitIndex - 3)
              leftNodeAgg(featureIndex)(3 * splitIndex + 1)
                = binData(shift + 3*splitIndex + 1) +
                leftNodeAgg(featureIndex)(3 * splitIndex - 3 + 1)
              leftNodeAgg(featureIndex)(3 * splitIndex + 2)
                = binData(shift + 3*splitIndex + 2) +
                leftNodeAgg(featureIndex)(3 * splitIndex - 3 + 2)
              rightNodeAgg(featureIndex)(3 * (numBins - 2 - splitIndex))
                = binData(shift + (3 * (numBins - 2 - splitIndex))) +
                rightNodeAgg(featureIndex)(3 * (numBins - 1 - splitIndex))
              rightNodeAgg(featureIndex)(3 * (numBins - 2 - splitIndex) + 1)
                = binData(shift + (3 * (numBins - 2 - splitIndex) + 1)) +
                rightNodeAgg(featureIndex)(3 * (numBins - 1 - splitIndex) + 1)
              rightNodeAgg(featureIndex)(3 * (numBins - 2 - splitIndex) + 2)
                = binData(shift + (3 * (numBins - 2 - splitIndex) + 2)) +
                rightNodeAgg(featureIndex)(3 * (numBins - 1 - splitIndex) + 2)
            }
          }
          (leftNodeAgg, rightNodeAgg)
        }
      }
    }

    def calculateGainsForAllNodeSplits(
        leftNodeAgg: Array[Array[Double]],
        rightNodeAgg: Array[Array[Double]],
        nodeImpurity: Double)
      : Array[Array[InformationGainStats]] = {

      val gains = Array.ofDim[InformationGainStats](numFeatures, numBins - 1)

      for (featureIndex <- 0 until numFeatures) {
        for (splitIndex <- 0 until numBins -1) {
          gains(featureIndex)(splitIndex) = calculateGainForSplit(leftNodeAgg, featureIndex,
            splitIndex, rightNodeAgg, nodeImpurity)
        }
      }
      gains
    }

     /**
     * Find the best split for a node given bin aggregate data
     * @param binData Array[Double] of size 2*numSplits*numFeatures
     * @param nodeImpurity impurity of the top node
     * @return
     */
    def binsToBestSplit(
        binData: Array[Double],
        nodeImpurity: Double)
      : (Split, InformationGainStats) = {

      logDebug("node impurity = " + nodeImpurity)
      val (leftNodeAgg, rightNodeAgg) = extractLeftRightNodeAggregates(binData)
      val gains = calculateGainsForAllNodeSplits(leftNodeAgg, rightNodeAgg, nodeImpurity)

      val (bestFeatureIndex,bestSplitIndex, gainStats) = {
        var bestFeatureIndex = 0
        var bestSplitIndex = 0
        // Initialization with infeasible values
        var bestGainStats = new InformationGainStats(Double.MinValue,-1.0,-1.0,-1.0,-1)
        for (featureIndex <- 0 until numFeatures) {
          for (splitIndex <- 0 until numBins - 1){
            val gainStats =  gains(featureIndex)(splitIndex)
            if(gainStats.gain > bestGainStats.gain) {
              bestGainStats = gainStats
              bestFeatureIndex = featureIndex
              bestSplitIndex = splitIndex
            }
          }
        }
        (bestFeatureIndex,bestSplitIndex,bestGainStats)
      }

      logDebug("best split bin = " + bins(bestFeatureIndex)(bestSplitIndex))
      logDebug("best split bin = " + splits(bestFeatureIndex)(bestSplitIndex))
      (splits(bestFeatureIndex)(bestSplitIndex),gainStats)
    }

    // Calculate best splits for all nodes at a given level
    val bestSplits = new Array[(Split, InformationGainStats)](numNodes)
    def getBinDataForNode(node: Int): Array[Double] = {
      strategy.algo match {
        case Classification => {
          val shift = 2 * node * numBins * numFeatures
          val binsForNode = binAggregates.slice(shift, shift + 2 * numBins * numFeatures)
          binsForNode
        }
        case Regression => {
          val shift = 3 * node * numBins * numFeatures
          val binsForNode = binAggregates.slice(shift, shift + 3 * numBins * numFeatures)
          binsForNode
        }
      }
    }

    for (node <- 0 until numNodes){
      val nodeImpurityIndex = scala.math.pow(2, level).toInt - 1 + node
      val binsForNode: Array[Double] = getBinDataForNode(node)
      logDebug("nodeImpurityIndex = " + nodeImpurityIndex)
      val parentNodeImpurity = parentImpurities(nodeImpurityIndex)
      logDebug("node impurity = " + parentNodeImpurity)
      bestSplits(node) = binsToBestSplit(binsForNode, parentNodeImpurity)
    }
    bestSplits
  }



  /**
   * Returns split and bins for decision tree calculation.
   * @param input RDD of [[org.apache.spark.mllib.regression.LabeledPoint]] used as training data
   *              for DecisionTree
   * @param strategy [[org.apache.spark.mllib.tree.configuration.Strategy]] instance containing
   *                parameters for construction the DecisionTree
   * @return a tuple of (splits,bins) where splits is an Array of [org.apache.spark.mllib.tree
   *         .model.Split] of size (numFeatures,numSplits-1) and bins is an Array of [org.apache
   *         .spark.mllib.tree.model.Bin] of size (numFeatures,numSplits1)
   */
  def findSplitsBins(
      input: RDD[LabeledPoint],
      strategy: Strategy)
    : (Array[Array[Split]], Array[Array[Bin]]) = {

    val count = input.count()

    // Find the number of features by looking at the first sample
    val numFeatures = input.take(1)(0).features.length

    val maxBins = strategy.maxBins
    val numBins = if (maxBins <= count) maxBins else count.toInt
    logDebug("numBins = " + numBins)

    // I will also add a require statement ensuring #bins is always greater than the categories
    // It's a limitation of the current implementation but a reasonable tradeoff since features
    // with large number of categories get favored over continuous features.
    if (strategy.categoricalFeaturesInfo.size > 0){
      val maxCategoriesForFeatures = strategy.categoricalFeaturesInfo.maxBy(_._2)._2
      require(numBins >= maxCategoriesForFeatures)
    }

    // Calculate the number of sample for approximate quantile calculation
    val requiredSamples = numBins*numBins
    val fraction = if (requiredSamples < count) requiredSamples.toDouble / count else 1.0
    logDebug("fraction of data used for calculating quantiles = " + fraction)

    // sampled input for RDD calculation
    val sampledInput = input.sample(false, fraction, new XORShiftRandom().nextInt()).collect()
    val numSamples = sampledInput.length

    val stride: Double = numSamples.toDouble / numBins
    logDebug("stride = " + stride)

    strategy.quantileCalculationStrategy match {
      case Sort => {
        val splits = Array.ofDim[Split](numFeatures, numBins-1)
        val bins = Array.ofDim[Bin](numFeatures, numBins)

        //Find all splits
        for (featureIndex <- 0 until numFeatures){
          val isFeatureContinuous = strategy.categoricalFeaturesInfo.get(featureIndex).isEmpty
          if (isFeatureContinuous) {
            val featureSamples  = sampledInput.map(lp => lp.features(featureIndex)).sorted

            val stride: Double = numSamples.toDouble/numBins
            logDebug("stride = " + stride)
            for (index <- 0 until numBins-1) {
              val sampleIndex = (index + 1)*stride.toInt
              val split = new Split(featureIndex, featureSamples(sampleIndex), Continuous, List())
              splits(featureIndex)(index) = split
            }
          } else {
            val maxFeatureValue = strategy.categoricalFeaturesInfo(featureIndex)

            require(maxFeatureValue < numBins, "number of categories should be less than number " +
              "of bins")

            val centriodForCategories
            = sampledInput.map(lp => (lp.features(featureIndex),lp.label))
              .groupBy(_._1).mapValues(x => x.map(_._2).sum / x.map(_._1).length)

            // Checking for missing categorical variables
            val fullCentriodForCategories = scala.collection.mutable.Map[Double,Double]()
            for (i <- 0 until maxFeatureValue) {
              if (centriodForCategories.contains(i)) {
                fullCentriodForCategories(i) = centriodForCategories(i)
              } else {
                fullCentriodForCategories(i) = Double.MaxValue
              }
            }

            val categoriesSortedByCentriod
            = fullCentriodForCategories.toList.sortBy{_._2}

            logDebug("centriod for categorical variable = " + categoriesSortedByCentriod)

            var categoriesForSplit = List[Double]()
            categoriesSortedByCentriod.iterator.zipWithIndex foreach {
              case((key, value), index) => {
                categoriesForSplit = key :: categoriesForSplit
                splits(featureIndex)(index) = new Split(featureIndex, Double.MinValue, Categorical,
                  categoriesForSplit)
                bins(featureIndex)(index) = {
                  if(index == 0) {
                    new Bin(new DummyCategoricalSplit(featureIndex, Categorical),
                      splits(featureIndex)(0), Categorical, key)
                  }
                  else {
                    new Bin(splits(featureIndex)(index-1), splits(featureIndex)(index),
                      Categorical, key)
                  }
                }
              }
            }
          }
        }

        // Find all bins
        for (featureIndex <- 0 until numFeatures){
          val isFeatureContinuous = strategy.categoricalFeaturesInfo.get(featureIndex).isEmpty
          if (isFeatureContinuous) {  // bins for categorical variables are already assigned
            bins(featureIndex)(0)
              = new Bin(new DummyLowSplit(featureIndex, Continuous),splits(featureIndex)(0),
              Continuous,Double.MinValue)
            for (index <- 1 until numBins - 1){
              val bin = new Bin(splits(featureIndex)(index-1), splits(featureIndex)(index),
                Continuous, Double.MinValue)
              bins(featureIndex)(index) = bin
            }
            bins(featureIndex)(numBins-1)
              = new Bin(splits(featureIndex)(numBins-2),new DummyHighSplit(featureIndex,
              Continuous), Continuous, Double.MinValue)
          }
        }
        (splits,bins)
      }
      case MinMax => {
        throw new UnsupportedOperationException("minmax not supported yet.")
      }
      case ApproxHist => {
        throw new UnsupportedOperationException("approximate histogram not supported yet.")
      }
    }
  }


  val usage = """
    Usage: DecisionTreeRunner <master>[slices] --algo <Classification,
    Regression> --trainDataDir path --testDataDir path --maxDepth num [--impurity <Gini,Entropy,
    Variance>] [--maxBins num]
              """


  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println(usage)
      System.exit(1)
    }

    val sc = new SparkContext(args(0), "DecisionTree")

    val argList = args.toList.drop(1)
    type OptionMap = Map[Symbol, Any]

    def nextOption(map : OptionMap, list: List[String]): OptionMap = {
      list match {
        case Nil => map
        case "--algo" :: string :: tail => nextOption(map ++ Map('algo -> string), tail)
        case "--impurity" :: string :: tail => nextOption(map ++ Map('impurity -> string), tail)
        case "--maxDepth" :: string :: tail => nextOption(map ++ Map('maxDepth -> string), tail)
        case "--maxBins" :: string :: tail => nextOption(map ++ Map('maxBins -> string), tail)
        case "--trainDataDir" :: string :: tail => nextOption(map ++ Map('trainDataDir -> string)
          , tail)
        case "--testDataDir" :: string :: tail => nextOption(map ++ Map('testDataDir -> string),
          tail)
        case string :: Nil =>  nextOption(map ++ Map('infile -> string), list.tail)
        case option :: tail => logError("Unknown option " + option)
          sys.exit(1)
      }
    }
    val options = nextOption(Map(),argList)
    logDebug(options.toString())

    // Load training data
    val trainData = loadLabeledData(sc, options.get('trainDataDir).get.toString)

    // Identify the type of algorithm
    val algoStr =  options.get('algo).get.toString
    val algo = algoStr match {
      case "Classification" => Classification
      case "Regression" => Regression
    }

    // Identify the type of impurity
    val impurityStr = options.getOrElse('impurity,
      if (algo == Classification) "Gini" else "Variance").toString
    val impurity = impurityStr match {
      case "Gini" => Gini
      case "Entropy" => Entropy
      case "Variance" => Variance
    }

    val maxDepth = options.getOrElse('maxDepth,"1").toString.toInt
    val maxBins = options.getOrElse('maxBins,"100").toString.toInt

    val strategy = new Strategy(algo, impurity, maxDepth, maxBins)
    val model = DecisionTree.train(trainData, strategy)

    // Load test data
    val testData = loadLabeledData(sc, options.get('testDataDir).get.toString)

    // Measure algorithm accuracy
    if (algo == Classification){
      val accuracy = accuracyScore(model, testData)
      logDebug("accuracy = " + accuracy)
    }

    if (algo == Regression){
      val mse = meanSquaredError(model, testData)
      logDebug("mean square error = " + mse)
    }

    sc.stop()
  }

  /**
   * Load labeled data from a file. The data format used here is
   * <L>, <f1> <f2> ...
   * where <f1>, <f2> are feature values in Double and <L> is the corresponding label as Double.
   *
   * @param sc SparkContext
   * @param dir Directory to the input data files.
   * @return An RDD of LabeledPoint. Each labeled point has two elements: the first element is
   *         the label, and the second element represents the feature values (an array of Double).
   */
  def loadLabeledData(sc: SparkContext, dir: String): RDD[LabeledPoint] = {
    sc.textFile(dir).map { line =>
      val parts = line.trim().split(",")
      val label = parts(0).toDouble
      val features = parts.slice(1,parts.length).map(_.toDouble)
      LabeledPoint(label, features)
    }
  }

  // TODO: Port this method to a generic metrics package
  /**
   * Calculates the classifier accuracy.
   */
  def accuracyScore(model: DecisionTreeModel, data: RDD[LabeledPoint]): Double = {
    val correctCount = data.filter(y => model.predict(y.features) == y.label).count()
    val count = data.count()
    logDebug("correct prediction count = " +  correctCount)
    logDebug("data count = " + count)
    correctCount.toDouble / count
  }

  // TODO: Port this method to a generic metrics package
  /**
   * Calculates the mean squared error for regression
   */
  def meanSquaredError(tree: DecisionTreeModel, data: RDD[LabeledPoint]): Double = {
    data.map(y => (tree.predict(y.features) - y.label)*(tree.predict(y.features) - y.label)).mean()
  }
}
