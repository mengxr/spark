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

package org.apache.spark.examples.mllib

import org.apache.spark.SparkConf
import org.apache.spark.mllib.stat.test.{TTestResult, StreamingTTest}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StreamingContext, Seconds}

/**
 * Perform a streaming t-test on data from two populations. These populations could
 * correspond to treatment and control in an A/B test, for example. The results of
 * the t-test are continually updated as data is provided.
 *
 * The t-test performed is Welch's two-sample t-test, intended to be used on
 * populations where all observations are independent and Normally distributed,
 * and are identically distributed within populations. The null hypothesis is that
 * the two populations have the same mean. The test does not assume the populations
 * have equal variances.
 *
 * The testing will terminate if the p-value ever drops below 0.01.
 *
 * Usage:
 * StreamingABTestExample <testResultsDir> <batchDuration> <windowLength> <peacePeriod> <maxBatches>
 *
 * First argument:
 * The path where the data will be deposited. The files in this path must have one row for each
 * observation. Each row should have the format
 * group,value
 * where 'group' is either 1 or 0, and 'value' is a Double.
 * Example input file:
 * 1,10.0
 * 1,10.3
 * 0,-10.0
 * 0,-10.3
 *
 * Second argument:
 * Time duration in seconds that specifies how often the test results will be updated. Must be
 * a positive integer.
 *
 * Third argument (optional):
 * Window length n.  If provided, t-tests are performed on a sliding window of n seconds. n must
 * be a positive integer. If set to 0, no windowing is performed.
 *
 * Fourth argument (optional):
 * peace period duration n. If provided, the t-test will ignore
 * the n initial batches. If the window length is non-zero, then this peace period
 * is ignored. Must be a positive integer. If set to 0, no peace period is used.
 *
 * Fifth argument (optional):
 * max number of batches n.  If provided, testing will terminate after n batches. Must be a
 * positive integer.
 *
 * For example:
 *  $bin/run-example mllib.StreamingABTestExample ~/AB_test_results 20 0 5 300
 *
 * will update the test results every 20 seconds with any files provided
 * in the location ~/AB_test_results/, will ignore the first 100 seconds of data,
 * and will terminate after 100 minutes (20 sec per bach * 300 batches = 100 minutes)
 */

object StreamingABTestExample {
  def printUsageAndQuit(): Unit = {
    System.err.println(
      "Usage: StreamingABTestExample <testResultsDir> <batchDuration> <windowLength> <peacePeriod>")
    System.exit(1)
  }

  def main(args: Array[String]) = {

    // Parse arguments
    if (args.length < 2) {
      printUsageAndQuit()
    }

    val batchLength: Long = try { args(1).toLong } catch { case _ => {
      System.err.println("Unable to parse batch duration: " + args(1) + " is not an integer")
      printUsageAndQuit()
      0
    }}

    val windowLength: Long =
      if (args.length < 3) {
        0
      } else {
        try { args(2).toLong } catch { case _ => {
          System.err.println("Unable to parse window length: " + args(2) + " is not an integer")
          printUsageAndQuit()
          0
        }}
      }

    val peacePeriod: Long =
      if (args.length < 4) {
        0
      } else {
        try { args(3).toLong } catch { case _ => {
          System.err.println("Unable to parse peace period: " + args(3) + " is not an integer")
          printUsageAndQuit()
          0
        }}
      }

    val maxBatches: Long =
      if (args.length < 5) {
        0
      } else {
        try { args(4).toLong } catch { case _ => {
          System.err.println("Unable to parse max batches: " + args(4) + " is not an integer")
          printUsageAndQuit()
          0
        }}
      }

    val conf = new SparkConf().setMaster("local").setAppName("StreamingABTestExample")
    val ssc = new StreamingContext(conf, Seconds(batchLength))

    val trainingData: DStream[(Boolean,Double)] = ssc.textFileStream(args(0)).map(line => {

      // Parse input data and check format
      val lineVals = line.split(",")
      if (lineVals.length != 2) {
        System.err.println("Each line of input must have two comma-separated fields, eg.")
        System.err.println("1,2.0")
        System.err.println("0,3.0")
        System.exit(1)
      }

      val group: Boolean = lineVals(0) match {
        case "0" => false
        case "1" => true
        case x => {
          System.err.println("Unrecognized treatment group: " + x)
          System.err.println("Treatment group must be 1 or 0")
          System.exit(1)
          false
        }
      }

      val outcome = try { Some(lineVals(1).toDouble) } catch { case _ => None }
      if (outcome == None) {
        System.err.println("Unable to parse outcome" + lineVals(1))
        System.exit(1)
      }

      (group, outcome.get)
    })

    val test = new StreamingTTest(windowLength = Seconds(windowLength), peacePeriod = peacePeriod)

    // Get test results
    val testDStream: DStream[TTestResult] = test.getTestResults(trainingData)
    testDStream.print()

    // To store the number of batches processed
    var nBatches = 0

    // After each batch, determine if any conditions for termination have been satisfied
    testDStream.foreachRDD((rdd: RDD[TTestResult]) => {
      nBatches = nBatches + 1
      // Check if we have reached max_batches
      if (maxBatches > 0 && nBatches > maxBatches) {
        ssc.stop(true)
      }
      // Check if we have reached significance
      val allResults = rdd.collect()
      allResults.foreach(result => {
        if (result.pValue < 0.01) {
          ssc.stop(true)
          1
        } else {
          0
        }
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
