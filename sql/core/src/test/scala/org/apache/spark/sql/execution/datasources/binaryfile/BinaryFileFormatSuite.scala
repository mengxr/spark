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

package org.apache.spark.sql.execution.datasources.binaryfile

import java.io.File
import java.nio.file.{Files, StandardOpenOption}
import java.sql.Timestamp

import scala.collection.JavaConverters._

import com.google.common.io.{ByteStreams, Closeables}
import org.apache.hadoop.fs.{FileStatus, FileSystem, GlobFilter, Path}
import org.mockito.Mockito.{mock, when}

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources._
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}
import org.apache.spark.util.Utils

class BinaryFileFormatSuite extends QueryTest with SharedSQLContext with SQLTestUtils {
  import BinaryFileFormat._

  private var testDir: String = _

  private var fsTestDir: Path = _

  private var fs: FileSystem = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    testDir = Utils.createTempDir().getAbsolutePath
    fsTestDir = new Path(testDir)
    fs = fsTestDir.getFileSystem(sparkContext.hadoopConfiguration)

    val year2014Dir = new File(testDir, "year=2014")
    year2014Dir.mkdir()
    val year2015Dir = new File(testDir, "year=2015")
    year2015Dir.mkdir()

    val file1 = new File(year2014Dir, "data.txt")
    Files.write(
      file1.toPath,
      Seq("2014-test").asJava,
      StandardOpenOption.CREATE, StandardOpenOption.WRITE
    )

    val file2 = new File(year2014Dir, "data2.bin")
    Files.write(
      file2.toPath,
      "2014-test-bin".getBytes,
      StandardOpenOption.CREATE, StandardOpenOption.WRITE
    )

    val file3 = new File(year2015Dir, "bool.csv")
    Files.write(
      file3.toPath,
      Seq("bool", "True", "False", "true").asJava,
      StandardOpenOption.CREATE, StandardOpenOption.WRITE
    )

    val file4 = new File(year2015Dir, "data.bin")
    Files.write(
      file4.toPath,
      "2015-test".getBytes,
      StandardOpenOption.CREATE, StandardOpenOption.WRITE
    )
  }

  def testBinaryFileDataSource(pathGlobFilter: String): Unit = {
    val dfReader = spark.read.format("binaryFile")
    if (pathGlobFilter != null) {
      dfReader.option("pathGlobFilter", pathGlobFilter)
    }
    val resultDF = dfReader.load(testDir).select(
        col(PATH),
        col(MODIFICATION_TIME),
        col(LENGTH),
        col(CONTENT),
        col("year") // this is a partition column
      )

    val expectedRowSet = new collection.mutable.HashSet[Row]()

    val globFilter = if (pathGlobFilter == null) null else new GlobFilter(pathGlobFilter)
    for (partitionDirStatus <- fs.listStatus(fsTestDir)) {
      val dirPath = partitionDirStatus.getPath

      val partitionName = dirPath.getName.split("=")(1)
      val year = partitionName.toInt // partition column "year" value which is `Int` type

      for (fileStatus <- fs.listStatus(dirPath)) {
        if (globFilter == null || globFilter.accept(fileStatus.getPath)) {
          val fpath = fileStatus.getPath.toString.replace("file:/", "file:///")
          val flen = fileStatus.getLen
          val modificationTime = new Timestamp(fileStatus.getModificationTime)

          val fcontent = {
            val stream = fs.open(fileStatus.getPath)
            val content = try {
              ByteStreams.toByteArray(stream)
            } finally {
              Closeables.close(stream, true)
            }
            content
          }

          val row = Row(fpath, modificationTime, flen, fcontent, year)
          expectedRowSet.add(row)
        }
      }
    }

    checkAnswer(resultDF, expectedRowSet.toSeq)
  }

  test("binary file data source test") {
    testBinaryFileDataSource(null)
    testBinaryFileDataSource("*.*")
    testBinaryFileDataSource("*.bin")
    testBinaryFileDataSource("*.txt")
    testBinaryFileDataSource("*.{txt,csv}")
    testBinaryFileDataSource("*.json")
  }

  test("binary file data source do not support write operation") {
    val df = spark.read.format("binaryFile").load(testDir)
    withTempDir { tmpDir =>
      val thrown = intercept[UnsupportedOperationException] {
        df.write
          .format("binaryFile")
          .save(tmpDir + "/test_save")
      }
      assert(thrown.getMessage.contains("Write is not supported for binary file data source"))
    }
  }

  def mockFileStatus(length: Long, modificationTime: Long): FileStatus = {
    val status = mock(classOf[FileStatus])
    when(status.getLen).thenReturn(length)
    when(status.getModificationTime).thenReturn(modificationTime)
    when(status.toString).thenReturn(
      s"FileStatus($LENGTH=$length, $MODIFICATION_TIME=$modificationTime)")
    status
  }

  def testCreateFilterFunction(
      filters: Seq[Filter],
      testCases: Seq[(FileStatus, Boolean)]): Unit = {
    val funcs = filters.map(BinaryFileFormat.createFilterFunction)
    testCases.foreach { case (status, expected) =>
      assert(funcs.forall(f => f(status)) === expected,
        s"$filters applied to $status should be $expected.")
    }
  }

  test("createFilterFunction") {
    // test filter applied on `length` column
    val l1 = mockFileStatus(1L, 0L)
    val l2 = mockFileStatus(2L, 0L)
    val l3 = mockFileStatus(3L, 0L)
    testCreateFilterFunction(
      Seq(LessThan(LENGTH, 2L)),
      Seq((l1, true), (l2, false), (l3, false)))
    testCreateFilterFunction(
      Seq(LessThanOrEqual(LENGTH, 2L)),
      Seq((l1, true), (l2, true), (l3, false)))
    testCreateFilterFunction(
      Seq(GreaterThan(LENGTH, 2L)),
      Seq((l1, false), (l2, false), (l3, true)))
    testCreateFilterFunction(
      Seq(GreaterThanOrEqual(LENGTH, 2L)),
      Seq((l1, false), (l2, true), (l3, true)))
    testCreateFilterFunction(
      Seq(EqualTo(LENGTH, 2L)),
      Seq((l1, false), (l2, true), (l3, false)))
    testCreateFilterFunction(
      Seq(Not(EqualTo(LENGTH, 2L))),
      Seq((l1, true), (l2, false), (l3, true)))
    testCreateFilterFunction(
      Seq(And(GreaterThan(LENGTH, 1L), LessThan(LENGTH, 3L))),
      Seq((l1, false), (l2, true), (l3, false)))
    testCreateFilterFunction(
      Seq(Or(LessThanOrEqual(LENGTH, 1L), GreaterThanOrEqual(LENGTH, 3L))),
      Seq((l1, true), (l2, false), (l3, true)))

    // test filter applied on `length` column
    val t1 = mockFileStatus(0L, 1L)
    val t2 = mockFileStatus(0L, 2L)
    val t3 = mockFileStatus(0L, 3L)
    testCreateFilterFunction(
      Seq(LessThan(MODIFICATION_TIME, new Timestamp(2L))),
      Seq((t1, true), (t2, false), (t3, false)))
    testCreateFilterFunction(
      Seq(LessThanOrEqual(MODIFICATION_TIME, new Timestamp(2L))),
      Seq((t1, true), (t2, true), (t3, false)))
    testCreateFilterFunction(
      Seq(GreaterThan(MODIFICATION_TIME, new Timestamp(2L))),
      Seq((t1, false), (t2, false), (t3, true)))
    testCreateFilterFunction(
      Seq(GreaterThanOrEqual(MODIFICATION_TIME, new Timestamp(2L))),
      Seq((t1, false), (t2, true), (t3, true)))
    testCreateFilterFunction(
      Seq(EqualTo(MODIFICATION_TIME, new Timestamp(2L))),
      Seq((t1, false), (t2, true), (t3, false)))
    testCreateFilterFunction(
      Seq(Not(EqualTo(MODIFICATION_TIME, new Timestamp(2L)))),
      Seq((t1, true), (t2, false), (t3, true)))
    testCreateFilterFunction(
      Seq(And(GreaterThan(MODIFICATION_TIME, new Timestamp(1L)),
        LessThan(MODIFICATION_TIME, new Timestamp(3L)))),
      Seq((t1, false), (t2, true), (t3, false)))
    testCreateFilterFunction(
      Seq(Or(LessThanOrEqual(MODIFICATION_TIME, new Timestamp(1L)),
        GreaterThanOrEqual(MODIFICATION_TIME, new Timestamp(3L)))),
      Seq((t1, true), (t2, false), (t3, true)))

    // test filters applied on both columns
    testCreateFilterFunction(
      Seq(And(GreaterThan(LENGTH, 2L), LessThan(MODIFICATION_TIME, new Timestamp(2L)))),
      Seq((l1, false), (l2, false), (l3, true), (t1, false), (t2, false), (t3, false)))

    // test nested filters
    testCreateFilterFunction(
      // NOT (length > 2 OR modificationTime < 2)
      Seq(Not(Or(GreaterThan(LENGTH, 2L), LessThan(MODIFICATION_TIME, new Timestamp(2L))))),
      Seq((l1, false), (l2, false), (l3, false), (t1, false), (t2, true), (t3, true)))
  }
}
