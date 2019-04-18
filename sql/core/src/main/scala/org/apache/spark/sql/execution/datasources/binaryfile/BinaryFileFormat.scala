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

import java.sql.Timestamp

import com.google.common.io.{ByteStreams, Closeables}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, GlobFilter, Path}
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{And, DataSourceRegister, Filter, GreaterThan, GreaterThanOrEqual,
  LessThan, LessThanOrEqual}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration


/**
 * The binary file data source.
 *
 * It reads binary files and converts each file into a single record that contains the raw content
 * and metadata of the file.
 *
 * Example:
 * {{{
 *   // Scala
 *   val df = spark.read.format("binaryFile")
 *     .option("pathGlobFilter", "*.png")
 *     .load("/path/to/fileDir")
 *
 *   // Java
 *   Dataset<Row> df = spark.read().format("binaryFile")
 *     .option("pathGlobFilter", "*.png")
 *     .load("/path/to/fileDir");
 * }}}
 */
class BinaryFileFormat extends FileFormat with DataSourceRegister {

  import BinaryFileFormat._

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = Some(schema)

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    throw new UnsupportedOperationException("Write is not supported for binary file data source")
  }

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
    false
  }

  override def shortName(): String = "binaryFile"

  override protected def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {

    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    val binaryFileSourceOptions = new BinaryFileSourceOptions(options)

    val pathGlobPattern = binaryFileSourceOptions.pathGlobFilter

    val filterFuncs = filters.flatMap(createFilterFunctions(_))

    (file: PartitionedFile) => {
      val path = file.filePath
      val fsPath = new Path(path)

      // TODO: Improve performance here: each file will recompile the glob pattern here.
      val globFilter = pathGlobPattern.map(new GlobFilter(_))
      if (!globFilter.isDefined || globFilter.get.accept(fsPath)) {
        val fs = fsPath.getFileSystem(broadcastedHadoopConf.value.value)
        val fileStatus = fs.getFileStatus(fsPath)
        val length = fileStatus.getLen()
        val modificationTime = fileStatus.getModificationTime()

        if (filterFuncs.forall(_.apply(fileStatus))) {
          val stream = fs.open(fsPath)
          val content = try {
            ByteStreams.toByteArray(stream)
          } finally {
            Closeables.close(stream, true)
          }

          val fullOutput = dataSchema.map { f =>
            AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()
          }
          val requiredOutput = fullOutput.filter { a =>
            requiredSchema.fieldNames.contains(a.name)
          }

          // TODO: Add column pruning
          // currently it still read the file content even if content column is not required.
          val requiredColumns = GenerateUnsafeProjection.generate(requiredOutput, fullOutput)

          val internalRow = InternalRow(
            UTF8String.fromString(path),
            DateTimeUtils.fromMillis(modificationTime),
            length,
            content
          )

          Iterator(requiredColumns(internalRow))
        } else {
          Iterator.empty
        }
      } else {
        Iterator.empty
      }
    }
  }
}

object BinaryFileFormat {

  /**
   * Schema for the binary file data source.
   *
   * Schema:
   *  - path (StringType): The path of the file.
   *  - modificationTime (TimestampType): The modification time of the file.
   *    In some Hadoop FileSystem implementation, this might be unavailable and fallback to some
   *    default value.
   *  - length (LongType): The length of the file in bytes.
   *  - content (BinaryType): The content of the file.
   */
  val schema = StructType(
    StructField("path", StringType, false) ::
    StructField("modificationTime", TimestampType, false) ::
    StructField("length", LongType, false) ::
    StructField("content", BinaryType, true) :: Nil)

  private[binaryfile] def createFilterFunctions(filter: Filter): Seq[FileStatus => Boolean] = {
    filter match {
      case andFilter: And =>
        createFilterFunctions(andFilter.left) ++ createFilterFunctions(andFilter.right)
      case LessThan("length", value: Long) =>
        Seq((status: FileStatus) => status.getLen < value)
      case LessThan("modificationTime", value: Timestamp) =>
        Seq((status: FileStatus) => status.getModificationTime < value.getTime)
      case LessThanOrEqual("length", value: Long) =>
        Seq((status: FileStatus) => status.getLen <= value)
      case LessThanOrEqual("modificationTime", value: Timestamp) =>
        Seq((status: FileStatus) => status.getModificationTime <= value.getTime)
      case GreaterThan("length", value: Long) =>
        Seq((status: FileStatus) => status.getLen > value)
      case GreaterThan("modificationTime", value: Timestamp) =>
        Seq((status: FileStatus) => status.getModificationTime > value.getTime)
      case GreaterThanOrEqual("length", value: Long) =>
        Seq((status: FileStatus) => status.getLen >= value)
      case GreaterThanOrEqual("modificationTime", value: Timestamp) =>
        Seq((status: FileStatus) => status.getModificationTime >= value.getTime)
      case _ => Seq.empty
    }
  }
}

class BinaryFileSourceOptions(
    @transient private val parameters: CaseInsensitiveMap[String]) extends Serializable {

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  /**
   * An optional glob pattern to only include files with paths matching the pattern.
   * The syntax follows [[org.apache.hadoop.fs.GlobFilter]].
   */
  val pathGlobFilter: Option[String] = parameters.get("pathGlobFilter")
}
