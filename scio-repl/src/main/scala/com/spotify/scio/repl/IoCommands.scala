/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.repl

import java.io._
import java.net.URI
import java.net.URLClassLoader
import java.nio.channels.Channels
import java.util.jar.{JarEntry, JarOutputStream}

import com.google.cloud.dataflow.sdk.options.PipelineOptions
import com.google.cloud.dataflow.sdk.util.GcsUtil
import com.google.cloud.dataflow.sdk.util.GcsUtil.GcsUtilFactory
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath
import com.google.common.io.Files
import com.spotify.scio.util.ScioUtil
import kantan.csv.{RowDecoder, RowEncoder}
import org.apache.avro.compiler.specific.SpecificCompiler
import org.apache.avro.file.{DataFileStream, DataFileWriter}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter, SpecificRecordBase}
import org.apache.commons.io.{Charsets, IOUtils}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.eclipse.jdt.core.compiler.batch.BatchCompiler

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/** Commands for simple file I/O in the REPL. */
class IoCommands(options: PipelineOptions) {

  private val logger: Logger = LoggerFactory.getLogger(classOf[IoCommands])

  private val TEXT = "text/plain"
  private val BINARY = "application/octet-stream"

  // TODO: figure out how to support HDFS without messing up dependencies
  private val gcsUtil: GcsUtil = new GcsUtilFactory().create(options)
  private val fs = FileSystem.get(new Configuration())

  // =======================================================================
  // Read operations
  // =======================================================================

  /** Read from an Avro file on local filesystem, GCS, or HDFS. */
  def readAvro[T : ClassTag](path: String): Iterator[T] = {
    val cls = ScioUtil.classOf[T]
    val reader = if (classOf[SpecificRecordBase] isAssignableFrom cls) {
      new SpecificDatumReader[T]()
    } else {
      new GenericDatumReader[T]()
    }
    new DataFileStream[T](inputStream(path), reader).iterator().asScala
  }

  /** Read from a text file on local filesystem, GCS, or HDFS. */
  def readText(path: String): Iterator[String] =
    IOUtils.lineIterator(inputStream(path), Charsets.UTF_8).asScala

  /** Read from a CSV file on local filesystem, GCS, or HDFS. */
  def readCsv[T: RowDecoder](path: String,
                             sep: Char = ',',
                             header: Boolean = false): Iterator[T] = {
    import kantan.csv.ops._
    implicit val codec = scala.io.Codec.UTF8
    inputStream(path).asUnsafeCsvReader[T](sep, header).toIterator
  }

  /** Read from a TSV file on local filesystem, GCS, or HDFS. */
  def readTsv[T: RowDecoder](path: String,
                             sep: Char = '\t',
                             header: Boolean = false): Iterator[T] = {
    import kantan.csv.ops._
    implicit val codec = scala.io.Codec.UTF8
    inputStream(path).asUnsafeCsvReader[T](sep, header).toIterator
  }

  // =======================================================================
  // Write operations
  // =======================================================================

  private def plural[T](data: Seq[T]): String = if (data.size > 1) "s" else ""

  /** Write to an Avro file on local filesystem, GCS, or HDFS. */
  def writeAvro[T: ClassTag](path: String, data: Seq[T]): Unit = {
    val cls = ScioUtil.classOf[T]
    val (writer, schema) = if (classOf[SpecificRecordBase] isAssignableFrom cls) {
      (new SpecificDatumWriter[T](cls), data.head.asInstanceOf[SpecificRecordBase].getSchema)
    } else {
      (new GenericDatumWriter[T](), data.head.asInstanceOf[GenericRecord].getSchema)
    }
    val fileWriter = new DataFileWriter[T](writer).create(schema, outputStream(path, BINARY))
    data.foreach(fileWriter.append)
    fileWriter.close()
    logger.info("{} record{} written to {}", Array(data.size, plural(data), path))
  }

  /** Write to a text file on local filesystem, GCS, or HDFS. */
  def writeText(path: String, data: Seq[String]): Unit = {
    IOUtils.writeLines(data.asJava, IOUtils.LINE_SEPARATOR, outputStream(path, TEXT))
    logger.info("{} line{} written to {}", Array(data.size, plural(data), path))
  }

  /** Write to a CSV file on local filesystem, GCS, or HDFS. */
  def writeCsv[T: RowEncoder](path: String, data: Seq[T],
                              sep: Char = ',',
                              header: Seq[String] = Seq.empty): Unit = {
    import kantan.csv.ops._
    IOUtils.write(data.asCsv(sep, header), outputStream(path, TEXT))
    logger.info("{} line{} written to {}", Array(data.size, plural(data), path))
  }

  /** Write to a TSV file on local filesystem, GCS, or HDFS. */
  def writeTsv[T: RowEncoder](path: String, data: Seq[T],
                              sep: Char = '\t',
                              header: Seq[String] = Seq.empty): Unit = {
    import kantan.csv.ops._
    IOUtils.write(data.asCsv(sep, header), outputStream(path, TEXT))
    logger.info("{} line{} written to {}", Array(data.size, plural(data), path))
  }

  // =======================================================================
  // Utilities
  // =======================================================================

  private def inputStream(path: String): InputStream = {
    val uri = new URI(path)
    if (ScioUtil.isGcsUri(uri)) {
      Channels.newInputStream(gcsUtil.open(GcsPath.fromUri(uri)))
    } else if (ScioUtil.isLocalUri(uri)) {
      new FileInputStream(path)
    } else if (uri.getScheme == "hdfs") {
      fs.open(new Path(path))
    } else {
      throw new IllegalArgumentException(s"Unsupported path $path")
    }
  }

  private def outputStream(path: String, contentType: String): OutputStream = {
    val uri = new URI(path)
    if (ScioUtil.isGcsUri(uri)) {
      Channels.newOutputStream(gcsUtil.create(GcsPath.fromUri(uri), contentType))
    } else if (ScioUtil.isLocalUri(uri)) {
      new FileOutputStream(path)
    } else if (uri.getScheme == "hdfs") {
      fs.create(new Path(path), false)
    } else {
      throw new IllegalArgumentException(s"Unsupported path $path")
    }
  }

  // =======================================================================
  // Avro
  // =======================================================================

  def avroSchemaJar(schemaFile: String): File = {
    val tmpDir = Files.createTempDir()

    val javaDir = new File(tmpDir, "java")
    javaDir.mkdirs()
    SpecificCompiler.compileSchema(new File(schemaFile), javaDir)

    val cp = getClassPath().mkString(":")
    val targetDir = new File(tmpDir, "target")
    targetDir.mkdirs()
    BatchCompiler.compile(
      s"-1.7 -cp $cp $javaDir -d $targetDir",
      new PrintWriter(System.out), new PrintWriter(System.err),
      null)

    val jarFile = new File(tmpDir, "avro.jar")
    val jarOut = new JarOutputStream(new FileOutputStream(jarFile))
    val root = targetDir.toPath
    Files.fileTreeTraverser().preOrderTraversal(targetDir).asScala
      .filter(_.isFile)
      .foreach { f =>
        jarOut.putNextEntry(new JarEntry(root.relativize(f.toPath).toString))
        jarOut.write(Files.toByteArray(f))
        jarOut.closeEntry()
      }
    jarOut.close()
    jarFile
  }

  def avroSchemaFile(path: String): File = {
    val input = inputStream(path)
    val reader = new GenericDatumReader[Void]()
    val schema = new DataFileStream[Void](input, reader).getSchema
    val schemaFile = new File(Files.createTempDir(), "schema.avsc")
    Files.write(schema.toString(true), schemaFile, Charsets.UTF_8)
    schemaFile
  }

  private def getClassPath(): Array[String] = {
    val javaHome = new File(sys.props("java.home")).getCanonicalPath
    val userDir = new File(sys.props("user.dir")).getCanonicalPath

    this.getClass.getClassLoader.asInstanceOf[URLClassLoader]
      .getURLs
      .map(url => new File(url.toURI).getCanonicalPath)
      .filter(p => !p.startsWith(javaHome) && p != userDir)
  }

}
