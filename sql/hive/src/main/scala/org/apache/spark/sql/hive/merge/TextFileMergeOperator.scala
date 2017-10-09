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

package org.apache.spark.sql.hive.merge

import java.io.{DataOutputStream, InputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.AbstractFileMergeOperator
import org.apache.hadoop.hive.ql.plan.FileMergeDesc
import org.apache.hadoop.hive.ql.plan.api.OperatorType
import org.apache.hadoop.io.compress.{CodecPool, CompressionCodec, CompressionCodecFactory, SplittableCompressionCodec}
import org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE

import org.apache.spark.internal.Logging
import org.apache.spark.SparkException

class TextFileMergeOperator(conf: Configuration)
    extends AbstractFileMergeOperator[FileMergeDesc] with Logging {

  override def getType: OperatorType = {
    OperatorType.ORCFILEMERGE
  }

  private var codec: CompressionCodec = _
  private var output: DataOutputStream = _

  def getRecordWriter(): DataOutputStream = {
    if (codec == null) {
      fs.create(outPath)
    } else {
      new DataOutputStream(codec.createOutputStream(fs.create(outPath)))
    }
  }

  override def process(row: scala.Any, tag: Int): Unit = {
    val keyValue = row.asInstanceOf[Array[AnyRef]]
    val file = keyValue(0).asInstanceOf[StringWrapper].file
    val filePath = new Path(file)
    if (codec == null) {
      codec = new CompressionCodecFactory(conf).getCodec(filePath)
    }
    if (output == null)  {
      output = getRecordWriter()
    }
    val length = fs.getFileStatus(filePath).getLen
    var inputStream = getInputStream(filePath, length)
    val buffer = new Array[Byte](1024 * 1024)
    var len = -1
    try {
      do {
        len = inputStream.read(buffer)
        if (len > 0) {
          output.write(buffer, 0, len)
        }
      } while (len > 0)
    } finally {
      if (inputStream != null) {
        inputStream.close()
      }
      inputStream = null
    }
  }

  def getInputStream(file: Path, length: Long): InputStream = {
    val dfsInputStream = fs.open(file)
    if (codec != null) {
      val decompressor = CodecPool.getDecompressor(codec)
      if (codec.isInstanceOf[SplittableCompressionCodec]) {
        return codec.asInstanceOf[SplittableCompressionCodec].createInputStream(dfsInputStream,
          decompressor, 0, length, READ_MODE.BYBLOCK)
      } else {
        return codec.createInputStream(dfsInputStream, decompressor)
      }
    }
    return dfsInputStream
  }

  override def closeOp(abort: Boolean): Unit = {
    try {
      if (output != null) {
        output.close()
        output = null
      }
    } catch {
      case e: Exception =>
        throw new SparkException("Unable to close TextFileMergeOperator", e)
    }
    super.closeOp(abort)
  }
}
