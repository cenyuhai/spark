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

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.AbstractFileMergeOperator
import org.apache.hadoop.hive.ql.plan.FileMergeDesc
import org.apache.hadoop.hive.ql.plan.api.OperatorType
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetWriter}

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.parquet.ParquetSchemaConverter
import org.apache.spark.sql.types.StructType

class ParquetFileMergeOperator(conf: Configuration, schema: StructType)
    extends AbstractFileMergeOperator[FileMergeDesc] with Logging {

  override def getType: OperatorType = {
    OperatorType.ORCFILEMERGE
  }

  val blockSize = conf.getLong("parquet.dfs.blocksize", 256 * 1024 * 1024L)

  var writer: ParquetFileWriter = null

  def getRecordWriter(conf: Configuration): ParquetFileWriter = {
    val messageType = new ParquetSchemaConverter(conf).convert(schema)
    new ParquetFileWriter(conf, messageType,
      outPath, ParquetFileWriter.Mode.CREATE,
      blockSize, ParquetWriter.MAX_PADDING_SIZE_DEFAULT)
  }

  override def process(row: scala.Any, tag: Int): Unit = {
    val keyValue = row.asInstanceOf[Array[AnyRef]]
    val file = keyValue(0).asInstanceOf[StringWrapper].file
    if (writer == null) {
      writer = getRecordWriter(conf)
      writer.start()
    }
    writer.appendFile(conf, new Path(file))
  }

  override def closeOp(abort: Boolean): Unit = {
    try {
      if (writer != null) {
        writer.end(new util.HashMap[String, String]())
        writer = null
      }
    } catch {
      case e: Exception =>
        throw new SparkException("Unable to close ParquetFileMergeOperator", e)
    }
    super.closeOp(abort)
  }
}
