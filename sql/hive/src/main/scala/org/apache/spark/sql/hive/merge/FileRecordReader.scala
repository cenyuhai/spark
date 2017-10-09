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

import org.apache.hadoop.mapred.RecordReader

case class StringWrapper(var file: String)

class FileRecordReader(file: String) extends RecordReader[StringWrapper, StringWrapper] {
  var isClose = false

  override def next(k: StringWrapper, v: StringWrapper): Boolean = {
    if (!isClose) {
      k.file = file
      v.file = file
      isClose = true
      true
    } else {
      false
    }
  }

  override def getProgress: Float = if (isClose) 1 else 0

  override def getPos: Long = if (isClose) 1 else 0

  override def createKey(): StringWrapper = StringWrapper(file)

  override def close(): Unit = {}

  override def createValue(): StringWrapper = StringWrapper(file)
}
