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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.io.RCFileInputFormat
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapred.lib.{CombineFileRecordReaderWrapper, _}

class CombineRCFileInputFormat extends CombineFileInputFormat[LongWritable, BytesRefArrayWritable] {
  def getRecordReader(split: InputSplit, conf: JobConf,
      reporter: Reporter): RecordReader[LongWritable, Text] = {
    new CombineFileRecordReader[_, _](conf, split.asInstanceOf[CombineFileSplit],
      reporter, classOf[CombineTextInputFormat.RCFileRecordReaderWrapper])
  }

  private class RCFileRecordReaderWrapper(val split: CombineFileSplit, val conf: Configuration,
      val reporter: Reporter, val idx: Integer)
      extends CombineFileRecordReaderWrapper[LongWritable, BytesRefArrayWritable](
        new RCFileInputFormat(), split, conf, reporter, idx) {
  }
}
