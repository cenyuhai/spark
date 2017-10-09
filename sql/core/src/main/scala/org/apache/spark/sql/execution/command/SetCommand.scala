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

package org.apache.spark.sql.execution.command

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}


/**
 * Command that runs
 * {{{
 *   set key = value;
 *   set -v;
 *   set;
 * }}}
 */
case class SetCommand(kv: Option[(String, Option[String])]) extends RunnableCommand with Logging {

  private def keyValueOutput: Seq[Attribute] = {
    val schema = StructType(
      StructField("key", StringType, nullable = false) ::
        StructField("value", StringType, nullable = false) :: Nil)
    schema.toAttributes
  }

  private val (_output, runFunc): (Seq[Attribute], SparkSession => Seq[Row]) = kv match {
    // Configures the deprecated "mapred.reduce.tasks" property.
    case Some((SQLConf.Deprecated.MAPRED_REDUCE_TASKS, Some(value))) =>
      val runFunc = (sparkSession: SparkSession) => {
        if (value.toInt < 1) {
          val msg =
            s"Setting negative ${SQLConf.Deprecated.MAPRED_REDUCE_TASKS} for automatically " +
              "determining the number of reducers is not supported."
          throw new IllegalArgumentException(msg)
        } else {
          sparkSession.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, value)
          Seq(Row(SQLConf.SHUFFLE_PARTITIONS.key, value))
        }
      }
      (keyValueOutput, runFunc)

    case Some((SQLConf.Replaced.MAPREDUCE_JOB_REDUCES, Some(value))) =>
      val runFunc = (sparkSession: SparkSession) => {
        if (value.toInt < 1) {
          val msg =
            s"Setting negative ${SQLConf.Replaced.MAPREDUCE_JOB_REDUCES} for automatically " +
              "determining the number of reducers is not supported."
          throw new IllegalArgumentException(msg)
        } else {
          sparkSession.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, value)
          Seq(Row(SQLConf.SHUFFLE_PARTITIONS.key, value))
        }
      }
      (keyValueOutput, runFunc)

    case Some((SQLConf.HiveVars.REDUCE_BYTES, Some(value))) =>
      val runFunc = (sparkSession: SparkSession) => {
        if (value.toInt < 1) {
          val msg =
            s"Setting negative ${SQLConf.HiveVars.REDUCE_BYTES} is illegal."
          throw new IllegalArgumentException(msg)
        } else {
          sparkSession.conf.set(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE.key, value)
          Seq(Row(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE.key, value))
        }
      }
      (keyValueOutput, runFunc)

    case Some((SQLConf.HiveVars.HIVE_DEFAULT_FILEFORMAT, Some(value))) =>
      val runFunc = (sparkSession: SparkSession) => {
        sparkSession.conf.set(SQLConf.DEFAULT_FILEFORMAT.key, value)
        Seq(Row(SQLConf.DEFAULT_FILEFORMAT.key, value))
      }
      (keyValueOutput, runFunc)

    case Some((key @ SetCommand.VariableName(name), Some(value))) =>
      val runFunc = (sparkSession: SparkSession) => {
        sparkSession.conf.set(name, value)
        Seq(Row(key, value))
      }
      (keyValueOutput, runFunc)

    // Configures a single property.
    case Some((key, Some(value))) =>
      val runFunc = (sparkSession: SparkSession) => {
        sparkSession.conf.set(key, value)
        Seq(Row(key, value))
      }
      (keyValueOutput, runFunc)

    // (In Hive, "SET" returns all changed properties while "SET -v" returns all properties.)
    // Queries all key-value pairs that are set in the SQLConf of the sparkSession.
    case None =>
      val runFunc = (sparkSession: SparkSession) => {
        sparkSession.conf.getAll.toSeq.sorted.map { case (k, v) => Row(k, v) }
      }
      (keyValueOutput, runFunc)

    // Queries all properties along with their default values and docs that are defined in the
    // SQLConf of the sparkSession.
    case Some(("-v", None)) =>
      val runFunc = (sparkSession: SparkSession) => {
        sparkSession.sessionState.conf.getAllDefinedConfs.sorted.map {
          case (key, defaultValue, doc) =>
            Row(key, Option(defaultValue).getOrElse("<undefined>"), doc)
        }
      }
      val schema = StructType(
        StructField("key", StringType, nullable = false) ::
          StructField("value", StringType, nullable = false) ::
          StructField("meaning", StringType, nullable = false) :: Nil)
      (schema.toAttributes, runFunc)

    // Queries the deprecated "mapred.reduce.tasks" property.
    case Some((SQLConf.Deprecated.MAPRED_REDUCE_TASKS, None)) =>
      val runFunc = (sparkSession: SparkSession) => {
        Seq(Row(
          SQLConf.SHUFFLE_PARTITIONS.key,
          sparkSession.sessionState.conf.numShufflePartitions.toString))
      }
      (keyValueOutput, runFunc)

    case Some((SQLConf.Replaced.MAPREDUCE_JOB_REDUCES, None)) =>
      val runFunc = (sparkSession: SparkSession) => {
        Seq(Row(
          SQLConf.SHUFFLE_PARTITIONS.key,
          sparkSession.sessionState.conf.numShufflePartitions.toString))
      }
      (keyValueOutput, runFunc)

    case Some((SQLConf.HiveVars.REDUCE_BYTES, None)) =>
      val runFunc = (sparkSession: SparkSession) => {
        Seq(Row(
          SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE.key,
          sparkSession.sessionState.conf.targetPostShuffleInputSize.toString))
      }
      (keyValueOutput, runFunc)

    // Queries a single property.
    case Some((key, None)) =>
      val runFunc = (sparkSession: SparkSession) => {
        val value = sparkSession.conf.getOption(key).getOrElse("<undefined>")
        Seq(Row(key, value))
      }
      (keyValueOutput, runFunc)
  }

  override val output: Seq[Attribute] = _output

  override def run(sparkSession: SparkSession): Seq[Row] = runFunc(sparkSession)

}

object SetCommand {
  val VariableName = """hivevar:([^=]+)""".r
}

/**
 * This command is for resetting SQLConf to the default values. Command that runs
 * {{{
 *   reset key;
 *   reset;
 * }}}
 */
case class ResetCommand(key: Option[String]) extends RunnableCommand with Logging {

  private val runFunc: (SparkSession => Unit) = key match {

    case None =>
      val runFunc = (sparkSession: SparkSession) => {
        sparkSession.sessionState.conf.clear()
      }
      runFunc

    // (In Hive, "UNSET key" clear a sepecific properties.)
    case Some(SQLConf.Deprecated.MAPRED_REDUCE_TASKS) =>
      val runFunc = (sparkSession: SparkSession) => {
        sparkSession.sessionState.conf.unsetConf(SQLConf.SHUFFLE_PARTITIONS.key)
      }
      runFunc

    case Some(SQLConf.Replaced.MAPREDUCE_JOB_REDUCES) =>
      val runFunc = (sparkSession: SparkSession) => {
        sparkSession.sessionState.conf.unsetConf(SQLConf.SHUFFLE_PARTITIONS.key)
      }
      runFunc

    case Some(SQLConf.HiveVars.REDUCE_BYTES) =>
      val runFunc = (sparkSession: SparkSession) => {
        sparkSession.sessionState.conf.unsetConf(SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE.key)
      }
      runFunc

    case Some(key) =>
      val runFunc = (sparkSession: SparkSession) => {
        sparkSession.sessionState.conf.unsetConf(key)
      }
      runFunc
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    runFunc(sparkSession)
    Seq.empty[Row]
  }
}
