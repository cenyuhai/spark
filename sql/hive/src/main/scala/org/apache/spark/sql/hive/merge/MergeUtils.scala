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

import java.io.IOException

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.common.FileUtils
import org.apache.hadoop.hive.ql.exec.{AbstractFileMergeOperator, OrcFileMergeOperator, RCFileMergeOperator}
import org.apache.hadoop.hive.ql.io.orc.OrcFileStripeMergeRecordReader
import org.apache.hadoop.hive.ql.io.rcfile.merge.RCFileBlockMergeRecordReader
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.ql.plan.{FileMergeDesc, OrcFileMergeDesc, RCFileMergeDesc}
import org.apache.hadoop.mapred.{FileSplit, RecordReader}

import org.apache.spark.{SparkContext, SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.util.SerializableJobConf

object MergeUtils extends Logging {

  val ORC = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"
  val RC = "org.apache.hadoop.hive.ql.io.RCFileOutputFormat"
  val PARQUET = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
  val TEXT = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
  val TEMP_DIR = "_tmp.-ext-10000"
  val SCHEMA = "mapred.table.schema"
  val SUPPORTED_FORMAT = Set(ORC, RC, PARQUET, TEXT)

  def getMergeFileReader[K, V](outputFormat: String, file: String, conf: Configuration)
      : RecordReader[K, V] = {
    val fs = FileSystem.get(conf)
    val fileStatus = fs.getFileStatus(new Path(file))
    outputFormat match {
      case ORC =>
        new OrcFileStripeMergeRecordReader(conf,
          new FileSplit(fileStatus.getPath, 0, fileStatus.getLen, Array.empty[String]))
          .asInstanceOf[RecordReader[K, V]]
      case RC =>
        new RCFileBlockMergeRecordReader(conf,
          new FileSplit(fileStatus.getPath, 0, fileStatus.getLen, Array.empty[String]))
          .asInstanceOf[RecordReader[K, V]]
      case PARQUET =>
        new FileRecordReader(file).asInstanceOf[RecordReader[K, V]]
      case TEXT =>
        new FileRecordReader(file).asInstanceOf[RecordReader[K, V]]
    }
  }

  def getFileMergeDesc[T <: FileMergeDesc](outputFormat: String, outputPath: Path): T = {
    val fmd = outputFormat match {
      case ORC =>
        new OrcFileMergeDesc().asInstanceOf[T]
      case RC =>
        new RCFileMergeDesc().asInstanceOf[T]
      case PARQUET =>
        new FileMergeDesc(null, outputPath).asInstanceOf[T]
      case TEXT =>
        new FileMergeDesc(null, outputPath).asInstanceOf[T]
    }
    fmd.setDpCtx(null)
    fmd.setHasDynamicPartitions(false)
    fmd.setListBucketingAlterTableConcatenate(false)
    fmd.setListBucketingDepth(0)
    fmd.setOutputPath(outputPath)
    fmd
  }

  def getFileMergeOperator[T <: FileMergeDesc](
      outputFormat: String, outputPath: Path,
      conf: Configuration): AbstractFileMergeOperator[T] = {
    val mergeOperator: AbstractFileMergeOperator[T] = outputFormat match {
      case ORC =>
        new OrcFileMergeOperator().asInstanceOf[AbstractFileMergeOperator[T]]
      case RC =>
        new RCFileMergeOperator().asInstanceOf[AbstractFileMergeOperator[T]]
      case PARQUET =>
        val schema = DataType.fromJson(conf.get(SCHEMA)).asInstanceOf[StructType]
        new ParquetFileMergeOperator(conf, schema).asInstanceOf[AbstractFileMergeOperator[T]]
      case TEXT =>
        new TextFileMergeOperator(conf).asInstanceOf[AbstractFileMergeOperator[T]]
    }
    val fmd = getFileMergeDesc[T](outputFormat, outputPath)
    mergeOperator.setConf(fmd)
    mergeOperator
  }

  def commitTask(fs: FileSystem, attemptPath: Path, taskPath: Path, retryWaitMs: Long): Unit = {
    var attempts = 0
    var lastException: Exception = null
    val maxRetries = 3
    while (attempts < maxRetries) {
      attempts += 1
      try {
        if (fs.exists(taskPath) || fs.rename(attemptPath, taskPath)) {
          return
        }
      } catch {
        case ie: InterruptedException => throw ie
        case e: Exception =>
          lastException = e
          e.printStackTrace()
      }

      if (attempts < maxRetries) {
        Thread.sleep(retryWaitMs)
      }
    }

    throw new SparkException(
      s"Error rename ${attemptPath.toString}", lastException)
  }

  def getExternalMergeTmpPath(tempPath: Path, hadoopConf: Configuration): Path = {
    val fs = tempPath.getFileSystem(hadoopConf)
    val tempMergePath = tempPath.toString.replace("-ext-10000", TEMP_DIR)
    val dir: Path = fs.makeQualified(new Path(tempMergePath))
    logDebug("Created temp merging dir = " + dir + " for path = " + tempPath)
    try {
      if (!FileUtils.mkdir(fs, dir, true, hadoopConf)) {
        throw new IllegalStateException("Cannot create staging directory  '" + dir.toString + "'")
      }
    } catch {
      case e: IOException =>
        throw new RuntimeException(
          "Cannot create temp merging directory '" + dir.toString + "': " + e.getMessage, e)
    }
    return dir
  }

  def getTargetFileNum(path: Path, conf: Configuration,
      avgConditionSize: Long, targetFileSize: Long): Int = {
    var numFiles = -1
    try {
      val fs = path.getFileSystem(conf)
      if (fs.exists(path)) {
        val averageSize = getPathSize(fs, path)
        numFiles = computeMergePartitionNum(averageSize, avgConditionSize, targetFileSize)
      }
    } catch {
      case e: IOException => log.error("get FileSystem failed!", e)
    }
    numFiles
  }

  class AverageSize(var totalSize: Long, var numFiles: Int, var files: Seq[String]) {
    def getAverageSize: Long = {
      if (numFiles != 0) {
        totalSize / numFiles
      } else {
        0
      }
    }

    override def toString: String = "{totalSize: " + totalSize + ", numFiles: " + numFiles + "}"
  }

  def getPathSize(fs: FileSystem, dirPath: Path): AverageSize = {
    val error = new AverageSize(-1, -1, Seq.empty[String])
    try {
      val fStats = fs.listStatus(dirPath).filter(_.getLen > 0)

      var totalSz: Long = 0L
      var numFiles: Int = 0

      for (fStat <- fStats) {
        if (fStat.isDirectory()) {
          val avgSzDir = getPathSize(fs, fStat.getPath)
          if (avgSzDir.totalSize < 0) {
            return error
          }
          totalSz += avgSzDir.totalSize
          numFiles += avgSzDir.numFiles
        } else {
          if (!fStat.getPath.toString.endsWith("_SUCCESS")) {
            totalSz += fStat.getLen
            numFiles += 1
          }
        }
      }
      new AverageSize(totalSz, numFiles, fStats.map(_.getPath.toString))
    } catch {
      case _: IOException => error
    }
  }

  def computeMergePartitionNum(averageSize: AverageSize, avgConditionSize: Long,
        targetFileSize: Long): Int = {
    var partitionNum = -1
    if (averageSize.numFiles <= 1) {
      partitionNum = -1
    } else {
      if (averageSize.getAverageSize > avgConditionSize) {
        partitionNum = -1
      } else {
        partitionNum = Math.ceil(averageSize.totalSize.toDouble / targetFileSize).toInt
      }
    }
    partitionNum
  }

  // get all the dynamic partition path and it's file size
  def getTmpDynamicPartPathInfo(
      fs: FileSystem,
      tmpPath: Path,
      conf: Configuration): mutable.Map[String, AverageSize] = {
    val fStatus = fs.listStatus(tmpPath)
    val partPathInfo = mutable.Map[String, AverageSize]()

    for (fStat <- fStatus) {
      if (fStat.isDirectory) {
        logDebug("[TmpDynamicPartPathInfo] path: " + fStat.getPath.toString)
        if (!hasFile(fs, fStat.getPath, conf)) {
          partPathInfo ++= getTmpDynamicPartPathInfo(fs, fStat.getPath, conf)
        } else {
          val avgSize = getPathSize(fs, fStat.getPath)
          logDebug("pathSizeMap: (" + fStat.getPath.toString + " -> " + avgSize.totalSize + ")")
          partPathInfo += (fStat.getPath.toString -> avgSize)
        }
      }
    }
    partPathInfo
  }

  def hasFile(fs: FileSystem, path: Path, conf: Configuration): Boolean = {
    val fStatus = fs.listStatus(path)
    for (fStat <- fStatus) {
      if (fStat.isFile) {
        return true
      }
    }
    false
  }

  case class DynamicMergeRule(path: String, files: Seq[String], numFiles: Int)

  def generateDynamicMergeRule(
      fs: FileSystem,
      path: Path,
      conf: Configuration,
      avgConditionSize: Long,
      targetFileSize: Long,
      directRenamePathList: java.util.List[String]): Seq[DynamicMergeRule] = {
    val tmpDynamicPartInfos = getTmpDynamicPartPathInfo(fs, path, conf)
    logDebug("[generateDynamicMergeRule] partInfo size: " + tmpDynamicPartInfos.size)
    val avgMergeSize: Long = if (!tmpDynamicPartInfos.isEmpty) {
      tmpDynamicPartInfos.map(part => part._2.totalSize).sum / tmpDynamicPartInfos.size
    } else {
      0L
    }
    logDebug("[generateDynamicMergeRule] avgMergeSize -> " + avgMergeSize)
    val mergeRule = new ArrayBuffer[DynamicMergeRule]()
    if (avgMergeSize > 0) {
      for (part <- tmpDynamicPartInfos) {
        // if the average file size is greater than the target merge size,
        // just move it to temp merge path
        if (part._2.numFiles <= 1 || part._2.getAverageSize > avgConditionSize) {
          logInfo("mark to rename [" + part._1.toString + " to "
            + part._1.toString.replace("-ext-10000", TEMP_DIR)
            + "]; number of files under " + part._1.toString + " is: " + part._2.numFiles)
          directRenamePathList.add(part._1)
        } else {
          val numFiles = computeMergePartitionNum(part._2, avgConditionSize, targetFileSize)
          mergeRule += DynamicMergeRule(part._1, part._2.files, numFiles)
        }
      }
    }
    mergeRule
  }

  def mergePathRDD(sc: SparkContext, files: Array[(String, Array[String])],
      numFiles: Int): RDD[(String, Array[String])] = {
    sc.parallelize(files, numFiles)
  }

  def mergeAction(conf: SerializableJobConf, outputClassName: String,
      files: Array[String], outputDir: String, tmpMergeLocationDir: String,
      extension: String, waitTime: Long): Unit = {
    val jobConf = conf.value
    val context = TaskContext.get()
    val taskId = "%06d".format(context.partitionId()) + "_" + context.attemptNumber()
    val fs = FileSystem.get(jobConf)
    val tmpMergeLocationPath = new Path(tmpMergeLocationDir)
    if (!fs.exists(tmpMergeLocationPath)) {
      fs.mkdirs(tmpMergeLocationPath)
    }
    val attemptPath = new Path(tmpMergeLocationDir + "/" + taskId)
    val outputPath = new Path(outputDir)
    val taskTmpPath = new Path(outputDir.replace("/-ext-10000", "/_task_tmp.-ext-10000"))
    jobConf.set("mapred.task.id", taskId)
    jobConf.set("mapred.task.path", attemptPath.toString)
    val mergeOp = getFileMergeOperator[FileMergeDesc](outputClassName, outputPath, jobConf)
    mergeOp.initializeOp(jobConf)
    val pClass = outputPath.getClass
    val updatePathsMethod = mergeOp.getClass.getSuperclass
      .getDeclaredMethod("updatePaths", pClass, pClass)
    updatePathsMethod.setAccessible(true)
    updatePathsMethod.invoke(mergeOp, new Path(tmpMergeLocationDir), taskTmpPath)
    val row = new Array[AnyRef](2)
    files.foreach { file =>
      val reader = getMergeFileReader[AnyRef, AnyRef](outputClassName,
        file, jobConf)
      var key = reader.createKey()
      var value = reader.createValue()
      try {
        while (reader.next(key, value)) {
          row(0) = key
          row(1) = value
          mergeOp.process(row, 0)
        }
      } catch {
        case e: HiveException =>
          throw new IOException(e)
      } finally {
        reader.close()
      }
    }
    mergeOp.closeOp(true)
    fs.cancelDeleteOnExit(outputPath)
    val taskPath = new Path(tmpMergeLocationDir + "/part-" +
      "%05d".format(context.partitionId()) + extension)
    commitTask(fs, attemptPath, taskPath, waitTime)
  }
}
