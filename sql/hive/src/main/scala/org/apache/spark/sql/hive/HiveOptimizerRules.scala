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

package org.apache.spark.sql.hive

import java.io.IOException

import scala.util.control.Breaks.{break, breakable}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.common.StatsSetupConst

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeSet, PredicateHelper, Rand}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.hive.merge.MergeUtils

case class DeterminePartitionedTableStats(sparkSession: SparkSession)
  extends Rule[LogicalPlan] with PredicateHelper {

  def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case filter@Filter(condition, relation: MetastoreRelation)
      if DDLUtils.isHiveTable(relation.catalogTable) &&
        !relation.catalogTable.partitionColumnNames.isEmpty &&
        sparkSession.sessionState.conf.metastorePartitionPruning =>
      val predicates = splitConjunctivePredicates(condition)
      val partitionSet = AttributeSet(relation.partitionKeys)
      val pruningPredicates = predicates.filter { predicate =>
        !predicate.references.isEmpty &&
          predicate.references.subsetOf(partitionSet)
      }
      if (pruningPredicates.nonEmpty) {
        val threshold = sparkSession.sessionState.conf.autoBroadcastJoinThreshold
        val prunedPartitions = sparkSession.sharedState.externalCatalog.listPartitionsByFilter(
          relation.catalogTable.database,
          relation.catalogTable.identifier.table,
          pruningPredicates)
        var sizeInBytes = 0L
        var hasError = false
        val partitions = prunedPartitions.filter(p => p.storage.locationUri.isDefined)
        val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
        var i = 0
        breakable {
          partitions.foreach { partition =>
            try {
              i = i + 1
              val rawDataSize = partition.parameters.get(StatsSetupConst.RAW_DATA_SIZE)
              val totalSize = partition.parameters.get(StatsSetupConst.TOTAL_SIZE)
              if ((rawDataSize.isDefined && rawDataSize.get.toLong > 0) ||
                  (totalSize.isDefined && totalSize.get.toLong > 0)) {
                if (rawDataSize.isDefined && rawDataSize.get.toLong > 0) {
                  sizeInBytes += rawDataSize.get.toLong
                } else {
                  sizeInBytes += totalSize.get.toLong
                }
              } else if (sparkSession.sessionState.conf.fallBackToHdfsForStatsEnabled) {
                val path = new Path(partition.storage.locationUri.get)
                val fs = path.getFileSystem(hadoopConf)
                sizeInBytes += fs.getContentSummary(path).getLength
              } else {
                hasError = true
                sizeInBytes = 0
                break()
              }
              if (sizeInBytes > threshold) {
                break()
              }
            } catch {
              case e: IOException =>
                logWarning("Failed to get table size from hdfs.", e)
                hasError = true
            }
          }
        }
        sizeInBytes = (sizeInBytes * partitions.size * 1d / i).toLong
        if (hasError && sizeInBytes == 0) {
          sizeInBytes = sparkSession.sessionState.conf.defaultSizeInBytes
        }
        relation.catalogTable.stats = Some(Statistics(sizeInBytes = BigInt(sizeInBytes)))
        Filter(condition, relation)
      } else {
        filter
      }
  }
}

case class MergeSmallFiles(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!sparkSession.sessionState.conf.mergeHiveFiles) return plan
    plan transformDown {
      case InsertIntoTable(table: MetastoreRelation, partition,
          child, overwrite, ifNotExists) if !child.isInstanceOf[Sort] &&
          !child.children.exists(a => a.isInstanceOf[RepartitionByExpression] ||
            a.isInstanceOf[Repartition] ||
            a.isInstanceOf[Sort]) && !table.databaseName.contains("temp") &&
            !MergeUtils.SUPPORTED_FORMAT.contains(table.tableDesc.getOutputFileFormatClassName) =>
        val rand = Alias(new Rand(), "SparkMergeTask")()
        val newProjected = Project(child.output :+ rand, child)
        val mergeFileStage = RepartitionByExpression(Seq(rand.toAttribute), newProjected, None)
        val finalOutput = Project(child.output, mergeFileStage)
        InsertIntoTable(table: MetastoreRelation, partition, finalOutput, overwrite, ifNotExists)
      case CreateTable(tableDesc, mode, Some(query)) if tableDesc.provider.get == "hive" &&
          !query.isInstanceOf[Sort] &&
          !query.children.exists(a => a.isInstanceOf[RepartitionByExpression]
            || a.isInstanceOf[Repartition] ||
            a.isInstanceOf[Sort]) && !tableDesc.database.contains("temp") &&
          !MergeUtils.SUPPORTED_FORMAT.contains(tableDesc.storage.outputFormat.get) =>
        CreateTable(tableDesc, mode, Some(RepartitionByExpression(
          Seq(Alias(new Rand(), "SparkMergeTask")()), query, None)))
    }
  }
}
