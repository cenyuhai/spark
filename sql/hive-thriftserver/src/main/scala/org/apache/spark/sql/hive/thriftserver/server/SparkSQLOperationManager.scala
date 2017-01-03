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

package org.apache.spark.sql.hive.thriftserver.server

import java.util.{Map => JMap}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.Executors

import scala.collection.JavaConverters._

import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.{ExecuteStatementOperation, Operation, OperationManager}
import org.apache.hive.service.cli.session.HiveSession

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveSessionState
import org.apache.spark.sql.hive.thriftserver.{ReflectionUtils, SparkExecuteStatementOperation}
import org.apache.spark.ui.jobs.UIData.JobUIData


/**
 * Executes queries using Spark SQL, and maintains a list of handles to active queries.
 */
private[thriftserver] class SparkSQLOperationManager()
  extends OperationManager with Logging {

  val handleToOperation = ReflectionUtils
    .getSuperField[JMap[OperationHandle, Operation]](this, "handleToOperation")

  val sessionToActivePool = new ConcurrentHashMap[SessionHandle, String]()
  val sessionToContexts = new ConcurrentHashMap[SessionHandle, SQLContext]()

  val DEFAULT_JOB_INFO = new JobUIData()
  Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new Runnable {
    override def run(): Unit = {
      if (handleToOperation.size() == 0) {
        return
      }
      val jobInfos = handleToOperation.values().asScala
        .map(op => sessionToContexts.get(op.getParentSession.getSessionHandle).sparkContext)
        .flatMap(context =>
          context.jobProgressListener.activeJobs.values
            .map(jobInfo => (jobInfo.jobGroup.getOrElse(""), jobInfo))
        ).toMap

      handleToOperation.values().asScala
        .foreach(operation => {
          val jobInfo = jobInfos.getOrElse(
            operation.asInstanceOf[SparkExecuteStatementOperation].statementId, DEFAULT_JOB_INFO)
          if (jobInfo != DEFAULT_JOB_INFO) {
            operation.getOperationLog.writeOperationLog(
              s"Job ID: ${jobInfo.jobId}, CompletedTasks: ${jobInfo.numCompletedTasks}, "
                + s"SkippedTasks: ${jobInfo.numSkippedTasks}, "
                + s"TotalTasks: ${jobInfo.numTasks} \n")
          }
        })
    }
  }, 0, 5, TimeUnit.SECONDS)

  override def newExecuteStatementOperation(
      parentSession: HiveSession,
      statement: String,
      confOverlay: JMap[String, String],
      async: Boolean): ExecuteStatementOperation = synchronized {
    val sqlContext = sessionToContexts.get(parentSession.getSessionHandle)
    require(sqlContext != null, s"Session handle: ${parentSession.getSessionHandle} has not been" +
      s" initialized or had already closed.")
    val sessionState = sqlContext.sessionState.asInstanceOf[HiveSessionState]
    val runInBackground = async && sessionState.hiveThriftServerAsync
    val operation = new SparkExecuteStatementOperation(parentSession, statement, confOverlay,
      runInBackground)(sqlContext, sessionToActivePool)
    handleToOperation.put(operation.getHandle, operation)
    logDebug(s"Created Operation for $statement with session=$parentSession, " +
      s"runInBackground=$runInBackground")
    operation
  }
}
