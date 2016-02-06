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

package org.apache.spark.util.collection

import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.{Logging, SparkEnv}

/**
 * Spills contents of an in-memory collection to disk when the memory threshold
 * has been exceeded.
 */
private[spark] abstract class Spillable[C](taskMemoryManager: TaskMemoryManager)
  extends MemoryConsumer(taskMemoryManager)
  with Logging {
  
  /**
   * Spills the current in-memory collection to disk, and releases the memory.
   *
   * @param collection collection to spill to disk
   */
  protected def spill(collection: C): Unit

  // Number of elements read from input since last spill
  protected def elementsRead: Long = _elementsRead

  // Called by subclasses every time a record is read
  // It's used for checking spilling frequency
  protected def addElementsRead(): Unit = { _elementsRead += 1 }

  // Initial threshold for the size of a collection before we start tracking its memory usage
  // For testing only
  private[this] val initialMemoryThreshold: Long =
    SparkEnv.get.conf.getLong("spark.shuffle.spill.initialMemoryThreshold", 5 * 1024 * 1024)

  // Force this collection to spill when there are this many elements in memory
  // For testing only
  private[this] val numElementsForceSpillThreshold: Long =
    SparkEnv.get.conf.getLong("spark.shuffle.spill.numElementsForceSpillThreshold", Long.MaxValue)

  // Force this collection to spill when it exceeds the max memory threshold
  private[this] val maxUsedMemoryForceSpillThreshold: Long =
    SparkEnv.get.conf.getSizeAsBytes("spark.shuffle.spill.memoryForceSpillThreshold", "640m")

  // Threshold for this collection's size in bytes before we start tracking its memory usage
  // To avoid a large number of small spills, initialize this to a value orders of magnitude > 0
  private[this] var myMemoryThreshold = initialMemoryThreshold

  // Number of elements read from input since last spill
  private[this] var _elementsRead = 0L

  // Number of bytes spilled in total
  private[this] var _memoryBytesSpilled = 0L

  /**
   * Spills the current in-memory collection to disk if needed. Attempts to acquire more
   * memory before spilling.
   *
   * @param collection collection to spill to disk
   * @param currentMemory estimated size of the collection in bytes
   * @return true if `collection` was spilled to disk; false otherwise
   */
  protected def maybeSpill(collection: C, currentMemory: Long): Boolean = {
    var shouldSpill = false
    val exceedMemoryThreshold = currentMemory > maxUsedMemoryForceSpillThreshold
    if (!exceedMemoryThreshold && elementsRead % 32 == 0 && currentMemory >= myMemoryThreshold) {
      // Claim up to double our current memory from the shuffle memory pool
      val amountToRequest = 2 * currentMemory - myMemoryThreshold
      val granted =
        taskMemoryManager.acquireExecutionMemory(amountToRequest, MemoryMode.ON_HEAP, null)
      myMemoryThreshold += granted
      // If we were granted too little memory to grow further (either tryToAcquire returned 0,
      // or we already had more memory than myMemoryThreshold), spill the current collection
      shouldSpill = currentMemory >= myMemoryThreshold
    }

    val exceedElementsRead = _elementsRead > numElementsForceSpillThreshold
    shouldSpill = shouldSpill || exceedElementsRead || exceedMemoryThreshold
    // Actually spill
    if (shouldSpill) {
      spillCount += 1
      logSpillage(currentMemory)
      val start = System.currentTimeMillis()
      spill(collection)
      val end = System.currentTimeMillis()
      spillTime += (end - start)
      logSpillTime(end - start)
      _elementsRead = 0
      _memoryBytesSpilled += currentMemory
      releaseMemory()
    }
    shouldSpill
  }

  override def spill(size: Long, trigger: MemoryConsumer): Long = {
    0L
  }

  /**
   * @return number of bytes spilled in total
   */
  def memoryBytesSpilled: Long = _memoryBytesSpilled

  /**
   * Release our memory back to the execution pool so that other tasks can grab it.
   */
  def releaseMemory(): Unit = {
    // The amount we requested does not include the initial memory tracking threshold
    taskMemoryManager.releaseExecutionMemory(
      myMemoryThreshold - initialMemoryThreshold, MemoryMode.ON_HEAP, null)
    myMemoryThreshold = initialMemoryThreshold
  }

  /**
   * Prints a standard log message detailing spillage.
   *
   * @param size number of bytes spilled
   */
  @inline private def logSpillage(size: Long) {
    val threadId = Thread.currentThread().getId
    logInfo("Thread %d spilling in-memory map of %s to disk (%d time%s so far)"
      .format(threadId, org.apache.spark.util.Utils.bytesToString(size),
        spillCount, if (spillCount > 1) "s" else ""))
  }

  /**
    * Prints a standard log message detailing spill time.
    *
    * @param time time of spill spend
    */
  @inline private def logSpillTime(time: Long) {
    val threadId = Thread.currentThread().getId
    logInfo("Thread %d spilling in-memory map to disk spent %s (%s so far)"
      .format(threadId,
        org.apache.spark.util.Utils.msDurationToString(time),
        org.apache.spark.util.Utils.msDurationToString(spillTime)))
  }
}
