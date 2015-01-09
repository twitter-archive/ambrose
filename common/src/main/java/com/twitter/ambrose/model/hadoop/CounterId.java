/*
Copyright 2013, Lorand Bendig

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.twitter.ambrose.model.hadoop;

/**
 * Common Hadoop counter identifiers.
 *
 * @see <a href="https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/resources/org/apache/hadoop/mapreduce/JobCounter.properties">JobCounter.properties</a>
 * @see <a href="https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/resources/org/apache/hadoop/mapreduce/TaskCounter.properties">TaskCounter.properties</a>
 * @see <a href="https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/resources/org/apache/hadoop/mapreduce/FileSystemCounter.properties">FileSystemCounter.properties</a>
 * @see <a href="https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/JobCounter.java">JobCounter.java</a>
 * @see <a href="https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapred/JobInProgress.java">JobInProgress.java</a>
 * @see <a href="https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/TaskCounter.java">TaskCounter.java</a>
 * @see <a href="https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapred/Task.java">Task.java</a>
 * @see <a href="https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/FileSystemCounter.java">FileSystemCounter.java</a>
 * @see <a href="https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/counters/FileSystemCounterGroup.java">FileSystemCounterGroup.java</a>
 */
public enum CounterId {

  // Job counters
  NUM_FAILED_MAPS(CounterGroupId.JOB),
  NUM_FAILED_REDUCES(CounterGroupId.JOB),
  NUM_KILLED_MAPS(CounterGroupId.JOB),
  NUM_KILLED_REDUCES(CounterGroupId.JOB),
  TOTAL_LAUNCHED_MAPS(CounterGroupId.JOB),
  TOTAL_LAUNCHED_REDUCES(CounterGroupId.JOB),
  OTHER_LOCAL_MAPS(CounterGroupId.JOB),
  DATA_LOCAL_MAPS(CounterGroupId.JOB),
  RACK_LOCAL_MAPS(CounterGroupId.JOB),
  // deprecated
  SLOTS_MILLIS_MAPS(CounterGroupId.JOB),
  // deprecated
  SLOTS_MILLIS_REDUCES(CounterGroupId.JOB),
  // deprecated
  FALLOW_SLOTS_MILLIS_MAPS(CounterGroupId.JOB),
  // deprecated
  FALLOW_SLOTS_MILLIS_REDUCES(CounterGroupId.JOB),
  TOTAL_LAUNCHED_UBERTASKS(CounterGroupId.JOB),
  NUM_UBER_SUBMAPS(CounterGroupId.JOB),
  NUM_UBER_SUBREDUCES(CounterGroupId.JOB),
  NUM_FAILED_UBERTASKS(CounterGroupId.JOB),
  TASKS_REQ_PREEMPT(CounterGroupId.JOB),
  CHECKPOINTS(CounterGroupId.JOB),
  CHECKPOINT_BYTES(CounterGroupId.JOB),
  CHECKPOINT_TIME(CounterGroupId.JOB),
  MILLIS_MAPS(CounterGroupId.JOB),
  MILLIS_REDUCES(CounterGroupId.JOB),
  VCORES_MILLIS_MAPS(CounterGroupId.JOB),
  VCORES_MILLIS_REDUCES(CounterGroupId.JOB),
  MB_MILLIS_MAPS(CounterGroupId.JOB),
  MB_MILLIS_REDUCES(CounterGroupId.JOB),

  // Task counters
  MAP_INPUT_RECORDS(CounterGroupId.TASK),
  MAP_OUTPUT_RECORDS(CounterGroupId.TASK),
  MAP_SKIPPED_RECORDS(CounterGroupId.TASK),
  // deprecated; use group FileInputFormatCounters, counter BYTES_READ
  MAP_INPUT_BYTES(CounterGroupId.TASK),
  MAP_OUTPUT_BYTES(CounterGroupId.TASK),
  MAP_OUTPUT_MATERIALIZED_BYTES(CounterGroupId.TASK),
  SPLIT_RAW_BYTES(CounterGroupId.TASK),
  COMBINE_INPUT_RECORDS(CounterGroupId.TASK),
  COMBINE_OUTPUT_RECORDS(CounterGroupId.TASK),
  REDUCE_INPUT_GROUPS(CounterGroupId.TASK),
  REDUCE_SHUFFLE_BYTES(CounterGroupId.TASK),
  REDUCE_INPUT_RECORDS(CounterGroupId.TASK),
  REDUCE_OUTPUT_RECORDS(CounterGroupId.TASK),
  REDUCE_SKIPPED_GROUPS(CounterGroupId.TASK),
  REDUCE_SKIPPED_RECORDS(CounterGroupId.TASK),
  SPILLED_RECORDS(CounterGroupId.TASK),
  SHUFFLED_MAPS(CounterGroupId.TASK),
  FAILED_SHUFFLE(CounterGroupId.TASK),
  MERGED_MAP_OUTPUTS(CounterGroupId.TASK),
  GC_TIME_MILLIS(CounterGroupId.TASK),
  CPU_MILLISECONDS(CounterGroupId.TASK),
  PHYSICAL_MEMORY_BYTES(CounterGroupId.TASK),
  VIRTUAL_MEMORY_BYTES(CounterGroupId.TASK),
  COMMITTED_HEAP_BYTES(CounterGroupId.TASK),

  // Filesystem counters
  FILE_BYTES_READ(CounterGroupId.FILE_SYSTEM),
  FILE_BYTES_WRITTEN(CounterGroupId.FILE_SYSTEM),
  FILE_READ_OPS(CounterGroupId.FILE_SYSTEM),
  FILE_LARGE_READ_OPS(CounterGroupId.FILE_SYSTEM),
  FILE_WRITE_OPS(CounterGroupId.FILE_SYSTEM),
  HDFS_BYTES_READ(CounterGroupId.FILE_SYSTEM),
  HDFS_BYTES_WRITTEN(CounterGroupId.FILE_SYSTEM),
  HDFS_READ_OPS(CounterGroupId.FILE_SYSTEM),
  HDFS_LARGE_READ_OPS(CounterGroupId.FILE_SYSTEM),
  HDFS_WRITE_OPS(CounterGroupId.FILE_SYSTEM);

  public final CounterGroupId counterGroupId;

  private CounterId(CounterGroupId counterGroupId) {
    this.counterGroupId = counterGroupId;
  }
}
