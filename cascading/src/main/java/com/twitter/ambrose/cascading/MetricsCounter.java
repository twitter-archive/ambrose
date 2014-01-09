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
package com.twitter.ambrose.cascading;

import java.util.Map;
import com.google.common.collect.Maps;

/**
 * Lookup class that constructs Counter names to be retrieved from Cascading
 * published statistics. Supports (legacy) Hadoop 0.20.x.x/1.x.x and YARN
 * counter names.
 *
 * @author Lorand Bendig <lbendig@gmail.com>
 *
 */
public enum MetricsCounter {

  // Task counters
  SLOTS_MILLIS_MAPS(1),
  SLOTS_MILLIS_REDUCES(1),

  // Filesystem counters
  FILE_BYTES_WRITTEN(2),
  HDFS_BYTES_WRITTEN(2),

  // Task counters
  MAP_INPUT_RECORDS(3),
  MAP_OUTPUT_RECORDS(3),
  SPILLED_RECORDS(3),
  REDUCE_INPUT_RECORDS(3),
  REDUCE_OUTPUT_RECORDS(3);

  private static final Map<MetricsCounter, String[]> lookup = Maps.newHashMap();
  static {
    for (MetricsCounter hjc : MetricsCounter.values()) {
      lookup.put(hjc, createLookupKeys(hjc));
    }
  }

  private int type;

  private MetricsCounter(int type) {
    this.type = type;
  }

  private static final String JOB_COUNTER = "org.apache.hadoop.mapred.JobInProgress$Counter";
  private static final String TASK_COUNTER = "org.apache.hadoop.mapred.Task$Counter";
  private static final String FS_COUNTER = "FileSystemCounters";

  private static final String JOB_COUNTER_YARN = "org.apache.hadoop.mapreduce.JobCounter";
  private static final String TASK_COUNTER_YARN = "org.apache.hadoop.mapreduce.TaskCounter";
  private static final String FS_COUNTER_YARN = "org.apache.hadoop.mapreduce.FileSystemCounter";

  private static final String[] EMPTY_ARR = {};

  public static String[] get(MetricsCounter hjc) {
    return lookup.get(hjc);
  }

  private static String[] createLookupKeys(MetricsCounter hjc) {
    switch (hjc.type) {
    // Job counter (type-1)
    case 1:
      return new String[] {
        JOB_COUNTER + "::" + hjc.name(),
        JOB_COUNTER_YARN + "::" + hjc.name()
      };
    // Task counter (type-2)
    case 2:
      return new String[] {
        TASK_COUNTER + "::" + hjc.name(),
        TASK_COUNTER_YARN + "::" + hjc.name()
      };
    // Filesystem counter (type-3)
    case 3:
      return new String[] {
        FS_COUNTER + "::" + hjc.name(),
        FS_COUNTER_YARN + "::" + hjc.name()
      };
    default:
      return EMPTY_ARR;
    }
  }

}
