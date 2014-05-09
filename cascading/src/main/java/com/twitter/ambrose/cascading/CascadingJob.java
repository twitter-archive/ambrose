/*
Copyright 2013 Twitter, Inc.

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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;

import cascading.stats.hadoop.HadoopStepStats;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.twitter.ambrose.model.hadoop.CounterGroup;
import com.twitter.ambrose.model.hadoop.MapReduceJob;

/**
 * Subclass of MapReduceJob used to hold initialization logic and Cascading-specific bindings for a Job.
 * It represents FlowStepJob and all related job metrics will be captured using HadoopStepStats class
 * @author Ahmed Mohsen
 */
@JsonTypeName("cascading")
public class CascadingJob extends MapReduceJob {

  protected static Log LOG = LogFactory.getLog(CascadingJob.class);

  private String[] features = {};

  public CascadingJob() {
    super();
  }

  public CascadingJob(@JsonProperty("features") String[] features) {
    this.features = features;
  }

  public String[] getFeatures() { return features; }

  public void setFeatures(String[] features) {
    this.features = features;
  }

  @JsonIgnore
  public void setJobStats(HadoopStepStats stats) {
    Map<String, Long> counterNameToValue = counterGroupInfoMapHelper(stats);

    // job metrics
    int totalMappers = stats.getNumMapTasks();
    int totalReducers = stats.getNumReduceTasks();
    Map<String, Number> metrics = new HashMap<String, Number>();
    metrics.put("numberMaps", totalMappers);
    metrics.put("numberReduces", totalReducers);
    metrics.put("avgMapTime",
        getAvgCounterValue(counterNameToValue, MetricsCounter.SLOTS_MILLIS_MAPS, totalMappers));
    metrics.put("avgReduceTime",
        getAvgCounterValue(counterNameToValue, MetricsCounter.SLOTS_MILLIS_REDUCES, totalReducers));
    metrics.put("bytesWritten",
        getCounterValue(counterNameToValue, MetricsCounter.FILE_BYTES_WRITTEN));
    metrics.put("hdfsBytesWritten",
        getCounterValue(counterNameToValue, MetricsCounter.HDFS_BYTES_WRITTEN));
    metrics.put("mapInputRecords",
        getCounterValue(counterNameToValue, MetricsCounter.MAP_INPUT_RECORDS));
    metrics.put("mapOutputRecords",
        getCounterValue(counterNameToValue, MetricsCounter.MAP_OUTPUT_RECORDS));
    metrics.put("proactiveSpillCountRecs",
        getCounterValue(counterNameToValue, MetricsCounter.SPILLED_RECORDS));
    metrics.put("reduceInputRecords",
        getCounterValue(counterNameToValue, MetricsCounter.REDUCE_INPUT_RECORDS));
    metrics.put("reduceOutputRecords",
        getCounterValue(counterNameToValue, MetricsCounter.REDUCE_OUTPUT_RECORDS));
    setMetrics(metrics);
  }

  /**
   * helper method for counter group map retrieval
   * @param  HadoopStepStats
   * @return a map of counter name to counter value
   */
  private Map<String,Long> counterGroupInfoMapHelper(HadoopStepStats stats){
    Counters counters = new Counters();
    Map<String, Long> counterNameToValue = new HashMap<String, Long>();
    for (String groupName : stats.getCounterGroups()) {  //retreiving groups
      for (String counterName : stats.getCountersFor(groupName)) { //retreiving counters in that group
        Long counterValue = stats.getCounterValue(groupName, counterName);
        counterNameToValue.put(groupName+"::"+counterName, counterValue);

        //creating counter
        Counter counter = counters.findCounter(groupName, counterName);
        counter.setValue(counterValue);
      }
    }
    setCounterGroupMap(CounterGroup.counterGroupInfoMap(counters));
    return counterNameToValue;
  }

  private Double getCounterValue(Map<String, Long> counterNameToValue, MetricsCounter hjc) {
    String[] keys = MetricsCounter.get(hjc);
    if(counterNameToValue.get(keys[1]) == null)
      return Double.valueOf(0.0d);
    return (counterNameToValue.get(keys[0]) == null) ? counterNameToValue.get(keys[1]).doubleValue()
        : counterNameToValue.get(keys[0]).doubleValue();
  }

  private Double getAvgCounterValue(Map<String, Long> counterNameToValue, MetricsCounter hjc, int divisor) {
    if (divisor == 0) {
      return Double.valueOf(0.0d);
    }
    return getCounterValue(counterNameToValue, hjc) / Double.valueOf((double)divisor);
  }
}
