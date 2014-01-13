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

import java.util.Map;
import java.util.HashMap;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeName;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;

import com.twitter.ambrose.util.JSONUtil;
import com.twitter.ambrose.model.Job;
import com.twitter.ambrose.model.hadoop.CounterGroup;
import com.twitter.ambrose.model.hadoop.MapReduceJobState;

import cascading.stats.hadoop.HadoopStepStats;

/**
 * Subclass of Job used to hold initialization logic and Cascading-specific bindings for a Job.
 * It represents FlowStepJob and all related job metrics will be captured using HadoopStepStats class
 * @author Ahmed Mohsen
 */

// FIXME: this makes this deserialize into PigJob on the hraven side, so that it does not need
// to be rebuilt to work with cascading
@JsonTypeName("pig")
public class CascadingJob extends Job{
  protected static Log LOG = LogFactory.getLog(CascadingJob.class);

  private String[] aliases;
  private String[] features;
  private MapReduceJobState mapReduceJobState;
  // TODO: inputInfoList and outputInfoList?

  private Map<String, CounterGroup> counterGroupMap;

  public CascadingJob(String[] aliases, String[] features) {
    super();
    this.aliases = aliases;
    this.features = features;
    // TODO: inputInfoList and outputInfoList?

  }

  @JsonCreator
  public CascadingJob(@JsonProperty("id") String id,
                 @JsonProperty("aliases") String[] aliases,
                 @JsonProperty("features") String[] features,
                 @JsonProperty("mapReduceJobState") MapReduceJobState mapReduceJobState,
                 @JsonProperty("counterGroupMap") Map<String, CounterGroup> counterGroupMap) {
    this(aliases, features);
    setId(id);
    this.mapReduceJobState = mapReduceJobState;
    this.counterGroupMap = counterGroupMap;
  }

  public String[] getAliases() { return aliases; }
  public String[] getFeatures() { return features; }

  public MapReduceJobState getMapReduceJobState() { return mapReduceJobState; }
  public void setMapReduceJobState(MapReduceJobState mapReduceJobState) {
    this.mapReduceJobState = mapReduceJobState;
  }

  public Map<String, CounterGroup> getCounterGroupMap() { return counterGroupMap; }
  public CounterGroup getCounterGroupInfo(String name) {
    return counterGroupMap == null ? null : counterGroupMap.get(name);
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
     this.counterGroupMap = CounterGroup.counterGroupInfoMap(counters);
     return counterNameToValue;
   }



  /**
   * This is a hack to get around how the json library requires subtype info to
   * be defined on the super-class, which doesn't always have access to the
   * subclasses at compile time. Since the mixinAnnotations method replaces the
   * existing annotation, this means that an action like this will need to be
   * taken once upon app startup to register all known types. If this action
   * happens multiple times, calls will override each other.
   *
   * @see com.twitter.ambrose.pig.HiveJob#mixinJsonAnnotations()
   */
  public static void mixinJsonAnnotations() {
    LOG.info("Mixing in JSON annotations for CascadingJob and Job into Job");
    JSONUtil.mixinAnnotatons(Job.class, AnnotationMixinClass.class);
  }

  @JsonSubTypes({
      @JsonSubTypes.Type(value = com.twitter.ambrose.model.Job.class, name = "default"),
      @JsonSubTypes.Type(value = com.twitter.ambrose.cascading.CascadingJob.class, name = "cascading") })
  private static class AnnotationMixinClass {}

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
