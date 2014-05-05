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
package com.twitter.ambrose.hive;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Maps;
import com.twitter.ambrose.model.Job;
import com.twitter.ambrose.model.hadoop.CounterGroup;
import com.twitter.ambrose.model.hadoop.MapReduceJobState;
import com.twitter.ambrose.util.JSONUtil;

/**
 * Subclass of Job used to hold initialization logic and Hive-specific bindings
 * for a Job. Encapsulates all information related to a run of a Hive job. A job
 * might have counters, job configuration and job metrics. Job metrics is
 * metadata about the job run that isn't set in the job configuration.
 * 
 * @author Lorand Bendig <lbendig@gmail.com>
 * 
 */
@JsonTypeName("hive")
public class HiveJob extends Job {

  private static final Log LOG = LogFactory.getLog(HiveJob.class);

  private final String[] aliases;
  private final String[] features;
  private MapReduceJobState mapReduceJobState;

  private Map<String, CounterGroup> counterGroupMap;

  public HiveJob(String[] aliases, String[] features) {
    super();
    this.aliases = aliases;
    this.features = features;
    // TODO: inputInfoList and outputInfoList?
  }

  @JsonCreator
  public HiveJob(@JsonProperty("id") String id,
    @JsonProperty("aliases") String[] aliases,
    @JsonProperty("features") String[] features,
    @JsonProperty("mapReduceJobState") MapReduceJobState mapReduceJobState,
    @JsonProperty("counterGroupMap") Map<String, CounterGroup> counterGroupMap) {
    this(aliases, features);
    setId(id);
    this.mapReduceJobState = mapReduceJobState;
    this.counterGroupMap = counterGroupMap;
  }

  public String[] getAliases() {
    return aliases;
  }

  public String[] getFeatures() {
    return features;
  }

  public MapReduceJobState getMapReduceJobState() {
    return mapReduceJobState;
  }

  public void setMapReduceJobState(MapReduceJobState mapReduceJobState) {
    this.mapReduceJobState = mapReduceJobState;
  }

  public Map<String, CounterGroup> getCounterGroupMap() {
    return counterGroupMap;
  }

  public CounterGroup getCounterGroupInfo(String name) {
    return counterGroupMap == null ? null : counterGroupMap.get(name);
  }

  @JsonIgnore
  public void setJobStats(Map<String, Double> counterNameToValue, int totalMappers,
    int totalReducers) {
    counterGroupMap = AmbroseHiveUtil.counterGroupInfoMap(counterNameToValue);

    // job metrics
    Map<String, Number> metrics = Maps.newHashMap();
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

  private Double getCounterValue(Map<String, Double> counterNameToValue, MetricsCounter hjc) {
    String[] keys = MetricsCounter.get(hjc);
    return (counterNameToValue.get(keys[0]) == null) ? counterNameToValue.get(keys[1])
        : counterNameToValue.get(keys[0]);
  }

  private Double getAvgCounterValue(Map<String, Double> counterNameToValue, MetricsCounter hjc, int divisor) {
    if (divisor == 0) {
      return Double.valueOf(0.0d);
    }
    return getCounterValue(counterNameToValue, hjc) / Double.valueOf((double)divisor);
  }

}
