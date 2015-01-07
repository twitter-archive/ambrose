/*
Copyright 2014 Twitter, Inc.

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

import java.util.Map;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Maps;

import com.twitter.ambrose.model.Job;

/**
 * Subclass of Job used to hold state information of a Hadoop map-reduce job. Encapsulates all
 * information related to a run of a MR job, including progress, status, and default counters
 * reported by Hadoop.
 *
 * @author amokashi
 */
@JsonTypeName("mapred")
public class MapReduceJob extends Job {
  private MapReduceJobState mapReduceJobState;
  private Map<String, CounterGroup> counterGroupMap;

  public MapReduceJob() {
    super();
  }

  @JsonCreator
  public MapReduceJob(
      @JsonProperty("mapReduceJobState") MapReduceJobState mapReduceJobState,
      @JsonProperty("counterGroupMap") Map<String, CounterGroup> counterGroupMap
  ) {
    super();
    this.mapReduceJobState = mapReduceJobState;
    this.counterGroupMap = counterGroupMap;
  }

  public MapReduceJobState getMapReduceJobState() {
    return mapReduceJobState;
  }

  public void setMapReduceJobState(MapReduceJobState mapReduceJobState) {
    this.mapReduceJobState = mapReduceJobState;
    updateMetrics();
  }

  public Map<String, CounterGroup> getCounterGroupMap() {
    return counterGroupMap;
  }

  public void setCounterGroupMap(Map<String, CounterGroup> counterGroupMap) {
    this.counterGroupMap = counterGroupMap;
    updateMetrics();
  }

  @Nullable
  public CounterGroup getCounterGroup(String name) {
    return counterGroupMap == null ? null : counterGroupMap.get(name);
  }

  @Nullable
  public Long getCounterValue(CounterId counterId) {
    CounterGroup counterGroup = null;
    for (String name : counterId.counterGroupId.groupNames) {
      counterGroup = getCounterGroup(name);
      if (counterGroup != null) {
        break;
      }
    }
    if (counterGroup != null) {
      CounterInfo counterInfo = counterGroup.getCounterInfo(counterId.name());
      if (counterInfo != null) {
        return counterInfo.getValue();
      }
    }
    return null;
  }

  /**
   * Initializes metrics values from data contained within mapreduce jobstate and counter group map.
   */
  protected void updateMetrics() {
    Map<String, Number> metrics = getMetrics();
    if (metrics == null) {
      metrics = Maps.newHashMap();
      setMetrics(metrics);
    }

    if (mapReduceJobState != null) {
      int totalMappers = mapReduceJobState.getTotalMappers();
      metrics.put("MAP_TASK_COUNT", totalMappers);

      int totalReducers = mapReduceJobState.getTotalReducers();
      metrics.put("REDUCE_TASK_COUNT", totalReducers);
    }

    if (counterGroupMap != null) {
      for (CounterId counterId : CounterId.values()) {
        Long value = getCounterValue(counterId);
        if (value != null) {
          getMetrics().put(counterId.name(), value);
        }
      }
    }
  }
}
