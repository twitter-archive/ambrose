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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.Counters;

import com.twitter.ambrose.model.hadoop.CounterGroup;
import com.twitter.ambrose.model.hadoop.MapReduceJob;

import cascading.stats.hadoop.HadoopStepStats;

/**
 * Subclass of MapReduceJob used to hold initialization logic and Cascading-specific bindings for a
 * Job. It represents FlowStepJob and all related job metrics will be captured using HadoopStepStats
 * class.
 *
 * @author Ahmed Mohsen
 */
@JsonTypeName("cascading")
public class CascadingJob extends MapReduceJob {

  private static Log LOG = LogFactory.getLog(CascadingJob.class);

  private String[] features = {};

  public CascadingJob() {
    super();
  }

  public CascadingJob(
      @JsonProperty("features") String[] features
  ) {
    super();
    this.features = features;
  }

  public String[] getFeatures() {
    return features;
  }

  public void setFeatures(String[] features) {
    this.features = features;
  }

  @JsonIgnore
  public void setJobStats(HadoopStepStats stats) {
    Counters counters = new Counters();
    for (String groupName : stats.getCounterGroups()) {
      for (String counterName : stats.getCountersFor(groupName)) {
        Long counterValue = stats.getCounterValue(groupName, counterName);
        counters.findCounter(groupName, counterName).setValue(counterValue);
      }
    }
    setCounterGroupMap(CounterGroup.counterGroupsByName(counters));
  }
}
