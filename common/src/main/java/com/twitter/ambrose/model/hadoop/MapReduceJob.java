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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.twitter.ambrose.model.Job;

/**
 * 
 * @author amokashi
 *
 */
@JsonTypeName("mapred")
public class MapReduceJob extends Job {
  private String[] aliases = {};
  private String[] features = {};
  private MapReduceJobState mapReduceJobState;
  private Map<String, CounterGroup> counterGroupMap;
  
  public MapReduceJob() {
    super();
  }
  
  @JsonCreator
  public MapReduceJob(
                 @JsonProperty("aliases") String[] aliases,
                 @JsonProperty("features") String[] features,
                 @JsonProperty("mapReduceJobState") MapReduceJobState mapReduceJobState,
                 @JsonProperty("counterGroupMap") Map<String, CounterGroup> counterGroupMap) {
    super();
    this.aliases = aliases;
    this.features = features;
    this.mapReduceJobState = mapReduceJobState;
    this.counterGroupMap = counterGroupMap;
  }

  public String[] getAliases() { return aliases; }
  
  public void setAliases(String[] aliases) {
    this.aliases = aliases;
  }

  public String[] getFeatures() { return features; }

  public void setFeatures(String[] features) {
    this.features = features;
  }
  
  public MapReduceJobState getMapReduceJobState() { return mapReduceJobState; }

  public void setMapReduceJobState(MapReduceJobState mapReduceJobState) {
    this.mapReduceJobState = mapReduceJobState;
  }

  public Map<String, CounterGroup> getCounterGroupMap() { return counterGroupMap; }

  public CounterGroup getCounterGroupInfo(String name) {
    return counterGroupMap == null ? null : counterGroupMap.get(name);
  }

  public void setCounterGroupMap(Map<String, CounterGroup> counterGroupMap) {
    this.counterGroupMap = counterGroupMap;
  }
}
