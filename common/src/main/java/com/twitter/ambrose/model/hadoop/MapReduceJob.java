package com.twitter.ambrose.model.hadoop;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.twitter.ambrose.model.Job;

public class MapReduceJob extends Job {
  private String[] aliases;
  private String[] features;
  private MapReduceJobState mapReduceJobState;
  private Map<String, CounterGroup> counterGroupMap;
  
  public MapReduceJob(String[] aliases, String[] features) {
    super();
    this.aliases = aliases;
    this.features = features;
  }
  
  @JsonCreator
  public MapReduceJob(@JsonProperty("id") String id,
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

}
