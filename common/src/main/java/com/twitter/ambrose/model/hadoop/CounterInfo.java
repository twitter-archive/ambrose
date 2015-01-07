package com.twitter.ambrose.model.hadoop;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.hadoop.mapred.Counters;

/**
 * CounterInfo holds the name, displayName and value of a given counter. A counter group contains
 * multiple of these.
 */
public class CounterInfo {
  private final String name;
  private final String displayName;
  private final long value;

  public CounterInfo(Counters.Counter counter) {
    this.name = counter.getName();
    this.displayName = counter.getDisplayName();
    this.value = counter.getValue();
  }

  @JsonCreator
  public CounterInfo(
      @JsonProperty("name") String name,
      @JsonProperty("displayName") String displayName,
      @JsonProperty("value") long value
  ) {
    this.name = name;
    this.displayName = displayName;
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public String getDisplayName() {
    return displayName;
  }

  public long getValue() {
    return value;
  }
}
