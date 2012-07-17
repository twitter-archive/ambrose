/*
Copyright 2012 Twitter, Inc.

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
package com.twitter.ambrose.model;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.HashMap;
import java.util.Map;

/**
 * Immutable class that represents a group of Hadoop counters along with the individual counter
 * names and values.
 *
 * @author billg
 */
@SuppressWarnings("deprecation")
@JsonSerialize(
  include=JsonSerialize.Inclusion.NON_NULL
)
public class CounterGroupInfo {

  private String groupName;
  private String groupDisplayName;
  private Map<String, CounterInfo> counterInfoMap;

  public CounterGroupInfo(Counters.Group group) {
    this.groupName = group.getName();
    this.groupDisplayName = group.getDisplayName();
    this.counterInfoMap = new HashMap<String, CounterInfo>();

    for (Counter counter : group) {
      CounterInfo counterInfo = new CounterInfo(counter);
      counterInfoMap.put(counterInfo.getName(), counterInfo);
    }
  }

  @JsonCreator
  public CounterGroupInfo(@JsonProperty("groupName") String groupName,
                          @JsonProperty("groupDisplayName") String groupDisplayName,
                          @JsonProperty("counterInfoMap") Map<String, CounterInfo> counterInfoMap) {
    this.groupName = groupName;
    this.groupDisplayName = groupDisplayName;
    this.counterInfoMap = counterInfoMap;
  }

  public String getGroupName() { return groupName; }
  public String getGroupDisplayName() { return groupDisplayName; }
  public Map<String, CounterInfo> getCounterInfoMap() { return counterInfoMap; }

  public CounterInfo getCounterInfo(String name) {
    return counterInfoMap == null ? null : counterInfoMap.get(name);
  }

  public static Map<String, CounterGroupInfo> counterGroupInfoMap(Counters counters) {
    Map<String, CounterGroupInfo> counterGroupInfoMap = new HashMap<String, CounterGroupInfo>();
    if (counters != null) {
      for (Counters.Group group : counters) {
        CounterGroupInfo counterGroupInfo = new CounterGroupInfo(group);
        counterGroupInfoMap.put(counterGroupInfo.getGroupName(), counterGroupInfo);
      }
    }
    return counterGroupInfoMap;
  }

  /**
   * CounterInfo holds the name, displayName and value of a given counter. A counter group contains
   * multiple of these.
   */
  @JsonSerialize(
    include=JsonSerialize.Inclusion.NON_NULL
  )
  public static class CounterInfo {
    private String name, displayName;
    private long value;

    public CounterInfo(Counter counter) {
      this.name = counter.getName();
      this.displayName = counter.getDisplayName();
      this.value = counter.getValue();
    }

    @JsonCreator
    public CounterInfo(@JsonProperty("name") String name,
                       @JsonProperty("displayName") String displayName,
                       @JsonProperty("value") long value) {
      this.name = name;
      this.displayName = displayName;
      this.value = value;
    }

    public String getName() { return name; }
    public String getDisplayName() { return displayName; }
    public long getValue() { return value; }
  }
}
