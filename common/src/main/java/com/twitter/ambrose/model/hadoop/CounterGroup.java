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
package com.twitter.ambrose.model.hadoop;

import java.util.Map;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;

/**
 * Immutable class that represents a group of Hadoop counters along with the individual counter
 * names and values.
 *
 * @author billg
 */
@SuppressWarnings("deprecation")
public class CounterGroup {

  /**
   * Constructs map of counter groups by name from Hadoop Counters instance.
   *
   * @param counters counters.
   * @return map of counter groups by name.
   */
  public static Map<String, CounterGroup> counterGroupsByName(Counters counters) {
    Map<String, CounterGroup> counterGroupsByName = Maps.newHashMap();
    if (counters != null) {
      for (Counters.Group group : counters) {
        CounterGroup counterGroup = new CounterGroup(group);
        counterGroupsByName.put(counterGroup.getGroupName(), counterGroup);
      }
    }
    return counterGroupsByName;
  }

  private final String groupName;
  private final String groupDisplayName;
  private final Map<String, CounterInfo> counterInfoMap;

  public CounterGroup(Counters.Group group) {
    this.groupName = group.getName();
    this.groupDisplayName = group.getDisplayName();
    this.counterInfoMap = Maps.newHashMap();

    for (Counter counter : group) {
      CounterInfo counterInfo = new CounterInfo(counter);
      counterInfoMap.put(counterInfo.getName(), counterInfo);
    }
  }

  @JsonCreator
  public CounterGroup(
      @JsonProperty("groupName") String groupName,
      @JsonProperty("groupDisplayName") String groupDisplayName,
      @JsonProperty("counterInfoMap") Map<String, CounterInfo> counterInfoMap
  ) {
    this.groupName = groupName;
    this.groupDisplayName = groupDisplayName;
    this.counterInfoMap = counterInfoMap;
  }

  public String getGroupName() {
    return groupName;
  }

  public String getGroupDisplayName() {
    return groupDisplayName;
  }

  public Map<String, CounterInfo> getCounterInfoMap() {
    return counterInfoMap;
  }

  @Nullable
  public CounterInfo getCounterInfo(String name) {
    return counterInfoMap == null ? null : counterInfoMap.get(name);
  }

}
