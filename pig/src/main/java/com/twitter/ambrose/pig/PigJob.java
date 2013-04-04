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
package com.twitter.ambrose.pig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.pig.tools.pigstats.InputStats;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import com.twitter.ambrose.model.Job;
import com.twitter.ambrose.model.hadoop.CounterGroup;
import com.twitter.ambrose.model.hadoop.MapReduceJobState;

/**
 * Subclass of Job used to hold initialization logic and Pig-specific bindings for a Job.
 * Encapsulates all information related to a run of a Pig job. A job might have multiple inputs and
 * outputs, as well as counters, job configuration and job metrics. Job metrics is metadata about
 * the job run that isn't set in the job configuration.
 *
 * @author billg
 */
public class PigJob extends Job {
  private static final String RUNTIME = "pig";

  private String[] aliases;
  private String[] features;
  private MapReduceJobState mapReduceJobState;
  private List<InputInfo> inputInfoList;
  private List<OutputInfo> outputInfoList;

  private Map<String, CounterGroup> counterGroupMap;

  public PigJob(String[] aliases, String[] features) {
    super(RUNTIME);
    this.aliases = aliases;
    this.features = features;
  }

  @JsonCreator
  public PigJob(@JsonProperty("id") String id,
                @JsonProperty("aliases") String[] aliases,
                @JsonProperty("features") String[] features) {
    super(RUNTIME);
    this.aliases = aliases;
    this.features = features;
    this.mapReduceJobState = mapReduceJobState;
    this.counterGroupMap = counterGroupMap;
    this.inputInfoList = inputInfoList;
    this.outputInfoList = outputInfoList;
  }

  public String[] getAliases() { return aliases; }
  public String[] getFeatures() { return features; }

  @JsonIgnore
  public MapReduceJobState getMapReduceJobState() { return mapReduceJobState; }
  @JsonIgnore
  public void setMapReduceJobState(MapReduceJobState mapReduceJobState) {
    this.mapReduceJobState = mapReduceJobState;
  }

  @JsonIgnore
  public Map<String, CounterGroup> getCounterGroupMap() { return counterGroupMap; }
  @JsonIgnore
  public CounterGroup getCounterGroupInfo(String name) {
    return counterGroupMap == null ? null : counterGroupMap.get(name);
  }

  @JsonIgnore
  public void setJobStats(JobStats stats) {
    this.counterGroupMap = CounterGroup.counterGroupInfoMap(stats.getHadoopCounters());
    this.inputInfoList = inputInfoList(stats.getInputs());
    this.outputInfoList = outputInfoList(stats.getOutputs());

    // job metrics
    Map<String, Number> metrics = new HashMap<String, Number>();
    metrics.put("avgMapTime", stats.getAvgMapTime());
    metrics.put("avgReduceTime", stats.getAvgREduceTime());
    metrics.put("bytesWritten", stats.getBytesWritten());
    metrics.put("hdfsBytesWritten", stats.getHdfsBytesWritten());
    metrics.put("mapInputRecords", stats.getMapInputRecords());
    metrics.put("mapOutputRecords", stats.getMapOutputRecords());
    metrics.put("maxMapTime", stats.getMaxMapTime());
    metrics.put("maxReduceTime", stats.getMaxReduceTime());
    metrics.put("minMapTime", stats.getMinMapTime());
    metrics.put("minReduceTime", stats.getMinReduceTime());
    metrics.put("numberMaps", stats.getNumberMaps());
    metrics.put("numberReduces", stats.getNumberReduces());
    metrics.put("proactiveSpillCountObjects", stats.getProactiveSpillCountObjects());
    metrics.put("proactiveSpillCountRecs", stats.getProactiveSpillCountRecs());
    metrics.put("recordWritten", stats.getRecordWrittern());
    metrics.put("reduceInputRecords", stats.getReduceInputRecords());
    metrics.put("reduceOutputRecords", stats.getReduceOutputRecords());
    metrics.put("SMMSpillCount", stats.getSMMSpillCount());
    setMetrics(metrics);
  }

  private static List<InputInfo> inputInfoList(List<InputStats> inputStatsList) {
    List<InputInfo> inputInfoList = new ArrayList<InputInfo>();
    if (inputStatsList == null) { return inputInfoList; }

    for (InputStats inputStats : inputStatsList) {
      inputInfoList.add(new PigInputInfo(inputStats));
    }

    return inputInfoList;
  }

  private static List<OutputInfo> outputInfoList(List<OutputStats> inputStatsList) {
    List<OutputInfo> inputInfoList = new ArrayList<OutputInfo>();
    if (inputStatsList == null) { return inputInfoList; }

    for (OutputStats inputStats : inputStatsList) {
      inputInfoList.add(new PigOutputInfo(inputStats));
    }

    return inputInfoList;
  }

  private static class PigInputInfo extends InputInfo {
    private PigInputInfo(InputStats inputStats) {
      super(inputStats.getName(),
        inputStats.getLocation(),
        inputStats.getBytes(),
        inputStats.getNumberRecords(),
        inputStats.isSuccessful(),
        enumToString(inputStats.getInputType()));
    }

    private static String enumToString(Enum<?> someEnum) {
      return someEnum != null ? someEnum.name() : "";
    }
  }

  private static class PigOutputInfo extends OutputInfo {
    private PigOutputInfo(OutputStats outputStats) {
      super(outputStats.getName(),
            outputStats.getLocation(),
            outputStats.getBytes(),
            outputStats.getNumberRecords(),
            outputStats.isSuccessful(),
            outputStats.getFunctionName(),
            outputStats.getAlias());
    }
  }
}
