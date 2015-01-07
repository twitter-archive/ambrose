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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.pig.tools.pigstats.InputStats;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.mapreduce.MRJobStats;

import com.twitter.ambrose.model.hadoop.CounterGroup;
import com.twitter.ambrose.model.hadoop.MapReduceJob;

/**
 * Subclass of MapReduceJob used to hold initialization logic and Pig-specific bindings for a Job.
 * Encapsulates all information related to a run of a Pig job. A job might have multiple inputs and
 * outputs, as well as counters, job configuration and job metrics. Job metrics is metadata about
 * the job run that isn't set in the job configuration.
 *
 * @author billg
 */
@JsonTypeName("pig")
public class PigJob extends MapReduceJob {
  private String[] aliases = {};
  private String[] features = {};
  private List<InputInfo> inputInfoList;
  private List<OutputInfo> outputInfoList;

  public PigJob() {
    super();
  }

  @JsonCreator
  public PigJob(
      @JsonProperty("aliases") String[] aliases,
      @JsonProperty("features") String[] features,
      @JsonProperty("inputInfoList") List<InputInfo> inputInfoList,
      @JsonProperty("outputInfoList") List<OutputInfo> outputInfoList
  ) {
    super();
    this.aliases = aliases;
    this.features = features;
    this.inputInfoList = inputInfoList;
    this.outputInfoList = outputInfoList;
  }

  @JsonIgnore
  public void setJobStats(JobStats stats) {
    this.inputInfoList = inputInfoList(stats.getInputs());
    this.outputInfoList = outputInfoList(stats.getOutputs());

    // job metrics
    Map<String, Number> metrics = Maps.newHashMap();
    metrics.put("hdfsBytesRead", stats.getHdfsBytesRead());
    metrics.put("hdfsBytesWritten", stats.getHdfsBytesWritten());
    metrics.put("bytesWritten", stats.getBytesWritten());
    metrics.put("recordWritten", stats.getRecordWrittern());
    if (stats instanceof MRJobStats) {
      MRJobStats mrStats = (MRJobStats) stats;

      setCounterGroupMap(CounterGroup.counterGroupsByName(mrStats.getHadoopCounters()));

      metrics.put("avgMapTime", mrStats.getAvgMapTime());

      // internal pig seems to have fixed typo in OSS pig method name; avoid NoSuchMethodException
      // TODO: Remove this once internal pig is replaced w/ OSS pig
      Number avgReduceTime = null;
      try {
        Method method = mrStats.getClass().getMethod("getAvgReduceTime");
        avgReduceTime = (Number) method.invoke(mrStats);
      } catch (NoSuchMethodException e) {
        // assume we're dealing with OSS pig; ignore
      } catch (InvocationTargetException e) {
        throw new RuntimeException("Failed to invoke MRJobStats.getAvgReduceTime", e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Failed to invoke MRJobStats.getAvgReduceTime", e);
      }
      if (avgReduceTime == null) {
        avgReduceTime = mrStats.getAvgREduceTime();
      }
      metrics.put("avgReduceTime", avgReduceTime);

      metrics.put("mapInputRecords", mrStats.getMapInputRecords());
      metrics.put("mapOutputRecords", mrStats.getMapOutputRecords());
      metrics.put("maxMapTime", mrStats.getMaxMapTime());
      metrics.put("maxReduceTime", mrStats.getMaxReduceTime());
      metrics.put("minMapTime", mrStats.getMinMapTime());
      metrics.put("minReduceTime", mrStats.getMinReduceTime());
      metrics.put("numberMaps", mrStats.getNumberMaps());
      metrics.put("numberReduces", mrStats.getNumberReduces());
      metrics.put("proactiveSpillCountObjects", mrStats.getProactiveSpillCountObjects());
      metrics.put("proactiveSpillCountRecs", mrStats.getProactiveSpillCountRecs());
      metrics.put("reduceInputRecords", mrStats.getReduceInputRecords());
      metrics.put("reduceOutputRecords", mrStats.getReduceOutputRecords());
      metrics.put("SMMSpillCount", mrStats.getSMMSpillCount());
    }

    setMetrics(metrics);
  }

  public String[] getAliases() {
    return aliases;
  }

  public void setAliases(String[] aliases) {
    this.aliases = aliases;
  }

  public String[] getFeatures() {
    return features;
  }

  public void setFeatures(String[] features) {
    this.features = features;
  }

  public List<InputInfo> getInputInfoList() {
    return inputInfoList;
  }

  public List<OutputInfo> getOutputInfoList() {
    return outputInfoList;
  }

  private static List<InputInfo> inputInfoList(List<InputStats> inputStatsList) {
    List<InputInfo> inputInfoList = Lists.newArrayList();
    if (inputStatsList == null) {
      return inputInfoList;
    }

    for (InputStats inputStats : inputStatsList) {
      inputInfoList.add(new PigInputInfo(inputStats));
    }

    return inputInfoList;
  }

  private static List<OutputInfo> outputInfoList(List<OutputStats> outputStatsList) {
    List<OutputInfo> outputInfoList = Lists.newArrayList();
    if (outputStatsList == null) {
      return outputInfoList;
    }

    for (OutputStats outputStats : outputStatsList) {
      outputInfoList.add(new PigOutputInfo(outputStats));
    }

    return outputInfoList;
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
