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

import com.twitter.ambrose.model.JobInfo;
import org.apache.pig.tools.pigstats.InputStats;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Sublclass of JobInfo used to hold initialization logic and Pig-specifc bindings for a JobInfo.
 *
 * @author billg
 */
public class PigJobInfo extends JobInfo {


  public PigJobInfo(JobStats stats, Properties jobConfProperties) {
    super(stats.getHadoopCounters(),
          jobConfProperties,
          inputInfoList(stats.getInputs()),
          outputInfoList(stats.getOutputs()));

    // job metadata
    // TODO: do we really need this here?
    Map<String, Object> jobData = new HashMap<String, Object>();
    jobData.put("alias", stats.getAlias());
    jobData.put("avgMapTime", stats.getAvgMapTime());
    jobData.put("avgReduceTime", stats.getAvgREduceTime());
    jobData.put("bytesWritten", stats.getBytesWritten());
    jobData.put("errorMessage", stats.getErrorMessage());
    jobData.put("exception", stats.getException());
    jobData.put("feature", stats.getFeature());
    jobData.put("hdfsBytesWritten", stats.getHdfsBytesWritten());
    jobData.put("jobId", stats.getJobId());
    jobData.put("mapInputRecords", stats.getMapInputRecords());
    jobData.put("mapOutputRecords", stats.getMapOutputRecords());
    jobData.put("maxMapTime", stats.getMaxMapTime());
    jobData.put("maxReduceTime", stats.getMaxReduceTime());
    jobData.put("minMapTime", stats.getMinMapTime());
    jobData.put("minReduceTime", stats.getMinReduceTime());
    jobData.put("name", stats.getName());
    jobData.put("numberMaps", stats.getNumberMaps());
    jobData.put("numberReduces", stats.getNumberReduces());
    jobData.put("proactiveSpillCountObjects", stats.getProactiveSpillCountObjects());
    jobData.put("proactiveSpillCountRecs", stats.getProactiveSpillCountRecs());
    jobData.put("recordWrittern", stats.getRecordWrittern());
    jobData.put("reduceInputRecords", stats.getReduceInputRecords());
    jobData.put("reduceOutputRecords", stats.getReduceOutputRecords());
    jobData.put("state", stats.getState().name());
    jobData.put("SMMSpillCount", stats.getSMMSpillCount());
    setJobData(jobData);
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
