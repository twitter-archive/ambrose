/*
Copyright 2013, Lorand Bendig

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
package com.twitter.ambrose.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import com.twitter.ambrose.model.hadoop.MapReduceJob;

/**
 * Subclass of Job used to hold initialization logic and Hive-specific bindings
 * for a Job. Encapsulates all information related to a run of a Hive job. A job
 * might have counters, job configuration and job metrics. Job metrics is
 * metadata about the job run that isn't set in the job configuration.
 *
 * @author Lorand Bendig <lbendig@gmail.com>
 */
@JsonTypeName("hive")
public class HiveJob extends MapReduceJob {

  private String[] aliases;
  private String[] features;

  public HiveJob() {
    super();
  }

  @JsonCreator
  public HiveJob(
      @JsonProperty("aliases") String[] aliases,
      @JsonProperty("features") String[] features
  ) {
    super();
    this.aliases = aliases;
    this.features = features;
    // TODO: inputInfoList and outputInfoList?
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
}
