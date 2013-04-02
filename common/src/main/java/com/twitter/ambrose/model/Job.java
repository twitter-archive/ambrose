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

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Class that encapsulates all information related to a run of a job. A job might have job
 * configuration and job metric data. Job metrics represents job data that is produced after the
 * conclusion of a job.
 *
 * @author billg
 */
@SuppressWarnings("deprecation")
@JsonSerialize(
  include=JsonSerialize.Inclusion.NON_NULL
)
public class Job {

  private String runtime;
  private Properties configuration;
  private Map<String, Number> metrics;

  public Job(String runtime, Properties configuration) {
    this.runtime = runtime;
    this.configuration = configuration;
  }

  @JsonCreator
  public Job(@JsonProperty("runtume") String runtime,
             @JsonProperty("metrics") Map<String, Number> metrics,
             @JsonProperty("configuration") Properties configuration) {
    this.metrics = metrics;
    this.configuration = configuration;
  }

  public String getRuntime() { return runtime; }

  public Properties getConfiguration() { return configuration; }

  public Map<String, Number> getMetrics() { return metrics; }
  protected void setMetrics(Map<String, Number> metrics) { this.metrics = metrics; }
}
