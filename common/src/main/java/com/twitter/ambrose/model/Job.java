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


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Map;
import java.util.Properties;

/**
 * Class that encapsulates all information related to a run of a job. A job might have job
 * configuration and job metric data. Job metrics represents job data that is produced after the
 * conclusion of a job.
 *
 * @author billg
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "runtime")
@JsonTypeName("default")
public class Job {
  private String id;
  private Properties configuration;
  private Map<String, Number> metrics;

  public Job() {
    this(null, null, null);
  }

  public Job(String id,
             Map<String, Number> metrics,
             Properties configuration) {
    this.id = id;
    this.metrics = metrics;
    this.configuration = configuration;
  }

  public String getId() { return id; }
  public void setId(String id) { this.id = id; }

  public Properties getConfiguration() { return configuration; }
  public void setConfiguration(Properties configuration) { this.configuration = configuration; }

  public Map<String, Number> getMetrics() { return metrics; }
  protected void setMetrics(Map<String, Number> metrics) { this.metrics = metrics; }
}
