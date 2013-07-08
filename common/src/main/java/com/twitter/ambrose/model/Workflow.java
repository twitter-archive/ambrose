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

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.twitter.ambrose.util.JSONUtil;

/**
 * Class that represents the runtime stats for a given workflow. A workflow consists of 1 or more
 * jobs with dependencies that are run to produce a result. Basically a workflow is a collection of
 * jobs arranged to for a DAG. A Pig script or a Cascading flow are both examples of a workflow.
 *
 * @author billg
 */
public class Workflow {

  private String workflowId;
  private String workflowFingerprint;
  private List<Job> jobs;

  /**
   * Creates a new immutable Workflow object.
   *
   * @param workflowId the id of the workflow. This surrogate id should distinguish between one
   * workflow invocation and another.
   * @param workflowFingerprint the fingerprint of this workflow. The same workflow logic run
   * repeatedly, potentially over different input data, should result in the same fingerprint. A
   * change to the logic should result in a different fingerprint.
   * @param jobs
   */
  @JsonCreator
  public Workflow(@JsonProperty("workflowId") String workflowId,
                  @JsonProperty("workflowFingerprint") String workflowFingerprint,
                  @JsonProperty("jobs") List<Job> jobs) {
    this.workflowId = workflowId;
    this.workflowFingerprint = workflowFingerprint;
    this.jobs = jobs;
  }

  public String getWorkflowId() { return workflowId; }
  public String getWorkflowFingerprint() { return workflowFingerprint; }
  public List<Job> getJobs() { return jobs; }

  /**
   * Serializes a Workflow object and it's children into a JSON String.
   *
   * @param workflow the object to serialize
   * @return a JSON string.
   * @throws IOException
   */
  public static String toJSON(Workflow workflow) throws IOException {
    return JSONUtil.toJson(workflow);
  }

  /**
   * Derializes a JSON Workflow string into a Workflow object. Unrecognized properties will
   * be ignored.
   *
   * @param workflowInfoJson the string to convert into a JSON object.
   * @return a Workflow object.
   * @throws IOException
   */
  public static Workflow fromJSON(String workflowInfoJson) throws IOException {
    return JSONUtil.toObject(workflowInfoJson, new TypeReference<Workflow>() {});
  }
}
