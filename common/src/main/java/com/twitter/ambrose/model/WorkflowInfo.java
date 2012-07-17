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
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.io.IOException;
import java.util.List;

/**
 * Class that represents the runtime stats for a given workflow. A workflow consists of 1 or more
 * jobs with dependancies that are run to produce a result. Basically a workflow is a collection of
 * jobs aranged to for a DAG. A Pig script or a Cascading flow are both examples of a workflow.
 *
 * @author billg
 */
@JsonSerialize(
  include=JsonSerialize.Inclusion.NON_NULL
)
public class WorkflowInfo {

  private String workflowId;
  private String workflowFingerprint;
  private List<JobInfo> jobInfoList;

  /**
   * Creates a new immutable WorkflowInfo object.
   *
   * @param workflowId the id of the workflow. This surrogate id should distinguish between one
   * workflow invocation and another.
   * @param workflowFingerprint the fingerprint of this workflow. The same workflow logic run
   * repeatedly, potentially over different input data, should result in the same fingerprint. A
   * change to the logic should result in a different fingerprint.
   * @param jobInfoList
   */
  @JsonCreator
  public WorkflowInfo(@JsonProperty("workflowId") String workflowId,
                      @JsonProperty("workflowFingerprint") String workflowFingerprint,
                      @JsonProperty("jobInfoList") List<JobInfo> jobInfoList) {
    this.workflowId = workflowId;
    this.workflowFingerprint = workflowFingerprint;
    this.jobInfoList = jobInfoList;
  }

  public String getWorkflowId() { return workflowId; }
  public String getWorkflowFingerprint() { return workflowFingerprint; }
  public List<JobInfo> getJobInfoList() { return jobInfoList; }

  /**
   * Serializes a WorkflowInfo object and it's children into a JSON String.
   *
   * @param workflowInfo the object to serialize
   * @return a JSON string.
   * @throws IOException
   */
  public static String toJSON(WorkflowInfo workflowInfo) throws IOException {
    ObjectMapper om = new ObjectMapper();
    om.getSerializationConfig().set(SerializationConfig.Feature.INDENT_OUTPUT, true);
    return om.writeValueAsString(workflowInfo);
  }

  /**
   * Derializes a JSON WorkflowInfo string into a WorkflowInfo object. Unrecognized properties will
   * be ignored.
   *
   * @param workflowInfoJson the string to convert into a JSON object.
   * @return a WorkflowInfo object.
   * @throws IOException
   */
  public static WorkflowInfo fromJSON(String workflowInfoJson) throws IOException {
    ObjectMapper om = new ObjectMapper();
    om.getDeserializationConfig().set(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    return om.readValue(workflowInfoJson, WorkflowInfo.class);
  }
}
