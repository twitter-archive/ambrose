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
package com.twitter.ambrose.service;

import com.twitter.ambrose.util.JSONUtil;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * Class that represents a Job node in the DAG. The job name must not be null. At DAG creation time
 * the jobID will probably be null. Ideally this will be set on the node when the job is started,
 * and the node will be sent as a <pre>WorkflowEvent.EVENT_TYPE.JOB_STARTED</pre> event.
 *
 * This class can be converted to JSON as-is by doing something like this:
 *
 * ObjectMapper om = new ObjectMapper();
 * om.getSerializationConfig().set(SerializationConfig.Feature.INDENT_OUTPUT, true);
 * String json = om.writeValueAsString(dagNode);
 */
@JsonSerialize(
  include=JsonSerialize.Inclusion.NON_NULL
)
public class DAGNode {
  private String name;
  private String[] aliases;
  private String[] features;
  private String jobId;
  private Collection<DAGNode> successors;
  private Collection<String> successorNames;
  private String runtimeName;
  private Integer dagLevel, x, y;

  public DAGNode(String name, String[] aliases, String[] features, String runtimeName) {
    this.name = name;
    this.aliases = aliases;
    this.features = features;
    this.runtimeName = runtimeName;
  }

  @JsonCreator
  public DAGNode(@JsonProperty("name") String name,
                 @JsonProperty("aliases") String[] aliases,
                 @JsonProperty("features") String[] features,
                 @JsonProperty("jobId") String jobId,
                 @JsonProperty("successorNames") Collection<String> successorNames,
                 @JsonProperty("runtimeName") String runtimeName) {
    this.name = name;
    this.aliases = aliases;
    this.features = features;
    this.jobId = jobId;
    this.successorNames = successorNames;
    this.runtimeName = runtimeName;
  }

  public String getName() { return name; }
  public String[] getAliases() { return aliases == null ? new String[0] : aliases; }
  public String[] getFeatures() { return features == null ? new String[0] : features; }
  public String getRuntimeName() { return runtimeName; }

  public String getJobId() { return jobId; }
  public void setJobId(String jobId) { this.jobId = jobId; }

  public Integer getDagLevel() { return dagLevel; }
  public void setDagLevel(Integer dagLevel) { this.dagLevel = dagLevel; }

  public Integer getX() { return x; }
  public void setX(Integer x) { this.x = x; }

  public Integer getY() { return y; }
  public void setY(Integer y) { this.y = y; }

  @JsonIgnore
  public synchronized Collection<DAGNode> getSuccessors() { return successors;}
  public synchronized void setSuccessors(Collection<DAGNode> successors) {
    Collection<String> successorNames = new HashSet<String>();
    if (successors != null) {
      for(DAGNode node : successors) {
        successorNames.add(node.getName());
      }
    }
    this.successors = successors;
    this.successorNames = successorNames;
  }

  public synchronized Collection<String> getSuccessorNames() { return successorNames; }

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws IOException {
    String sourceFile = "pig/src/main/resources/web/data/large-dag.json";
    String json = JSONUtil.readFile(sourceFile);
    List<DAGNode> nodes =
      (List<DAGNode>)JSONUtil.readJson(json, new TypeReference<List<DAGNode>>() { });
    for (DAGNode node : nodes) {
      // useful if we need to read a file, add a field, output and re-generate
      node.setRuntimeName("pig");
    }

    JSONUtil.writeJson(sourceFile + "2", nodes);
  }

  private void setRuntimeName(String runtimeName) { this.runtimeName = runtimeName; }
}
