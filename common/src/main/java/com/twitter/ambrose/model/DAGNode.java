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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.type.TypeReference;

import com.twitter.ambrose.util.JSONUtil;

/**
 * Class that represents a Job node in the DAG. The job name must not be null. At DAG creation time
 * the jobID will probably be null. Ideally this will be set on the node when the job is started,
 * and the node will be sent as a <pre>Event.Type.JOB_STARTED</pre> event.
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
public class DAGNode<T extends Job> {
  private String name;
  private T job;

  private Collection<DAGNode> successors;
  private Collection<String> successorNames;

  public DAGNode(String name, T job) {
    this.name = name;
    this.job = job;
  }

  @JsonCreator
  public DAGNode(@JsonProperty("name") String name,
                 @JsonProperty("job") T job,
                 @JsonProperty("successorNames") Collection<String> successorNames) {
    this.name = name;
    this.successorNames = successorNames;
  }

  public String getName() { return name; }
  public T getJob() { return job; }

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

    JSONUtil.writeJson(sourceFile + "2", nodes);
  }
}
