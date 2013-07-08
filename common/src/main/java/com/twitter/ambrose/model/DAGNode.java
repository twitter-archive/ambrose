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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Objects;

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
public class DAGNode<T extends Job> {
  private String name;
  private T job;
  @JsonIgnore
  private Collection<DAGNode<? extends Job>> successors;
  private Collection<String> successorNames;

  @JsonCreator
  public DAGNode(@JsonProperty("name") String name,
                 @JsonProperty("job") T job) {
    this.name = name;
    this.job = job;
  }

  public String getName() { return name; }
  public T getJob() { return job; }

  public synchronized Collection<DAGNode<? extends Job>> getSuccessors() { return successors; }
  public synchronized void setSuccessors(Collection<DAGNode<? extends Job>> successors) {
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

  @Override
  public int hashCode() {
    return Objects.hashCode(name, job, successorNames);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    DAGNode<?> that = (DAGNode<?>) obj;
    return Objects.equal(name, that.name)
        && Objects.equal(job, that.job)
        && Objects.equal(successorNames, that.successorNames);
  }

  public String toJson() throws IOException {
    return JSONUtil.toJson(this);
  }

  public static DAGNode<? extends Job> fromJson(String json) throws IOException {
    return JSONUtil.toObject(json, new TypeReference<DAGNode<? extends Job>>() { });
  }

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws IOException {
    String sourceFile = "pig/src/main/resources/web/data/large-dag.json";
    String json = JSONUtil.readFile(sourceFile);
    List<DAGNode> nodes = JSONUtil.toObject(json, new TypeReference<List<DAGNode>>() { });
    JSONUtil.writeJson(sourceFile + "2", nodes);
  }
}
