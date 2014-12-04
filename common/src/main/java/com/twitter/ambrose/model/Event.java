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
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;

import com.twitter.ambrose.util.JSONUtil;

/**
 * Class that represents a Event of a given Type. Each one of these created will have
 * a unique id that increments up for each object. eventIds will always be >= 0.
 * The data associated with the event currently can be anything.
 *
 * @author billg
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value=Event.WorkflowProgressEvent.class, name="WORKFLOW_PROGRESS"),
    @JsonSubTypes.Type(value=Event.JobStartedEvent.class, name="JOB_STARTED"),
    @JsonSubTypes.Type(value=Event.JobProgressEvent.class, name="JOB_PROGRESS"),
    @JsonSubTypes.Type(value=Event.JobFinishedEvent.class, name="JOB_FINISHED"),
    @JsonSubTypes.Type(value=Event.JobFailedEvent.class, name="JOB_FAILED")
})
public class Event<T> {
  private static AtomicInteger NEXT_ID = new AtomicInteger();

  public static enum Type { JOB_STARTED, JOB_FINISHED, JOB_FAILED, JOB_PROGRESS, WORKFLOW_PROGRESS }

  public static enum WorkflowProgressField {
    workflowProgress
  }

  private int id;
  @JsonIgnore
  private Type type;
  private long timestamp;
  private T payload;

  public Event(int eventId, Type type, long timestamp, T payload) {
    this.id = eventId;
    this.type = type;
    this.timestamp = timestamp;
    this.payload = payload;
  }

  private Event(Type type, T payload) {
    this(NEXT_ID.incrementAndGet(), type, System.currentTimeMillis(), payload);
  }

  public Event() {}

  public int getId() { return id; }
  public Type getType() { return type; }
  public long getTimestamp() { return timestamp; }
  public T getPayload() { return payload; }

  public String toJson() throws IOException {
    return JSONUtil.toJson(this);
  }

  public static Event<?> fromJson(String json) throws IOException {
    return JSONUtil.toObject(json, new TypeReference<Event<?>>() {});
  }

  /**
   * Helper method to create instances of the proper event. It is the responsibility of the caller
   * to assure that their types are aligned with the object passed.
   * @param type
   * @param data
   * @return
   */
  @SuppressWarnings("unchecked")
  public static Event create(Type type, Object data) {
    switch (type) {
      case JOB_STARTED:
        return new JobStartedEvent((DAGNode<Job>) data);
      case JOB_PROGRESS:
        return new JobStartedEvent((DAGNode<Job>) data);
      case JOB_FINISHED:
        return new JobFinishedEvent((DAGNode<Job>) data);
      case JOB_FAILED:
        return new JobStartedEvent((DAGNode<Job>) data);
      case WORKFLOW_PROGRESS:
        return new JobStartedEvent((DAGNode<Job>) data);
      default:
        throw new IllegalArgumentException(
            String.format("Unknown event type %s for data payload %s", type, data));
    }
  }

  public static class JobStartedEvent extends Event<DAGNode<? extends Job>> {
    @JsonCreator
    public JobStartedEvent(@JsonProperty("payload") DAGNode<? extends Job> payload) {
      super(Type.JOB_STARTED, payload);
    }
  }

  public static class JobProgressEvent extends Event<DAGNode<? extends Job>> {
    @JsonCreator
    public JobProgressEvent(@JsonProperty("payload") DAGNode<? extends Job> payload) {
      super(Type.JOB_PROGRESS, payload);
    }
  }

  public static class JobFinishedEvent extends Event<DAGNode<? extends Job>> {
    @JsonCreator
    public JobFinishedEvent(@JsonProperty("payload") DAGNode<? extends Job> payload) {
      super(Type.JOB_FINISHED, payload);
    }
  }

  public static class JobFailedEvent extends Event<DAGNode<? extends Job>> {
    @JsonCreator
    public JobFailedEvent(@JsonProperty("payload") DAGNode<? extends Job> payload) {
      super(Type.JOB_FAILED, payload);
    }
  }

  public static class WorkflowProgressEvent extends Event<Map<WorkflowProgressField, String>> {
    @JsonCreator
    public WorkflowProgressEvent(@JsonProperty("payload") Map<WorkflowProgressField, String> payload) {
      super(Type.WORKFLOW_PROGRESS, payload);
    }
  }

  public static void main(String[] args) throws IOException {
    String json = JSONUtil.readFile("pig/src/main/resources/web/data/small-events.json");
    List<Event> events = JSONUtil.toObject(json, new TypeReference<List<Event>>() { });
    for (Event event : events) {
      // useful if we need to read a file, add a field, output and re-generate
    }
    JSONUtil.writeJson("pig/src/main/resources/web/data/small-events.json2", events);
  }
}
