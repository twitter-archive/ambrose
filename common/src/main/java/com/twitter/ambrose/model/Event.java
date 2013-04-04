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

import com.twitter.ambrose.util.JSONUtil;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.core.type.TypeReference;

/**
 * Class that represents a Event of a given Type. Each one of these created will have
 * a unique id that increments up for each object. eventIds will always be >= 0.
 * The data associated with the event currently can be anything.
 *
 * @author billg
 */
public class Event<T> {
  private static AtomicInteger NEXT_ID = new AtomicInteger();

  public static enum Type { JOB_STARTED, JOB_FINISHED, JOB_FAILED, JOB_PROGRESS, WORKFLOW_PROGRESS};

  public static enum WorkflowProgressField {
    workflowProgress;
  }

  private long timestamp;
  private int id;
  private Type type;
  private T payload;

  private Event(Type type, T payload) {
    this.id = NEXT_ID.incrementAndGet();
    this.timestamp = System.currentTimeMillis();
    this.type = type;
    this.payload = payload;
  }

  public Event(int eventId, long timestamp, Type type, T payload) {
    this.id = eventId;
    this.timestamp = timestamp;
    this.type = type;
    this.payload = payload;
  }

  public long getTimestamp() { return timestamp; }
  public int getId() { return id; }
  public Type getType() { return type; }
  public Object getPayload() { return payload; }

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws IOException {
    String json = JSONUtil.readFile("pig/src/main/resources/web/data/small-events.json");
    List<Event> events = JSONUtil.toObject(json, new TypeReference<List<Event>>() { });
    for (Event event : events) {
      // useful if we need to read a file, add a field, output and re-generate
    }
    JSONUtil.writeJson("pig/src/main/resources/web/data/small-events.json2", events);
  }

  /**
   * Helper method to create instances of the proper event. It is the reposibility of the caller to
   * assure that their types are aligned with
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
    public JobStartedEvent(DAGNode<? extends Job> eventData) {
      super(Type.JOB_STARTED, eventData);
    }
  }

  public static class JobProgressEvent extends Event<DAGNode<? extends Job>> {
    public JobProgressEvent(DAGNode<? extends Job> eventData) {
      super(Type.JOB_PROGRESS, eventData);
    }
  }

  public static class JobFinishedEvent extends Event<DAGNode<? extends Job>> {
    public JobFinishedEvent(DAGNode<? extends Job> eventData) {
      super(Type.JOB_FINISHED, eventData);
    }
  }

  public static class JobFailedEvent extends Event<DAGNode<? extends Job>> {
    public JobFailedEvent(DAGNode<? extends Job> eventData) {
      super(Type.JOB_FAILED, eventData);
    }
  }

  public static class WorkflowProgressEvent extends Event<Map<WorkflowProgressField, String>> {
    public WorkflowProgressEvent(Map<WorkflowProgressField, String> eventData) {
      super(Type.WORKFLOW_PROGRESS, eventData);
    }
  }
}
