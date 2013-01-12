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
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class that represents a WorkflowEvent of a given EVENT_TYPE. Each one of these created will have
 * a unique eventId that increments up for each object. eventIds will always be >= 0.
 * The data associated with the event currently can be anything.
 * TODO: can we type eventData better?
 *
 * @author billg
 */
public class WorkflowEvent {
  private static AtomicInteger NEXT_ID = new AtomicInteger();

  public static enum EVENT_TYPE { JOB_STARTED, JOB_FINISHED, JOB_FAILED, JOB_PROGRESS, WORKFLOW_PROGRESS};

  private long timestamp;
  private int eventId;
  private String runtime;
  private EVENT_TYPE eventType;
  private Object eventData;

  public WorkflowEvent(EVENT_TYPE eventType, Object eventData, String runtime) {
    this.eventId = NEXT_ID.incrementAndGet();
    this.timestamp = System.currentTimeMillis();
    this.eventType = eventType;
    this.eventData = eventData;
    this.runtime = runtime;
  }

  public WorkflowEvent(int eventId, long timestamp, EVENT_TYPE eventType, Object eventData, String runtime) {
    this.eventId = eventId;
    this.timestamp = timestamp;
    this.eventType = eventType;
    this.eventData = eventData;
    this.runtime = runtime;
  }

  public long getTimestamp() { return timestamp; }
  public int getEventId() { return eventId; }
  public EVENT_TYPE getEventType() { return eventType; }
  public Object getEventData() { return eventData; }
  public String getRuntime() { return runtime; }

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws IOException {
    String json = JSONUtil.readFile("pig/src/main/resources/web/data/small-events.json");
    List<WorkflowEvent> events =
      (List<WorkflowEvent>)JSONUtil.readJson(json, new TypeReference<List<WorkflowEvent>>() { });
//    for (WorkflowEvent event : events) {
//      // useful if we need to read a file, add a field, output and re-generate
//    }

    JSONUtil.writeJson("pig/src/main/resources/web/data/small-events.json2", events);
  }
}
