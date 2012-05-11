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
  private EVENT_TYPE eventType;
  private Object eventData;

  public WorkflowEvent(EVENT_TYPE eventType, Object eventData) {
    this.eventId = NEXT_ID.incrementAndGet();
    this.timestamp = System.currentTimeMillis();
    this.eventType = eventType;
    this.eventData = eventData;
  }

  public long getTimestamp() { return timestamp; }
  public int getEventId() { return eventId; }
  public EVENT_TYPE getEventType() { return eventType; }
  public Object getEventData() { return eventData; }
}
