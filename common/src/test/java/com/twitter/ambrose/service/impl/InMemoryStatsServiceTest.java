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
package com.twitter.ambrose.service.impl;

import com.twitter.ambrose.service.WorkflowEvent;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author billg
 */
public class InMemoryStatsServiceTest {

  private InMemoryStatsService service;

  private final String workflowId = "id1";
  private final WorkflowEvent[] testEvents = new WorkflowEvent[] {
    new WorkflowEvent(WorkflowEvent.EVENT_TYPE.JOB_STARTED, "jobIdFoo", "someRuntime"),
    new WorkflowEvent(WorkflowEvent.EVENT_TYPE.JOB_PROGRESS, "50", "someRuntime"),
    new WorkflowEvent(WorkflowEvent.EVENT_TYPE.JOB_FINISHED, "done", "someRuntime")
  };

  @Before
  public void setup() {
    service = new InMemoryStatsService();
  }

  @Test
  public void testGetAllEvents() throws IOException {
    for(WorkflowEvent event : testEvents) {
      service.pushEvent(workflowId, event);
    }

    Collection<WorkflowEvent> events = service.getEventsSinceId(workflowId, -1);
    Iterator<WorkflowEvent> foundEvents = events.iterator();

    assertTrue("No events returned", foundEvents.hasNext());
    for(WorkflowEvent sentEvent : testEvents) {
      assertEqualWorkflows(sentEvent, foundEvents.next());
    }
    assertFalse("Wrong number of events returned", foundEvents.hasNext());
  }

  @Test
  public void testGetEventsSince() throws IOException {
    for(WorkflowEvent event : testEvents) {
      service.pushEvent(workflowId, event);
    }

    // first, peek at the first eventId
    Collection<WorkflowEvent> allEvents = service.getEventsSinceId(workflowId, -1);
    int sinceId = allEvents.iterator().next().getEventId();

    // get all events since the first
    Collection<WorkflowEvent> events = service.getEventsSinceId(workflowId, sinceId);
    Iterator<WorkflowEvent> foundEvents = events.iterator();

    assertEquals("Wrong number of events returned", testEvents.length - 1, events.size());
    for(WorkflowEvent sentEvent : testEvents) {
      if (sentEvent.getEventId() <= sinceId) { continue; }

      assertEqualWorkflows(sentEvent, foundEvents.next());
    }
    assertFalse("Wrong number of events returned", foundEvents.hasNext());
  }

  private void assertEqualWorkflows(WorkflowEvent expected, WorkflowEvent found) {
    assertEquals("Wrong eventId found", expected.getEventId(), found.getEventId());
    assertEquals("Wrong eventType found", expected.getEventType(), found.getEventType());
    assertEquals("Wrong eventData found", expected.getEventData(), found.getEventData());
  }
}
