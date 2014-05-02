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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.twitter.ambrose.model.DAGNode;
import com.twitter.ambrose.model.Event;
import com.twitter.ambrose.model.Job;
import com.twitter.ambrose.model.PaginatedList;
import com.twitter.ambrose.model.WorkflowSummary;
import com.twitter.ambrose.service.StatsReadService;
import com.twitter.ambrose.service.StatsWriteService;
import com.twitter.ambrose.service.WorkflowIndexReadService;
import com.twitter.ambrose.util.JSONUtil;

/**
 * In-memory implementation of both StatsReadService and StatsWriteService. Used when stats
 * collection and stats serving are happening within the same VM. This class is intended to run in a
 * VM that only handles a single workflow. Hence it ignores workflowId.
 * <p/>
 * Upon job completion this class can optionally write all json data to disk. This is useful for
 * debugging. The written files can also be replayed in the Ambrose UI without re-running the Job
 * via the <code>bin/demo</code> script. To write all json data to disk, set the following values as
 * system properties using <code>-D</code>:
 * <pre>
 *   <ul>
 *     <li><code>{@value #DUMP_WORKFLOW_FILE_PARAM}</code> - file in which to write the workflow
 * json.</li>
 *     <li><code>{@value #DUMP_EVENTS_FILE_PARAM}</code> - file in which to write the events
 * json.</li>
 *   </ul>
 * </pre>
 */
public class InMemoryStatsService implements StatsReadService, StatsWriteService<Job>,
    WorkflowIndexReadService {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryStatsService.class);
  private static final String DUMP_WORKFLOW_FILE_PARAM = "ambrose.write.dag.file";
  private static final String DUMP_EVENTS_FILE_PARAM = "ambrose.write.events.file";
  private final WorkflowSummary summary = new WorkflowSummary(null,
      System.getProperty("user.name", "unknown"), "unknown", null, 0, System.currentTimeMillis());
  private final PaginatedList<WorkflowSummary> summaries =
      new PaginatedList<WorkflowSummary>(ImmutableList.of(summary));
  private boolean jobFailed = false;
  private Map<String, DAGNode<Job>> dagNodeNameMap = Maps.newHashMap();
  private SortedMap<Integer, Event> eventMap = new ConcurrentSkipListMap<Integer, Event>();
  private Writer workflowWriter;
  private Writer eventsWriter;
  private boolean eventWritten = false;

  public InMemoryStatsService() {
    String dumpWorkflowFileName = System.getProperty(DUMP_WORKFLOW_FILE_PARAM);
    String dumpEventsFileName = System.getProperty(DUMP_EVENTS_FILE_PARAM);

    if (dumpWorkflowFileName != null) {
      try {
        workflowWriter = new PrintWriter(dumpWorkflowFileName);
      } catch (FileNotFoundException e) {
        LOG.error("Could not create dag PrintWriter at " + dumpWorkflowFileName, e);
      }
    }

    if (dumpEventsFileName != null) {
      try {
        eventsWriter = new PrintWriter(dumpEventsFileName);
      } catch (FileNotFoundException e) {
        LOG.error("Could not create events PrintWriter at " + dumpEventsFileName, e);
      }
    }
  }

  @Override
  public synchronized void sendDagNodeNameMap(String workflowId,
      Map<String, DAGNode<Job>> dagNodeNameMap) throws IOException {
    this.summary.setId(workflowId);
    this.summary.setStatus(WorkflowSummary.Status.RUNNING);
    this.summary.setProgress(0);
    this.dagNodeNameMap = dagNodeNameMap;
    writeJsonDagNodenameMapToDisk(dagNodeNameMap);
  }

  @Override
  public synchronized void pushEvent(String workflowId, Event event) throws IOException {
    eventMap.put(event.getId(), event);
    switch (event.getType()) {
      case WORKFLOW_PROGRESS:
        Event.WorkflowProgressEvent workflowProgressEvent = (Event.WorkflowProgressEvent) event;
        String progressString =
            workflowProgressEvent.getPayload().get(Event.WorkflowProgressField.workflowProgress);
        int progress = Integer.parseInt(progressString);
        summary.setProgress(progress);
        if (progress == 100) {
          summary.setStatus(jobFailed
              ? WorkflowSummary.Status.FAILED
              : WorkflowSummary.Status.SUCCEEDED);
        }
        break;
      case JOB_FAILED:
        jobFailed = true;
      default:
        // nothing
    }
    writeJsonEventToDisk(event);
  }

  @Override
  public synchronized Map<String, DAGNode<Job>> getDagNodeNameMap(String workflowId) {
    return dagNodeNameMap;
  }

  @Override
  public synchronized Collection<Event> getEventsSinceId(String workflowId, int sinceId) {
    int minId = sinceId >= 0 ? sinceId + 1 : sinceId;
    return eventMap.tailMap(minId).values();
  }

  @Override
  public Map<String, String> getClusters() throws IOException {
    return ImmutableMap.of("default", "default");
  }

  @Override
  public synchronized PaginatedList<WorkflowSummary> getWorkflows(String cluster,
      WorkflowSummary.Status status, String userId, int numResults, byte[] startKey)
      throws IOException {
    return summaries;
  }

  private void writeJsonDagNodenameMapToDisk(Map<String, DAGNode<Job>> dagNodeNameMap)
      throws IOException {
    if (workflowWriter != null && dagNodeNameMap != null) {
      JSONUtil.writeJson(workflowWriter, dagNodeNameMap.values());
    }
  }

  private void writeJsonEventToDisk(Event event) throws IOException {
    if (eventsWriter != null && event != null) {
      eventsWriter.write(!eventWritten ? "[ " : ", ");
      JSONUtil.writeJson(eventsWriter, event);
      eventsWriter.flush();
      eventWritten = true;
    }
  }

  public void flushJsonToDisk() throws IOException {
    if (workflowWriter != null) {
      workflowWriter.close();
    }
    if (eventsWriter != null) {
      if (eventWritten) {
        eventsWriter.write(" ]\n");
      }
      eventsWriter.close();
    }
  }

  @Override
  public Collection<Event> getEventsSinceId(String workflowId, int sinceId,
    int maxEvents) throws IOException {
    int minId = sinceId >= 0 ? sinceId + 1 : sinceId;
    return Lists.newArrayList(Iterables.limit(eventMap.tailMap(minId).values(), maxEvents));
  }

  @Override
  public void initWriteService(Properties properties) throws IOException {
    // Do nothing
  }

  @Override
  public void initReadService(Properties properties) throws IOException {
    // Do nothing
  }
}
