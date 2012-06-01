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

import com.twitter.ambrose.service.DAGNode;
import com.twitter.ambrose.service.DAGTransformer;
import com.twitter.ambrose.service.StatsReadService;
import com.twitter.ambrose.service.StatsWriteService;
import com.twitter.ambrose.service.WorkflowEvent;
import com.twitter.ambrose.util.JSONUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * In-memory implementation of both StatsReadService and StatsWriteService. Used when stats
 * collection and stats serving are happening within the same VM. This class is intended to run in
 * a VM that only handles a single workflow. Hence it ignores workflowId.
 * <P>
 * Upon job completion this class can optionally write all json data to disk. This is useful for
 * debugging. The written files can also be replayed in the Ambrose UI without re-running the Job
 * via the <pre>bin/demo</pre> script. To write all json data to disk, set the following values
 * as system properties using <pre>-D</pre>:
 * <ul>
 *   <li><pre>ambrose.write.dag.file</pre> file to write the dag data to</li>
 *   <li><pre>ambrose.write.events.file</pre> file to write the events data to</li>
 * </ul>
 * </P>
 *
 * @author billg
 */
public class InMemoryStatsService implements StatsReadService, StatsWriteService {
  public static final Log LOG = LogFactory.getLog(InMemoryStatsService.class);

  private static final String DUMP_DAG_FILE_PARAM = "ambrose.write.dag.file";
  private static final String DUMP_EVENTS_FILE_PARAM = "ambrose.write.events.file";

  private Map<String, DAGNode> dagNodeNameMap = new HashMap<String, DAGNode>();
  private SortedMap<Integer, WorkflowEvent> eventMap =
    new ConcurrentSkipListMap<Integer, WorkflowEvent>();

  private Writer dagWriter = null;
  private Writer eventsWriter = null;

  public InMemoryStatsService() {
    String dumpDagFileName = System.getProperty(DUMP_DAG_FILE_PARAM);
    String dumpEventsFileName = System.getProperty(DUMP_EVENTS_FILE_PARAM);

    if (dumpDagFileName != null) {
      try {
        dagWriter = new PrintWriter(dumpDagFileName);
      } catch (FileNotFoundException e) {
        LOG.error("Could not create dag PrintWriter at " + dumpDagFileName, e);
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
  public synchronized void sendDagNodeNameMap(String workflowId, Map<String, DAGNode> dagNodeNameMap) {
    this.dagNodeNameMap = dagNodeNameMap;
  }

  @Override
  public synchronized Map<String, DAGNode> getDagNodeNameMap(String workflowId) {
    return dagNodeNameMap;
  }

  @Override
  public synchronized Collection<WorkflowEvent> getEventsSinceId(String workflowId, int sinceId) {
    int minId = sinceId >= 0 ? sinceId + 1 : sinceId;

    SortedMap<Integer, WorkflowEvent> tailMap = eventMap.tailMap(minId);
    return tailMap.values();
  }

  @Override
  public synchronized void pushEvent(String workflowId, WorkflowEvent event) {
    eventMap.put(event.getEventId(), event);
  }

  public void writeJsonToDisk() throws IOException {

    if (dagWriter != null && dagNodeNameMap != null) {
      Collection<DAGNode> nodes = getDagNodeNameMap(null).values();
      JSONUtil.writeJson(dagWriter, nodes.toArray(new DAGNode[dagNodeNameMap.size()]));
      dagWriter.close();
    }

    if (eventsWriter != null && eventMap != null) {
      Collection<WorkflowEvent> events = getEventsSinceId(null, -1);
      JSONUtil.writeJson(eventsWriter, events.toArray(new WorkflowEvent[events.size()]));
      eventsWriter.close();
    }
  }
}
