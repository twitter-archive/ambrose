/*
Copyright 2014 Twitter, Inc.

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
package com.twitter.ambrose.service.impl.hraven;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.twitter.ambrose.model.DAGNode;
import com.twitter.ambrose.model.Event;
import com.twitter.ambrose.model.WorkflowId;
import com.twitter.ambrose.service.StatsReadService;
import com.twitter.ambrose.util.JSONUtil;
import com.twitter.hraven.Flow;
import com.twitter.hraven.FlowEvent;
import com.twitter.hraven.FlowEventKey;
import com.twitter.hraven.FlowKey;
import com.twitter.hraven.datasource.FlowEventService;
import com.twitter.hraven.datasource.FlowQueueService;

/**
 * Service that is able to read the dag and event from HRaven.
 */
public class HRavenStatsReadService implements StatsReadService {
  private static final Log LOG = LogFactory.getLog(HRavenStatsReadService.class);

  private final FlowQueueService flowQueueService;
  private final FlowEventService flowEventService;
  
  // By default, we return as many events as possible in getEventsSinceId api
  private static final int DEFAULT_MAX_EVENTS = Integer.MAX_VALUE;

  /**
   * Creates an HRavenStatsReadService
   */
  public HRavenStatsReadService() {
    Configuration configuration = HBaseConfiguration.create();

    try {
      flowQueueService = new FlowQueueService(configuration);
    } catch (IOException e) {
      throw new RuntimeException("Could not instantiate hRaven FlowQueueService", e);
    }

    try {
      flowEventService = new FlowEventService(configuration);
    } catch (IOException e) {
      throw new RuntimeException("Could not instantiate hRaven FlowEventService", e);
    }
  }
  
  @Override
  public void initReadService(Properties properties) throws IOException {
    // Do nothing
  }

  /**
   * Gets the dag nodes for this workflowId. Returns null if the workflow does not exist.
   *
   * @param workflowId the id of the workflow
   * @return a map of nodeIds to DAGNodes
   * @throws IOException
   */
  @SuppressWarnings("rawtypes")
  @Override
  public Map<String, DAGNode> getDagNodeNameMap(String workflowId) throws IOException {
    WorkflowId id = WorkflowId.parseString(workflowId);
    Flow flow = flowQueueService.getFlowFromQueue(
        id.getCluster(), id.getTimestamp(), id.getFlowId());

    if (flow == null) {
      return null;
    }

    // TODO This may not work nicely with multiple type of jobs
    // See: https://github.com/twitter/ambrose/pull/131
    Map<String, DAGNode> dagMap = JSONUtil.toObject(
        flow.getJobGraphJSON(), new TypeReference<Map<String, DAGNode>>() {
        });

    return dagMap;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public List<Event> getEventsSinceId(String workflowId, int eventId)
      throws IOException {
    return getEventsSinceId(workflowId, eventId, DEFAULT_MAX_EVENTS);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public List<Event> getEventsSinceId(String workflowId, int eventId, int maxEvents)
      throws IOException {
    Preconditions.checkArgument(maxEvents > 0);
    WorkflowId id = WorkflowId.parseString(workflowId);
    FlowEventKey flowEventKey = new FlowEventKey(toFlowKey(id), eventId);
    List<FlowEvent> flowEventList = flowEventService.getFlowEventsSince(flowEventKey);

    // TODO push this limit into the FlowEventService
    int numElems = 0;
    List<Event> workflowEvents = Lists.newArrayListWithCapacity(maxEvents);
    for (FlowEvent flowEvent : flowEventList) {
      if (numElems >= maxEvents) {
        break;
      }
      String eventDataJson = flowEvent.getEventDataJSON();
      try {
        Event event = Event.fromJson(eventDataJson);
        numElems++;
        workflowEvents.add(event);
      } catch (JsonMappingException e) {
        LOG.error("Could not deserialize json: " + eventDataJson, e);
      }
    }

    return workflowEvents;
  }

  private static FlowKey toFlowKey(WorkflowId id) {
    return new FlowKey(id.getCluster(), id.getUserId(), id.getAppId(), id.getRunId());
  }
}
