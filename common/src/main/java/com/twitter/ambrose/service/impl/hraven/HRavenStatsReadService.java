package com.twitter.ambrose.service.impl.hraven;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.base.Preconditions;

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
@SuppressWarnings("rawtypes")
public class HRavenStatsReadService implements StatsReadService {
  private static final Log LOG = LogFactory.getLog(HRavenStatsReadService.class);

  private FlowQueueService flowQueueService;
  private FlowEventService flowEventService;

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

  /**
   * Gets the dag nodes for this workflowId. Returns null if the workflow does not exist.
   *
   * @param workflowId the id of the workflow
   * @return a map of nodeIds to DAGNodes
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
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

  @Override
  public List<Event> getEventsSinceId(String workflowId, int eventId)
      throws IOException {
    return getEventsSinceId(workflowId, eventId, Integer.MAX_VALUE);
  }

  @Override
  public List<Event> getEventsSinceId(String workflowId, int eventId, int maxEvents)
      throws IOException {

    Preconditions.checkArgument(maxEvents > 0);

    WorkflowId id = WorkflowId.parseString(workflowId);
    FlowEventKey flowEventKey = new FlowEventKey(toFlowKey(id), eventId);
    List<FlowEvent> flowEventList = flowEventService.getFlowEventsSince(flowEventKey);

    // TODO push this limit into the FlowEventService
    int numElems = 0;
    List<Event> workflowEvents = new ArrayList<Event>();
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

  /**
   * Utility class to test working with Hraven
   */
  private static class Test {

    /**
     * Main method for testing
     */
    public static void main(String[] args) throws IOException {

      //cluster!userName!appId!runId!timestamp!flowId
      String workflowId = args[0];
      HRavenStatsReadService service = new HRavenStatsReadService();

      Map<String, DAGNode> dagMap = service.getDagNodeNameMap(workflowId);
      if (dagMap == null) {
        print("No dagNodeNameMap found for " + workflowId);
      } else {
        print(String.format("Found %d dapMap entries", dagMap.size()));
        for (Map.Entry<String, DAGNode> entry : dagMap.entrySet()) {
          DAGNode node = entry.getValue();
          String jobId = node.getJob() != null ? node.getJob().getId() : null;
          print(String.format("%s: nodeName=%s jobId=%s successors=%s",
              entry.getKey(), node.getName(), jobId, node.getSuccessorNames()));
        }
      }

      List<Event> events = service.getEventsSinceId(workflowId, -1);
      print(String.format("Found %d events", events.size()));
      for (Event event : events) {
        print(String.format("%d %d %s %s",
            event.getId(), event.getTimestamp(), event.getType(), event.getPayload()));
      }
    }

    private static void print(String object) { System.out.println(object); }
  }
}
