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
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sun.jersey.core.util.Base64;
import com.twitter.ambrose.model.PaginatedList;
import com.twitter.ambrose.model.WorkflowId;
import com.twitter.ambrose.model.WorkflowSummary;
import com.twitter.ambrose.model.WorkflowSummary.Status;
import com.twitter.ambrose.service.WorkflowIndexReadService;
import com.twitter.hraven.Cluster;
import com.twitter.hraven.Constants;
import com.twitter.hraven.Flow;
import com.twitter.hraven.datasource.FlowQueueService;
import com.twitter.hraven.rest.PaginatedResult;

/**
 * Implementaton of WorkflowIndexReadService that knows how to read workflow info from HRaven.
 */
public class HRavenWorkflowIndexReadService implements WorkflowIndexReadService {

  private FlowQueueService flowQueueService;

  /**
   * Creates a HRavenWorkflowIndexReadService
   */
  public HRavenWorkflowIndexReadService() {
    Configuration configuration = HBaseConfiguration.create();

    try {
      flowQueueService = new FlowQueueService(configuration);
    } catch (IOException e) {
      throw new RuntimeException("Could not instantiate hRaven FlowQueueService", e);
    }
  }

  @Override
  public Map<String, String> getClusters() throws IOException {
    Map<String, String> clusterMap = Maps.newHashMap();
    //TODO make this an api in hraven
    Properties props = new Properties();
    String filename = Constants.HRAVEN_CLUSTER_PROPERTIES_FILENAME;
    clusterMap.put("default", "default");
    try {
      //TODO : property file to be moved out from resources into config dir
      InputStream inp = Cluster.class.getResourceAsStream("/" + filename);
      if (inp == null) {
        throw new RuntimeException(filename
            + " for mapping clusters to cluster identifiers in hRaven does not exist");
      }
      props.load(inp);
      for (Entry<Object, Object> prop : props.entrySet()) {
        String clusterName = String.valueOf(prop.getValue());
        clusterMap.put(clusterName.replace("@", "_"), clusterName);
      }
    } catch (IOException e) {
      throw new RuntimeException(" Could not load properties file " + filename
          + " for mapping clusters to cluster identifiers in hRaven");
    }
    return clusterMap;
  }

  @Override
  public PaginatedList<WorkflowSummary> getWorkflows(String cluster, Status status,
      String username, int numResults, byte[] nextPageStart) throws IOException {

    List<WorkflowSummary> workflowSummaryList = Lists.newArrayList();
    PaginatedResult<Flow> flows =
        flowQueueService.getPaginatedFlowsForStatus(cluster, convertStatus(status), numResults,
            username, nextPageStart);

    for (Flow flow : flows.getValues()) {
      workflowSummaryList.add(toWorkflowSummary(flow));
    }

    PaginatedList<WorkflowSummary> paginatedList =
        new PaginatedList<WorkflowSummary>(workflowSummaryList);
    if (flows.getNextStartRow() != null) {
      paginatedList.setNextPageStart(new String(Base64.encode(flows.getNextStartRow())));
    }

    return paginatedList;
  }

  private static Flow.Status convertStatus(Status status) {
    return Flow.Status.valueOf(status.name());
  }

  private static Status convertStatus(Flow.Status status) {
    return Status.valueOf(status.name());
  }

  /** Generates a new instance from an hRaven {@link Flow}. */
  private static WorkflowSummary toWorkflowSummary(Flow flow) {
    WorkflowId id = toWorkflowId(flow);
    return new WorkflowSummary(id.toId(), flow.getUserName(), flow.getFlowName(),
        Status.valueOf(flow.getQueueKey().getStatus().name()), flow.getProgress(),
        id.getTimestamp());
  }

  private static WorkflowId toWorkflowId(Flow flow) {
    return new WorkflowId(flow.getCluster(), flow.getUserName(), flow.getFlowKey().getAppId(),
        flow.getRunId(), flow.getQueueKey().getTimestamp(), flow.getQueueKey().getFlowId());
  }
}
