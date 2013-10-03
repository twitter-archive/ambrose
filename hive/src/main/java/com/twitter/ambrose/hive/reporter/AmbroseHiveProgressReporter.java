/*
Copyright 2013, Lorand Bendig

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
package com.twitter.ambrose.hive.reporter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.twitter.ambrose.model.DAGNode;
import com.twitter.ambrose.model.Event;
import com.twitter.ambrose.model.Job;
import com.twitter.ambrose.service.StatsWriteService;

/**
 * 
 * This abstract reporter maintains a shared global state between hooks.
 * It collects job information and job/workflow status reports from the hooks
 * and passes them to an Ambrose StatsWriteService object during the life cycle
 * of the running Hive script.
 * <br><br>
 * See {@link EmbeddedAmbroseHiveProgressReporter} for a sublclass that can be used to run an
 * embedded Ambrose web server from Hive client process.
 * 
 * @author Lorand Bendig <lbendig@gmail.com>
 * 
 */
public abstract class AmbroseHiveProgressReporter {

  private static final Log LOG = LogFactory.getLog(AmbroseHiveProgressReporter.class);

  /** DAG and workflow progress shared between ClientStatsPublisher threads */
  private Map<String, DAGNode<Job>> nodeIdToDAGNode;
  private Map<String, Integer> jobIdToProgress;
  private Map<String, String> jobIdToNodeId;
  private List<Job> jobs;
  private Set<String> completedJobIds;

  private int totalMRJobs;
  private String workflowVersion;

  /** holds all events within a script (for all workflows) */
  protected SortedMap<Integer, Event<?>> allEvents = new ConcurrentSkipListMap<Integer, Event<?>>();

  /** holds all dagNodes within a script (for all workflows) */
  protected SortedMap<String, DAGNode<Job>> allDagNodes = new ConcurrentSkipListMap<String, DAGNode<Job>>();
 
  private StatsWriteService statsWriteService;

  AmbroseHiveProgressReporter(StatsWriteService statsWriteService) {
    this.statsWriteService = statsWriteService;
    init();
  }

  private void init() {
    jobIdToProgress = new ConcurrentHashMap<String, Integer>();
    jobIdToNodeId = new ConcurrentHashMap<String, String>();
    jobs = new CopyOnWriteArrayList<Job>();
    completedJobIds = new CopyOnWriteArraySet<String>();
    totalMRJobs = 0;
    workflowVersion = null;
  }
  
  /**
   * Subclasses may have additional field initialization
   */
  public abstract void resetAdditionals();
  
  /**
   * Saves events and DAGNodes for a given workflow
   */
  public abstract void saveEventStack();

  /**
   * Restores events and DAGNodes of all workflows within a script This enables
   * to replay all the workflows when the script finishes
   */
  public abstract void restoreEventStack();
  
  protected StatsWriteService<? extends Job> getStatsWriteService() {
    return statsWriteService;
  }
  
  public void reset() {
    init();
    nodeIdToDAGNode = new ConcurrentSkipListMap<String, DAGNode<Job>>();
    sendDagNodeNameMap(null, nodeIdToDAGNode);
    resetAdditionals(); //TODO order?
  }

  public Map<String, DAGNode<Job>> getNodeIdToDAGNode() {
    return nodeIdToDAGNode;
  }

  public void setNodeIdToDAGNode(Map<String, DAGNode<Job>> nodeIdToDAGNode) {
    this.nodeIdToDAGNode = nodeIdToDAGNode;
  }

  public void addJobIdToProgress(String jobID, int progressUpdate) {
    jobIdToProgress.put(jobID, progressUpdate);
  }

  public Map<String, String> getJobIdToNodeId() {
    return jobIdToNodeId;
  }

  public void addJobIdToNodeId(String jobId, String nodeId) {
    jobIdToNodeId.put(jobId, nodeId);
  }

  public DAGNode<Job> getDAGNodeFromNodeId(String nodeId) {
    return nodeIdToDAGNode.get(nodeId);
  }

  /**
   * Overall progress of the submitted script
   * 
   * @return a number between 0 and 100
   */
  public synchronized int getOverallProgress() {
    int sum = 0;
    for (int progress : jobIdToProgress.values()) {
      sum += progress;
    }
    return sum / totalMRJobs;
  }

  public void addJob(Job job) {
    jobs.add(job);
  }

  public List<Job> getJobs() {
    return jobs;
  }

  public String getWorkflowVersion() {
    return workflowVersion;
  }

  public void setWorkflowVersion(String workflowVersion) {
    this.workflowVersion = workflowVersion;
  }

  /**
   * Forwards an event to the Ambrose server
   * 
   * @param queryId
   * @param event
   */
  public void pushEvent(String queryId, Event<?> event) {
    try {
      statsWriteService.pushEvent(queryId, event);
    }
    catch (IOException e) {
      LOG.error("Couldn't send event to StatsWriteService!", e);
    }
  }

  public void sendDagNodeNameMap(String queryId, Map<String, DAGNode<Job>> nodeIdToDAGNode) {
    try {
      statsWriteService.sendDagNodeNameMap(queryId, nodeIdToDAGNode);
    }
    catch (IOException e) {
      LOG.error("Couldn't send DAGNode information to server!", e);
    }
  }

  public void setTotalMRJobs(int totalMRJobs) {
    this.totalMRJobs = totalMRJobs;
  }

  public Set<String> getCompletedJobIds() {
    return completedJobIds;
  }

  public void addCompletedJobIds(String jobID) {
    completedJobIds.add(jobID);
  }

}
