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
package com.twitter.ambrose.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobClient;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigProgressNotificationListener;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.ScriptState;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.twitter.ambrose.model.DAGNode;
import com.twitter.ambrose.model.Event;
import com.twitter.ambrose.model.Job;
import com.twitter.ambrose.model.Workflow;
import com.twitter.ambrose.model.hadoop.MapReduceUtils;
import com.twitter.ambrose.service.StatsWriteService;

/**
 * PigProgressNotificationListener that collects plan and job information from within a Pig runtime,
 * builds Ambrose model objects, and passes the objects to an Ambrose StatsWriteService object. This
 * listener can be used regardless of what mode Ambrose is running in.
 *
 * @see EmbeddedAmbrosePigProgressNotificationListener for a sublclass that can be used to run an
 * embedded Abrose web server from Pig client process.
 *
 */
public class AmbrosePigProgressNotificationListener implements PigProgressNotificationListener {
  protected Log log = LogFactory.getLog(getClass());
  private StatsWriteService statsWriteService;
  private String workflowVersion;
  private List<Job> jobs = new ArrayList<Job>();
  private Map<String, DAGNode<PigJob>> dagNodeNameMap = Maps.newTreeMap();
  private Map<String, DAGNode<PigJob>> dagNodeJobIdMap = Maps.newTreeMap();
  private Set<String> completedJobIds = Sets.newHashSet();
  private JobClient jobClient;
  private PigStats.JobGraph jobGraph;

  /**
   * Initialize this class with an instance of StatsWriteService to push stats to.
   *
   * @param statsWriteService
   */
  public AmbrosePigProgressNotificationListener(StatsWriteService statsWriteService) {
    this.statsWriteService = statsWriteService;
  }

  protected StatsWriteService getStatsWriteService() { return statsWriteService; }

  /**
   * Called after the job DAG has been created, but before any jobs are fired.
   * @param plan the MROperPlan that represents the DAG of operations. Each operation will become
   * a MapReduce job when it's launched.
   */
  @Override
  public void initialPlanNotification(String scriptId, MROperPlan plan) {
    log.info("initialPlanNotification - scriptId " + scriptId + " plan " + plan);
    // Pig will make sure that jobClient and jobGraph is initialized before initialPlanNotification is called
    this.jobClient = PigStats.get().getJobClient();
    this.jobGraph = PigStats.get().getJobGraph();
    this.workflowVersion = PigStats.get().getPigProperties().getProperty("pig.logical.plan.signature");

    // For ambrose to work above 3 must be non-null
    Preconditions.checkNotNull(jobClient);
    Preconditions.checkNotNull(jobGraph);
    Preconditions.checkNotNull(workflowVersion);

    Map<OperatorKey, MapReduceOper> planKeys = plan.getKeys();

    // first pass builds all nodes
    for (Map.Entry<OperatorKey, MapReduceOper> entry : planKeys.entrySet()) {
      String nodeName = entry.getKey().toString();
      String[] aliases = toArray(ScriptState.get().getAlias(entry.getValue()).trim());
      String[] features = toArray(ScriptState.get().getPigFeature(entry.getValue()).trim());

      PigJob job = new PigJob();
      job.setAliases(aliases);
      job.setFeatures(features);

      DAGNode<PigJob> node = new DAGNode<PigJob>(nodeName, job);

      this.dagNodeNameMap.put(node.getName(), node);

      // this shows how we can get the basic info about all nameless jobs before any execute.
      // we can traverse the plan to build a DAG of this info
      log.info("initialPlanNotification: aliases: " + Arrays.toString(aliases) +
          ", name: " + node.getName() + ", features: " + Arrays.toString(features));
    }

    // second pass connects the edges
    for (Map.Entry<OperatorKey, MapReduceOper> entry : planKeys.entrySet()) {
      DAGNode node = this.dagNodeNameMap.get(entry.getKey().toString());
      List<DAGNode<? extends Job>> successorNodeList = Lists.newArrayList();
      List<MapReduceOper> successors = plan.getSuccessors(entry.getValue());

      if (successors != null) {
        for (MapReduceOper successor : successors) {
          DAGNode<? extends Job> successorNode =
              this.dagNodeNameMap.get(successor.getOperatorKey().toString());
          successorNodeList.add(successorNode);
        }
      }
      node.setSuccessors(successorNodeList);
    }

    MapReduceUtils.sendDagNodeNameMap(statsWriteService, scriptId, dagNodeNameMap);
  }

  /**
   * Called with a job is started. This is the first time that we are notified of a new jobId for a
   * launched job. Hence this method binds the jobId to the DAGNode and pushes a status event.
   * @param scriptId scriptId of the running script
   * @param assignedJobId the jobId assigned to the job started.
   */
  @Override
  public void jobStartedNotification(String scriptId, String assignedJobId) {
    log.info("jobStartedNotification - scriptId " + scriptId + "jobId " + assignedJobId);

    // for each job in the graph, check if the stats for a job with this name is found. If so, look
    // up it's scope and bind the jobId to the DAGNode with the same scope.
    for (JobStats jobStats : jobGraph) {
      if (assignedJobId.equals(jobStats.getJobId())) {
        log.info("jobStartedNotification - scope " + jobStats.getName() + " is jobId " + assignedJobId);
        DAGNode<PigJob> node = this.dagNodeNameMap.get(jobStats.getName());

        if (node == null) {
          log.warn("jobStartedNotification - unrecognized operator name found ("
              + jobStats.getName() + ") for jobId " + assignedJobId);
          return;
        } 

        PigJob job = node.getJob();
        job.setId(assignedJobId);
        MapReduceUtils.addMapReduceJobState(job, jobClient);

        dagNodeJobIdMap.put(job.getId(), node);
        MapReduceUtils.pushEvent(statsWriteService, scriptId, new Event.JobStartedEvent(node));
      }
    }
  }

  /**
   * Called when a job fails. Mark the job as failed and push a status event.
   * @param scriptId scriptId of the running script
   * @param stats JobStats for the failed job.
   */
  @Override
  public void jobFailedNotification(String scriptId, JobStats stats) {
    log.info("jobFailedNotification - scriptId " + scriptId);
    if (stats.getJobId() == null) {
      log.warn("jobId for failed job not found. This should only happen in local mode");
      return;
    }

    DAGNode<PigJob> node = dagNodeJobIdMap.get(stats.getJobId());
    if (node == null) {
      log.warn("Unrecognized jobId reported for failed job: " + stats.getJobId());
      return;
    }

    addCompletedJobStats(node.getJob(), stats);
    MapReduceUtils.pushEvent(statsWriteService, scriptId, new Event.JobFailedEvent(node));
  }

  /**
   * Called when a job completes. Mark the job as finished and push a status event.
   * @param scriptId scriptId of the running script
   * @param stats JobStats for the completed job.
   */
  @Override
  public void jobFinishedNotification(String scriptId, JobStats stats) {
    log.info("jobFinishedNotification - scriptId " + scriptId);
    DAGNode<PigJob> node = dagNodeJobIdMap.get(stats.getJobId());
    if (node == null) {
      log.warn("Unrecognized jobId reported for succeeded job: " + stats.getJobId());
      return;
    }

    addCompletedJobStats(node.getJob(), stats);
    MapReduceUtils.pushEvent(statsWriteService, scriptId, new Event.JobFinishedEvent(node));
  }

  /**
   * Called after the launch of the script is complete. This means that zero or more jobs have
   * succeeded and there is no more work to be done.
   *
   * @param scriptId scriptId of the running script
   * @param numJobsSucceeded how many jobs have succeeded
   */
  @Override
  public void launchCompletedNotification(String scriptId, int numJobsSucceeded) {
    Workflow workflow = new Workflow(scriptId, workflowVersion, jobs);
    try {
      outputStatsData(workflow);
    } catch (IOException e) {
      log.error("Exception outputting workflow", e);
    }
  }

  /**
   * Called throughout execution of the script with progress notifications.
   * @param scriptId scriptId of the running script
   * @param progress is an integer between 0 and 100 the represents percent completion
   */
  @Override
  public void progressUpdatedNotification(String scriptId, int progress) {
    log.info("progressUpdatedNotification - scriptId " + scriptId + " progress " + progress);
    // first we report the scripts progress
    Map<Event.WorkflowProgressField, String> eventData = Maps.newHashMap();
    eventData.put(Event.WorkflowProgressField.workflowProgress, Integer.toString(progress));
    MapReduceUtils.pushEvent(statsWriteService, scriptId, new Event.WorkflowProgressEvent(eventData));

    // then for each running job, we report the job progress
    for (DAGNode<PigJob> node : dagNodeNameMap.values()) {
      // don't send progress events for unstarted jobs
      if (node.getJob().getId() == null) { 
        continue;
      }
      //only push job progress events for a completed job once
      if(completedJobIds.contains(node.getJob().getId())) {
        continue;
      }

      MapReduceUtils.addMapReduceJobState(node.getJob(), jobClient);

      if (node.getJob().getMapReduceJobState() != null) {       
        MapReduceUtils.pushEvent(statsWriteService, scriptId, new Event.JobProgressEvent(node));

        if (node.getJob().getMapReduceJobState().isComplete()) {
          completedJobIds.add(node.getJob().getId());
        }
      }
    }
  }

  @Override
  public void launchStartedNotification(String scriptId, int numJobsToLaunch) { }

  @Override
  public void jobsSubmittedNotification(String scriptId, int numJobsSubmitted) { }

  @Override
  public void outputCompletedNotification(String scriptId, OutputStats outputStats) { }

  /**
   * Collects statistics from JobStats and builds a nested Map of values.
   */
  private void addCompletedJobStats(PigJob job, JobStats stats) {
    job.setJobStats(stats);
    jobs.add(job);
  }

  // Helper methods

  private void outputStatsData(Workflow workflow) throws IOException {
    if(log.isDebugEnabled()) {
      log.debug("Collected stats for script:\n" + Workflow.toJSON(workflow));
    }
  }

  private static String[] toArray(String string) {
    return string == null ? new String[0] : string.trim().split(",");
  }
}
