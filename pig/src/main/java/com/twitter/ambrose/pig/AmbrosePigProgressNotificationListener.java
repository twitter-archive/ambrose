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

import com.twitter.ambrose.model.DAGNode;
import com.twitter.ambrose.model.Event;
import com.twitter.ambrose.model.Job;
import com.twitter.ambrose.model.Workflow;
import com.twitter.ambrose.model.hadoop.MapReduceJobState;
import com.twitter.ambrose.service.StatsWriteService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigProgressNotificationListener;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.ScriptState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * PigProgressNotificationListener that collects plan and job information from within a Pig runtime,
 * builds Ambrose model objects, and passes the objects to an Ambrose StatsWriteService object. This
 * listener can be used regardless of what mode Ambrose is running in.
 *
 * @see EmbeddedAmbrosePigProgressNotificationListener for a sublclass that can be used to run an
 * embedded Abrose web server from Pig client process.
 *
 */
@SuppressWarnings("deprecation")
public class AmbrosePigProgressNotificationListener implements PigProgressNotificationListener {
  protected Log log = LogFactory.getLog(getClass());

  private StatsWriteService statsWriteService;

  private String workflowVersion;
  private List<Job> jobs = new ArrayList<Job>();
  private Map<String, DAGNode<PigJob>> dagNodeNameMap = Maps.newTreeMap();
  private Map<String, DAGNode<PigJob>> dagNodeJobIdMap = Maps.newTreeMap();

  private HashSet<String> completedJobIds = Sets.newHashSet();

  protected static enum JobProgressField {
    jobId, jobName, trackingUrl, isComplete, isSuccessful,
    mapProgress, reduceProgress, totalMappers, totalReducers;
  }

  /**
   * Intialize this class with an instance of StatsWriteService to push stats to.
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
    Map<OperatorKey, MapReduceOper>  planKeys = plan.getKeys();

    // first pass builds all nodes
    for (Map.Entry<OperatorKey, MapReduceOper> entry : planKeys.entrySet()) {
      String nodeName = entry.getKey().toString();
      String[] aliases = toArray(ScriptState.get().getAlias(entry.getValue()).trim());
      String[] features = toArray(ScriptState.get().getPigFeature(entry.getValue()).trim());

      DAGNode<PigJob> node = new DAGNode<PigJob>(nodeName, new PigJob(aliases, features));

      this.dagNodeNameMap.put(node.getName(), node);

      // this shows how we can get the basic info about all nameless jobs before any execute.
      // we can traverse the plan to build a DAG of this info
      log.info("initialPlanNotification: aliases: " + aliases + ", name: " + node.getName() +
          ", features: " + features);
    }

    // second pass connects the edges
    for (Map.Entry<OperatorKey, MapReduceOper> entry : planKeys.entrySet()) {
      DAGNode node = this.dagNodeNameMap.get(entry.getKey().toString());
      List<DAGNode> successorNodeList = Lists.newArrayList();
      List<MapReduceOper> successors = plan.getSuccessors(entry.getValue());

      if (successors != null) {
        for (MapReduceOper successor : successors) {
          DAGNode successorNode = this.dagNodeNameMap.get(successor.getOperatorKey().toString());
          successorNodeList.add(successorNode);
        }
      }

      node.setSuccessors(successorNodeList);
    }

    //TODO: upgrade to trunk pig which has scriptId and pass it here
    try {
      statsWriteService.sendDagNodeNameMap(null, this.dagNodeNameMap);
    } catch (IOException e) {
      log.error("Couldn't send dag to StatsWriteService", e);
    }
  }

  /**
   * Called with a job is started. This is the first time that we are notified of a new jobId for a
   * launched job. Hence this method binds the jobId to the DAGNode and pushes a status event.
   * @param scriptId scriptId of the running script
   * @param assignedJobId the jobId assigned to the job started.
   */
  @Override
  public void jobStartedNotification(String scriptId, String assignedJobId) {
    PigStats.JobGraph jobGraph = PigStats.get().getJobGraph();
    log.info("jobStartedNotification - jobId " + assignedJobId + ", jobGraph:\n" + jobGraph);

    // for each job in the graph, check if the stats for a job with this name is found. If so, look
    // up it's scope and bind the jobId to the DAGNode with the same scope.
    for (JobStats jobStats : jobGraph) {
      if (assignedJobId.equals(jobStats.getJobId())) {
        log.info("jobStartedNotification - scope " + jobStats.getName() + " is jobId " + assignedJobId);
        DAGNode<PigJob> node = this.dagNodeNameMap.get(jobStats.getName());

        if (node == null) {
          log.warn("jobStartedNotification - unrecognized operator name found ("
                  + jobStats.getName() + ") for jobId " + assignedJobId);
        } else {
          node.getJob().setId(assignedJobId);
          addMapReduceJobState(node.getJob());
          dagNodeJobIdMap.put(node.getJob().getId(), node);
          pushEvent(scriptId, new Event.JobStartedEvent(node));
        }
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
    DAGNode<PigJob> node = dagNodeJobIdMap.get(stats.getJobId());
    if (node == null) {
      log.warn("Unrecognized jobId reported for failed job: " + stats.getJobId());
      return;
    }

    addCompletedJobStats(node.getJob(), stats);
    pushEvent(scriptId, new Event.JobFailedEvent(node));
  }

  /**
   * Called when a job completes. Mark the job as finished and push a status event.
   * @param scriptId scriptId of the running script
   * @param stats JobStats for the completed job.
   */
  @Override
  public void jobFinishedNotification(String scriptId, JobStats stats) {
    DAGNode<PigJob> node = dagNodeJobIdMap.get(stats.getJobId());
    if (node == null) {
      log.warn("Unrecognized jobId reported for succeeded job: " + stats.getJobId());
      return;
    }

    addCompletedJobStats(node.getJob(), stats);
    pushEvent(scriptId, new Event.JobFinishedEvent(node));
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

    if (workflowVersion == null) {
      log.warn("scriptFingerprint not set for this script - not saving stats." );
    } else {
      Workflow workflow = new Workflow(scriptId, workflowVersion, jobs);

      try {
        outputStatsData(workflow);
      } catch (IOException e) {
        log.error("Exception outputting workflow", e);
      }
    }
  }

  /**
   * Called throught execution of the script with progress notifications.
   * @param scriptId scriptId of the running script
   * @param progress is an integer between 0 and 100 the represents percent completion
   */
  @Override
  public void progressUpdatedNotification(String scriptId, int progress) {

    // first we report the scripts progress
    Map<Event.WorkflowProgressField, String> eventData = Maps.newHashMap();
    eventData.put(Event.WorkflowProgressField.workflowProgress, Integer.toString(progress));
    pushEvent(scriptId, new Event.WorkflowProgressEvent(eventData));

    // then for each running job, we report the job progress
    for (DAGNode<PigJob> node : dagNodeNameMap.values()) {
      // don't send progress events for unstarted jobs
      if (node.getJob().getId() == null) { continue; }

      addMapReduceJobState(node.getJob());

      //only push job progress events for a completed job once
      if (node.getJob().getMapReduceJobState() != null && !completedJobIds.contains(node.getJob().getId())) {
        pushEvent(scriptId, new Event.JobProgressEvent(node));

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

    // put the job conf into a Properties object so we can serialize them
    Properties jobConfProperties = new Properties();
    if (stats.getInputs() != null && stats.getInputs().size() > 0 &&
      stats.getInputs().get(0).getConf() != null) {

      Configuration conf = stats.getInputs().get(0).getConf();
      for (Map.Entry<String, String> entry : conf) {
        jobConfProperties.setProperty(entry.getKey(), entry.getValue());
      }

      if (workflowVersion == null)  {
        workflowVersion = conf.get("pig.logical.plan.signature");
      }
    }

    job.setJobStats(stats);
    job.setConfiguration(jobConfProperties);
    jobs.add(job);
  }

  private void outputStatsData(Workflow workflow) throws IOException {
    if(log.isDebugEnabled()) {
      log.debug("Collected stats for script:\n" + Workflow.toJSON(workflow));
    }
  }

  private void pushEvent(String scriptId, Event event) {
    try {
      statsWriteService.pushEvent(scriptId, event);
    } catch (IOException e) {
      log.error("Couldn't send event to StatsWriteService", e);
    }
  }

  @SuppressWarnings("deprecation")
  private void addMapReduceJobState(PigJob pigJob) {
    JobClient jobClient = PigStats.get().getJobClient();

    try {
      RunningJob runningJob = jobClient.getJob(pigJob.getId());
      if (runningJob == null) {
        log.warn("Couldn't find job status for jobId=" + pigJob.getId());
        return;
      }

      JobID jobID = runningJob.getID();
      TaskReport[] mapTaskReport = jobClient.getMapTaskReports(jobID);
      TaskReport[] reduceTaskReport = jobClient.getReduceTaskReports(jobID);
      pigJob.setMapReduceJobState(new MapReduceJobState(runningJob, mapTaskReport, reduceTaskReport));
    } catch (IOException e) {
      log.error("Error getting job info.", e);
    }
  }

  private static String[] toArray(String string) {
    return string == null ? new String[0] : string.trim().split(",");
  }

  private static String toString(String[] array) {
    StringBuilder sb = new StringBuilder();
    for (String string : array) {
      if (sb.length() > 0) { sb.append(","); }
      sb.append(string);
    }
    return sb.toString();
  }
}
