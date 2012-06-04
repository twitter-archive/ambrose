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

import com.twitter.ambrose.model.JobInfo;
import com.twitter.ambrose.model.WorkflowInfo;
import com.twitter.ambrose.service.DAGNode;
import com.twitter.ambrose.service.StatsWriteService;
import com.twitter.ambrose.service.WorkflowEvent;
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

  private static final String RUNTIME = "pig";

  private StatsWriteService statsWriteService;

  private String workflowVersion;
  private List<JobInfo> jobInfoList = new ArrayList<JobInfo>();
  private Map<String, DAGNode> dagNodeNameMap = new TreeMap<String, DAGNode>();

  private HashSet<String> completedJobIds = new HashSet<String>();

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
      DAGNode node = new DAGNode(entry.getKey().toString(),
        toArray(ScriptState.get().getAlias(entry.getValue())),
        toArray(ScriptState.get().getPigFeature(entry.getValue())), RUNTIME);

      this.dagNodeNameMap.put(node.getName(), node);

      // this shows how we can get the basic info about all nameless jobs before any execute.
      // we can traverse the plan to build a DAG of this info
      log.info("initialPlanNotification: alias: " + toString(node.getAliases())
              + ", name: " + node.getName() + ", feature: " + toString(node.getFeatures()));
    }

    // second pass connects the edges
    for (Map.Entry<OperatorKey, MapReduceOper> entry : planKeys.entrySet()) {
      DAGNode node = this.dagNodeNameMap.get(entry.getKey().toString());
      List<DAGNode> successorNodeList = new ArrayList<DAGNode>();
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
        DAGNode node = this.dagNodeNameMap.get(jobStats.getName());

        if (node == null) {
          log.warn("jobStartedNotification - unrecorgnized operator name found ("
                  + jobStats.getName() + ") for jobId " + assignedJobId);
        } else {
          node.setJobId(assignedJobId);
          pushEvent(scriptId, WorkflowEvent.EVENT_TYPE.JOB_STARTED, node);

          Map<String, String> progressMap = buildJobStatusMap(assignedJobId);
          if (progressMap != null) {
            pushEvent(scriptId, WorkflowEvent.EVENT_TYPE.JOB_PROGRESS, progressMap);
          }
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
    JobInfo jobInfo = collectStats(scriptId, stats);
    jobInfoList.add(jobInfo);
    pushEvent(scriptId, WorkflowEvent.EVENT_TYPE.JOB_FAILED, jobInfo);
  }

  /**
   * Called when a job completes. Mark the job as finished and push a status event.
   * @param scriptId scriptId of the running script
   * @param stats JobStats for the completed job.
   */
  @Override
  public void jobFinishedNotification(String scriptId, JobStats stats) {
    JobInfo jobInfo = collectStats(scriptId, stats);
    jobInfoList.add(jobInfo);
    pushEvent(scriptId, WorkflowEvent.EVENT_TYPE.JOB_FINISHED, jobInfo);
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
      WorkflowInfo workflowInfo = new WorkflowInfo(scriptId, workflowVersion, jobInfoList);

      try {
        outputStatsData(workflowInfo);
      } catch (IOException e) {
        log.error("Exception outputting workflowInfo", e);
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
    Map<String, String> eventData = new HashMap<String, String>();
    eventData.put("scriptProgress", Integer.toString(progress));
    pushEvent(scriptId, WorkflowEvent.EVENT_TYPE.WORKFLOW_PROGRESS, eventData);

    // then for each running job, we report the job progress
    for (DAGNode node : dagNodeNameMap.values()) {
      // don't send progress events for unstarted jobs
      if (node.getJobId() == null) { continue; }

      Map<String, String> progressMap = buildJobStatusMap(node.getJobId());

      //only push job progress events for a completed job once
      if (progressMap != null && !completedJobIds.contains(node.getJobId())) {
        pushEvent(scriptId, WorkflowEvent.EVENT_TYPE.JOB_PROGRESS, progressMap);

        if ("true".equals(progressMap.get("isCompleted"))) {
          completedJobIds.add(node.getJobId());
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
   * Collects statistics from JobStats and builds a nested Map of values. Subsclass ond override
   * if you'd like to generate different stats.
   *
   * @param scriptId
   * @param stats
   * @return
   */
  protected JobInfo collectStats(String scriptId, JobStats stats) {

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

    return new PigJobInfo(stats, jobConfProperties);
  }

  private void outputStatsData(WorkflowInfo workflowInfo) throws IOException {
    if(log.isDebugEnabled()) {
      log.debug("Collected stats for script:\n" + WorkflowInfo.toJSON(workflowInfo));
    }
  }

  private void pushEvent(String scriptId, WorkflowEvent.EVENT_TYPE eventType, Object eventData) {
    try {
      statsWriteService.pushEvent(scriptId, new WorkflowEvent(eventType, eventData, RUNTIME));
    } catch (IOException e) {
      log.error("Couldn't send event to StatsWriteService", e);
    }
  }

  private Map<String, String> buildJobStatusMap(String jobId)  {
    JobClient jobClient = PigStats.get().getJobClient();

    try {
      RunningJob rj = jobClient.getJob(jobId);
      if (rj == null) {
        log.warn("Couldn't find job status for jobId=" + jobId);
        return null;
      }

      JobID jobID = rj.getID();
      TaskReport[] mapTaskReport = jobClient.getMapTaskReports(jobID);
      TaskReport[] reduceTaskReport = jobClient.getReduceTaskReports(jobID);
      Map<String, String> progressMap = new HashMap<String, String>();

      //TODO: change this into a typed JobProgress object so other workflows can produce similar data
      progressMap.put("jobId", jobId.toString());
      progressMap.put("jobName", rj.getJobName());
      progressMap.put("trackingUrl", rj.getTrackingURL());
      progressMap.put("isComplete", Boolean.toString(rj.isComplete()));
      progressMap.put("isSuccessful", Boolean.toString(rj.isSuccessful()));
      progressMap.put("mapProgress", Float.toString(rj.mapProgress()));
      progressMap.put("reduceProgress", Float.toString(rj.reduceProgress()));
      progressMap.put("totalMappers", Integer.toString(mapTaskReport.length));
      progressMap.put("totalReducers", Integer.toString(reduceTaskReport.length));
      return progressMap;
    } catch (IOException e) {
      log.error("Error getting job info.", e);
    }

    return null;
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