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
package com.twitter.ambrose.hive;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.ClientStatsPublisher;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;

import com.twitter.ambrose.hive.reporter.EmbeddedAmbroseHiveProgressReporter;
import com.twitter.ambrose.model.DAGNode;
import com.twitter.ambrose.model.Event;
import com.twitter.ambrose.model.Event.WorkflowProgressField;
import com.twitter.ambrose.model.Job;
import com.twitter.ambrose.model.hadoop.MapReduceJobState;

import static com.twitter.ambrose.hive.reporter.AmbroseHiveReporterFactory.getEmbeddedProgressReporter;

/**
 * Hook that is invoked every <tt>hive.exec.counters.pull.interval</tt> seconds
 * to report a given job's status to
 * {@link com.twitter.ambrose.hive.reporter.AmbroseHiveProgressReporter AmbroseHiveProgressReporter}
 * <br>
 * If <tt>hive.exec.parallel</tt> is set each thread obtain an instance from
 * this class.
 *
 * @author Lorand Bendig <lbendig@gmail.com>
 *
 */
public class AmbroseHiveStatPublisher implements ClientStatsPublisher {

  private static final Log LOG = LogFactory.getLog(AmbroseHiveStatPublisher.class);

  /** Running job information */
  private final JobClient jobClient;
  private RunningJob rj;
  private MapReduceJobState jobProgress;

  private String nodeId;
  private JobID jobId;

  private final Map<WorkflowProgressField, String> eventData = Maps.newHashMapWithExpectedSize(1);
  private boolean init = true;

  public AmbroseHiveStatPublisher() throws IOException {
    Configuration conf = SessionState.get().getConf();
    this.jobClient = new JobClient(new JobConf(conf));
  }

  @Override
  public void run(Map<String, Double> counterValues, String jobIdStr) {
    if (init) {
      init(jobIdStr);
      init = false;
    }
    // send job statistics to the Ambrose server
    send(jobIdStr, counterValues);
  }

  private void init(String jobIDStr) {
    try {
      jobId = JobID.forName(jobIDStr);
      rj = jobClient.getJob(jobId);
      nodeId = AmbroseHiveUtil.getNodeIdFromJob(SessionState.get().getConf(), rj);
    }
    catch (IOException e) {
      LOG.error("Error getting running job for id : " + jobIDStr, e);
    }
  }

  private void send(String jobIDStr, Map<String, Double> counterValues) {

    EmbeddedAmbroseHiveProgressReporter reporter = getEmbeddedProgressReporter();
    Configuration conf = SessionState.get().getConf();
    String queryId = AmbroseHiveUtil.getHiveQueryId(conf);
    Map<String, DAGNode<Job>> nodeIdToDAGNode = reporter.getNodeIdToDAGNode();

    DAGNode<Job> dagNode = nodeIdToDAGNode.get(nodeId);
    if (dagNode == null) {
      LOG.warn("jobStartedNotification - unrecorgnized operator name found for " + "jobId "
          + jobIDStr);
      return;
    }
    HiveJob job = (HiveJob) dagNode.getJob();
    // a job has been started
    if (job.getId() == null) {
      // job identifier on GUI
      job.setId(AmbroseHiveUtil.asDisplayId(queryId, jobIDStr, nodeId));
      reporter.addJobIdToNodeId(jobIDStr, nodeId);
      reporter.pushEvent(queryId, new Event.JobStartedEvent(dagNode));
    }
    try {

      boolean isUpdated = updateJobState();
      if (isUpdated && !reporter.getCompletedJobIds().contains(jobIDStr)) {

        Event<DAGNode<? extends Job>> event = null;
        job.setMapReduceJobState(jobProgress);
        if (jobProgress.isComplete()) {
          event = new Event.JobFinishedEvent(dagNode);

          // update reduce progress to 1 if we have no reducers
          int reducers = jobProgress.getTotalReducers();
          if (reducers == 0) {
            jobProgress.setReduceProgress(1.0f);
          }

          // update job state
          job.setConfiguration(((HiveConf) conf).getAllProperties());
          job.setCounterGroupMap(AmbroseHiveUtil.counterGroupInfoMap(counterValues));

          reporter.addCompletedJobIds(jobIDStr);
          reporter.addJob(job);
        }
        else {
          event = new Event.JobProgressEvent(dagNode);
        }
        reporter.addJobIdToProgress(jobIDStr, getJobProgress());
        pushWorkflowProgress(queryId, reporter);
        reporter.pushEvent(queryId, event);
      }
    }
    catch (IOException e) {
      LOG.error("Error getting job info!", e);
    }
  }

  private void pushWorkflowProgress(String queryId, EmbeddedAmbroseHiveProgressReporter reporter) {
    eventData.put(WorkflowProgressField.workflowProgress,
        Integer.toString(reporter.getOverallProgress()));
    reporter.pushEvent(queryId, new Event.WorkflowProgressEvent(eventData));
  }

  private boolean updateJobState() throws IOException {
    if (jobProgress == null) {
      jobProgress = new MapReduceJobState(
          rj, jobClient.getMapTaskReports(jobId), jobClient.getReduceTaskReports(jobId));
      return true;
    }

    boolean complete = rj.isComplete();
    boolean successful = rj.isSuccessful();
    float mapProgress = rj.mapProgress();
    float reduceProgress = rj.reduceProgress();

    boolean update = !(
        jobProgress.isComplete() == complete &&
        jobProgress.isSuccessful() == successful &&
        AmbroseHiveUtil.isEqual(jobProgress.getMapProgress(), mapProgress) &&
        AmbroseHiveUtil.isEqual(jobProgress.getReduceProgress(), reduceProgress)
    );

    //do progress report only if necessary
    if (update) {
      jobProgress = new MapReduceJobState(
          rj, jobClient.getMapTaskReports(jobId), jobClient.getReduceTaskReports(jobId));
      jobProgress.setJobLastUpdateTime(System.currentTimeMillis());
    }
    return update;
  }

  private int getJobProgress() {
    float result = ((jobProgress.getMapProgress() + jobProgress.getReduceProgress()) * 100) / 2;
    return (int) result;
  }

}
