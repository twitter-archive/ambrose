/*
Copyright ......

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
package com.twitter.ambrose.cascading;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobClient;
import org.jgrapht.graph.SimpleDirectedGraph;

import cascading.flow.Flow;
import cascading.flow.FlowListener;
import cascading.flow.FlowStep;
import cascading.flow.FlowStepListener;
import cascading.flow.Flows;
import cascading.flow.hadoop.HadoopFlowStep;
import cascading.flow.planner.BaseFlowStep;
import cascading.stats.hadoop.HadoopStepStats;

import com.google.common.collect.Maps;
import com.twitter.ambrose.model.DAGNode;
import com.twitter.ambrose.model.Event;
import com.twitter.ambrose.model.Job;
import com.twitter.ambrose.model.hadoop.MapReduceHelper;
import com.twitter.ambrose.service.StatsWriteService;
import com.twitter.ambrose.util.AmbroseUtils;

/**
 * CascadingNotifier that collects plan and job information from within a cascading
 * runtime, builds Ambrose model objects, and passes the objects to an Ambrose
 * StatsWriteService object. This listener can be used regardless of what mode
 * Ambrose is running in.
 *
 * @see EmbeddedAmbroseCascadingNotifier for a subclass that can be used to run
 * an embedded Ambrose web server from Main method.
 * @author Ahmed Mohsen
 */
public class AmbroseCascadingNotifier implements FlowListener, FlowStepListener {

  protected Log log = LogFactory.getLog(getClass());
  private StatsWriteService<?> statsWriteService;
  private List<Job> jobs = new ArrayList<Job>();
  private Map<String, DAGNode<CascadingJob>> dagNodeNameMap = Maps.newTreeMap();
  private Map<String, DAGNode<CascadingJob>> dagNodeJobIdMap = Maps.newTreeMap();
  private HashSet<String> completedJobIds = new HashSet<String>();
  private int totalNumberOfJobs;
  private int runningJobs = 0;
  private String currentFlowId;

  private MapReduceHelper mapReduceHelper = new MapReduceHelper();
  
  /**
   * Initialize this class with an instance of StatsWriteService to push stats
   * to.
   *
   * @param statsWriteService
   */
  public AmbroseCascadingNotifier(StatsWriteService<?> statsWriteService) {
      this.statsWriteService = statsWriteService;
  }

  protected StatsWriteService<?> getStatsWriteService() {
      return statsWriteService;
  }

  /**
   * The onStarting event is fired when a Flow instance receives the start()
   * message. -a Flow is cut down into executing units called stepFlow
   * -stepFlow contains a stepFlowJob which represents the mapreduce job to be
   * submitted to Hadoop -the DAG graph is constructed from the step graph
   * found in flow object
   *
   * @param flow
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
@Override
  public void onStarting(Flow flow) {
    //init flow
    List<BaseFlowStep> steps = flow.getFlowSteps();
    totalNumberOfJobs = steps.size();
    currentFlowId = flow.getID();
    
    Properties props = new Properties();
    props.putAll(flow.getConfigAsProperties());
    try {
      statsWriteService.initWriteService(props);
    } catch (IOException ioe) {
      throw new RuntimeException("Exception while initializing statsWriteService", ioe);
    }

    // convert the graph generated by cascading toDAGNodes Graph to be sent to ambrose
    AmbroseCascadingGraphConverter convertor =
        new AmbroseCascadingGraphConverter((SimpleDirectedGraph) Flows.getStepGraphFrom(flow), dagNodeNameMap);
    convertor.convert();
    AmbroseUtils.sendDagNodeNameMap(statsWriteService, currentFlowId, this.dagNodeNameMap);
  }

  /**
   * The onStopping event is fired when a Flow instance receives the stop()
   * message.
   *
   * @param flow
   */
  @SuppressWarnings("rawtypes")
@Override
  public void onStopping(Flow flow) {}

  /**
   * The onCompleted event is fired when a Flow instance has completed all
   * work whether if was success or failed. If there was a thrown exception,
   * onThrowable will be fired before this event.
   *
   * @param flow
   */
  @SuppressWarnings("rawtypes")
@Override
  public void onCompleted(Flow flow) {}

  /**
   * The onThrowable event is fired if any child
   * {@link cascading.flow.FlowStep} throws a Throwable type. This throwable
   * is passed as an argument to the event. This event method should return
   * true if the given throwable was handled and should not be rethrown from
   * the {@link Flow#complete()} method.
   *
   * @param flow
   * @param throwable
   * @return true if this listener has handled the given throwable
   */
  @Override
  public boolean onThrowable(@SuppressWarnings("rawtypes") Flow flow, Throwable throwable) {
      return false;
  }

  /**
   * onStepStarting event is fired whenever a job is submitted to Hadoop and begun
   * its excution
   *
   * @param flowStep the step in the flow that represents the MapReduce job
   */
  @Override
  public void onStepStarting(@SuppressWarnings("rawtypes") FlowStep flowStep) {
    //getting Hadoop job client
    HadoopStepStats stats = (HadoopStepStats)((HadoopFlowStep)flowStep).getFlowStepStats();
    String assignedJobId = stats.getJobID();
    String jobName = flowStep.getName();
    JobClient jc = stats.getJobClient();

    runningJobs++; //update overall progress

    DAGNode<CascadingJob> node = this.dagNodeNameMap.get(jobName);
    if (node == null) {
      log.warn("jobStartedNotification - unrecognized operator name found ("
              + jobName + ") for jobId " + assignedJobId);
    } else {
      CascadingJob job = node.getJob();
      job.setId(assignedJobId);
      job.setJobStats(stats);
      mapReduceHelper.addMapReduceJobState(job, jc);

      dagNodeJobIdMap.put(job.getId(), node);
      AmbroseUtils.pushEvent(statsWriteService, currentFlowId, new Event.JobStartedEvent(node));
    }
  }

  /**
   * onStepCompleted event is fired when a stepFlowJob completed its work
   *
   * @param flowStep the step in the flow that represents the MapReduce job
   */
  @Override
  public void onStepCompleted(@SuppressWarnings("rawtypes") FlowStep flowStep) {
       HadoopStepStats stats = (HadoopStepStats)flowStep.getFlowStepStats();
       String jobId = stats.getJobID();

      //get job node
      DAGNode<CascadingJob> node = dagNodeJobIdMap.get(jobId);
      if (node == null) {
        log.warn("Unrecognized jobId reported for succeeded job: " + stats.getJobID());
        return;
      }
      mapReduceHelper.addMapReduceJobState(node.getJob(), stats.getJobClient());
      addCompletedJobStats(node.getJob(), stats);
      AmbroseUtils.pushEvent(statsWriteService, currentFlowId, new Event.JobFinishedEvent(node));
  }

  /**
   * onStepThrowable event is fired if job failed during execution A job_failed
   * event is pushed with node represents the failed job
   *
   * @param flowStep the step in the flow that represents the MapReduce job
   * @param throwable  the exception that caused the job to fail
   */
  @Override
  public boolean onStepThrowable(@SuppressWarnings("rawtypes") FlowStep flowStep , Throwable throwable) {
    HadoopStepStats stats = (HadoopStepStats)flowStep.getFlowStepStats();
    String jobName = flowStep.getName();

    //get job node
    DAGNode<CascadingJob> node = dagNodeNameMap.get(jobName);
    if (node == null) {
      log.warn("Unrecognized jobId reported for succeeded job: " + stats.getJobID());
      return false;
    }
    mapReduceHelper.addMapReduceJobState(node.getJob(), stats.getJobClient());
    addCompletedJobStats(node.getJob(), stats);
    AmbroseUtils.pushEvent(statsWriteService, currentFlowId, new Event.JobFailedEvent(node));
    return false;
  }

  /**
   * onStepProgressing event is fired whenever a job made a progress
   *
   * @param flowStep the step in the flow that represents the MapReduce job
   */
  @Override
  public void onStepRunning(@SuppressWarnings("rawtypes") FlowStep flowStep) {
    //getting Hadoop running job and job client
    HadoopStepStats stats = (HadoopStepStats)flowStep.getFlowStepStats();
    JobClient jc = stats.getJobClient();

    // first we report the scripts progress
    int progress = (int) (((runningJobs * 1.0) / totalNumberOfJobs) * 100);
    AmbroseUtils.pushWorkflowProgressEvent(statsWriteService, currentFlowId, progress);

    //get job node
    String jobId = stats.getJobID();
    DAGNode<CascadingJob> node = dagNodeJobIdMap.get(jobId);
    if (node == null) {
      log.warn("Unrecognized jobId reported for succeeded job: " + stats.getJobID());
      return;
    }
    
    //only push job progress events for a completed job once
    if(completedJobIds.contains(node.getJob().getId())) {
      return;
    }
    
    mapReduceHelper.addMapReduceJobState(node.getJob(), jc);

    if (node.getJob().getMapReduceJobState() != null) {
      AmbroseUtils.pushEvent(statsWriteService, currentFlowId, new Event.JobProgressEvent(node));
      
      if (node.getJob().getMapReduceJobState().isComplete()) {
        completedJobIds.add(node.getJob().getId());
      }
    }
  }

  @Override
  public void onStepStopping(@SuppressWarnings("rawtypes") FlowStep flowStep) {
  }

  private void addCompletedJobStats(CascadingJob job, HadoopStepStats stats) {
    job.setJobStats(stats);
    jobs.add(job);
  }
}

