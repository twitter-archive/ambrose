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
package com.twitter.ambrose.cascading3;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.jgrapht.DirectedGraph;
import org.jgrapht.EdgeFactory;
import org.jgrapht.graph.DefaultDirectedGraph;

import com.twitter.ambrose.model.DAGNode;
import com.twitter.ambrose.model.Event;
import com.twitter.ambrose.model.hadoop.MapReduceHelper;
import com.twitter.ambrose.service.StatsWriteService;
import com.twitter.ambrose.util.AmbroseUtils;

import cascading.flow.Flow;
import cascading.flow.FlowListener;
import cascading.flow.FlowStep;
import cascading.flow.FlowStepListener;
import cascading.flow.Flows;
import cascading.flow.planner.BaseFlowStep;
import cascading.flow.planner.process.FlowStepGraph;
import cascading.flow.planner.process.ProcessEdge;
import cascading.stats.hadoop.HadoopStepStats;

/**
 * CascadingNotifier that collects plan and job information from within a cascading runtime, builds
 * Ambrose model objects, and passes the objects to an Ambrose StatsWriteService object. This
 * listener can be used regardless of what mode Ambrose is running in.
 *
 * @author Ahmed Mohsen
 * @see EmbeddedAmbroseCascadingNotifier for a subclass that can be used to run an embedded Ambrose
 * web server from Main method.
 */
// TODO: Rename this class to AmbroseFlowListener
public class AmbroseCascadingNotifier implements FlowListener, FlowStepListener {

  private static final Log LOG = LogFactory.getLog(AmbroseCascadingNotifier.class);
  private final MapReduceHelper mapReduceHelper = new MapReduceHelper();
  private final StatsWriteService statsWriteService;
  private final Map<String, DAGNode<CascadingJob>> nodesByName = Maps.newTreeMap();
  private final Set<String> completedStepNames = Sets.newHashSet();
  private int totalNumberOfJobs;
  private int runningJobs;
  private String currentFlowId;

  public class FlowGraphEdge {
    // These are step IDs which should be unique
    String srcID;
    String destID;

    public FlowGraphEdge(String srcID, String destID) {
      this.srcID = srcID;
      this.destID = destID;
    }
  }

  /**
   * Constructs new instance.
   *
   * @param statsWriteService ambrose stats write service to which stats are written.
   */
  public AmbroseCascadingNotifier(StatsWriteService statsWriteService) {
    this.statsWriteService = statsWriteService;
  }

  protected StatsWriteService getStatsWriteService() {
    return statsWriteService;
  }

  /**
   * Retrieves the ambrose node associated with the given flow step.
   *
   * @param step step for which node should be retrieved.
   * @return node associated with step.
   */
  private DAGNode<CascadingJob> getNode(FlowStep step) {
    String name = step.getName();
    DAGNode<CascadingJob> node = nodesByName.get(name);
    if (node == null) {
      throw new IllegalStateException(String.format("Node with name '%s' not found", name));
    }
    return node;
  }

  /**
   * Retrieves and updates ambrose node associated with the given flow step.
   *
   * @param step step with which to update ambrose node state.
   * @return node associated with step.
   */
  private DAGNode<CascadingJob> updateNode(FlowStep step) {
    DAGNode<CascadingJob> node = getNode(step);
    CascadingJob job = node.getJob();
    HadoopStepStats stats = (HadoopStepStats) step.getFlowStepStats();
    job.setId(stats.getProcessStepID());
    job.setJobStats(stats);
    mapReduceHelper.addMapReduceJobState(job, stats.getJobClient());
    return node;
  }

  /**
   * The onStarting event is fired when a Flow instance receives the start() message. A Flow is cut
   * down into executing units called stepFlow. A stepFlow contains a stepFlowJob which represents
   * the mapreduce job to be submitted to Hadoop. The ambrose graph is constructed from the step
   * graph found in flow object.
   *
   * @param flow the flow.
   */
  @Override
  @SuppressWarnings("unchecked")
  public void onStarting(Flow flow) {
    // init flow
    List<FlowStep> steps = flow.getFlowSteps();
    totalNumberOfJobs = steps.size();
    currentFlowId = flow.getID();

    Properties props = new Properties();
    props.putAll(flow.getConfigAsProperties());
    try {
      statsWriteService.initWriteService(props);
    } catch (IOException e) {
      LOG.error("Failed to initialize statsWriteService", e);
    }

    // convert graph from cascading to jgrapht
    FlowStepGraph flowStepGraph = Flows.getStepGraphFrom(flow);
    DirectedGraph graph = new DefaultDirectedGraph<BaseFlowStep, FlowGraphEdge>(
      new EdgeFactory<BaseFlowStep, FlowGraphEdge>() {
        @Override
        public FlowGraphEdge createEdge(BaseFlowStep src, BaseFlowStep dest) {
          return new FlowGraphEdge(src.getID(), dest.getID());
        }
      }
    );
    for (FlowStep v: flowStepGraph.vertexSet()) {
      graph.addVertex(v);
    }
    for (ProcessEdge e: flowStepGraph.edgeSet()) {
      graph.addEdge(e.getSourceProcessID(), e.getSinkProcessID());
    }

    // convert graph from jgrapht to ambrose
    AmbroseCascadingGraphConverter converter =
        new AmbroseCascadingGraphConverter(graph, nodesByName);
    converter.convert();
    AmbroseUtils.sendDagNodeNameMap(statsWriteService, currentFlowId, nodesByName);
  }

  /**
   * The onStopping event is fired when a Flow instance receives the stop() message.
   *
   * @param flow the flow.
   */
  @Override
  public void onStopping(Flow flow) {
  }

  /**
   * The onThrowable event is fired if any child {@link FlowStep} throws a Throwable type. This
   * throwable is passed as an argument to the event. This event method should return true if the
   * given throwable was handled and should not be rethrown from the {@link Flow#complete()}
   * method.
   *
   * @param flow the flow.
   * @param throwable the exception.
   * @return true if this listener has handled the given throwable.
   */
  @Override
  public boolean onThrowable(Flow flow, Throwable throwable) {
    return false;
  }

  /**
   * The onCompleted event is fired when a Flow instance has completed all work whether if was
   * success or failed. If there was a thrown exception, onThrowable will be fired before this
   * event.
   *
   * @param flow the flow.
   */
  @Override
  public void onCompleted(Flow flow) {
    // ensure workflow progress reflects completion
    AmbroseUtils.pushWorkflowProgressEvent(statsWriteService, currentFlowId, 100);
  }

  /**
   * onStepStarting event is fired whenever a job is submitted to Hadoop and begun its execution.
   *
   * @param step the step in the flow that represents the MapReduce job.
   */
  @Override
  public void onStepStarting(FlowStep step) {
    // update overall progress
    runningJobs++;
    try {
      DAGNode<CascadingJob> node = updateNode(step);
      AmbroseUtils.pushEvent(statsWriteService, currentFlowId, new Event.JobStartedEvent(node));
    } catch (Exception e) {
      LOG.error("Failed to handle onStepStarting event", e);
    }
  }

  /**
   * onStepCompleted event is fired when a stepFlowJob completed its work.
   *
   * @param step the step in the flow that represents the MapReduce job.
   */
  @Override
  public void onStepCompleted(FlowStep step) {
    try {
      DAGNode<CascadingJob> node = updateNode(step);
      AmbroseUtils.pushEvent(statsWriteService, currentFlowId, new Event.JobFinishedEvent(node));
    } catch (Exception e) {
      LOG.error("Failed to handle onStepCompleted event", e);
    }
  }

  /**
   * onStepThrowable event is fired if job failed during execution A job_failed event is pushed with
   * node represents the failed job.
   *
   * @param step the step in the flow that represents the MapReduce job.
   * @param throwable the exception that caused the job to fail.
   */
  @Override
  public boolean onStepThrowable(FlowStep step, Throwable throwable) {
    try {
      DAGNode<CascadingJob> node = updateNode(step);
      AmbroseUtils.pushEvent(statsWriteService, currentFlowId, new Event.JobFailedEvent(node));
    } catch (Exception e) {
      LOG.error("Failed to handle onStepThrowable event", e);
    }
    return false;
  }

  /**
   * onStepProgressing event is fired whenever a job makes progress.
   *
   * @param step the step in the flow that represents the MapReduce job.
   */
  @Override
  public void onStepRunning(FlowStep step) {
    // first we report the scripts progress
    int progress = (int) ((((double) runningJobs) / totalNumberOfJobs) * 100);
    AmbroseUtils.pushWorkflowProgressEvent(statsWriteService, currentFlowId, progress);

    // only push job progress events for a completed step once
    if (completedStepNames.contains(step.getName())) {
      return;
    }

    try {
      // update node
      DAGNode<CascadingJob> node = updateNode(step);

      if (node.getJob().getMapReduceJobState() != null) {
        AmbroseUtils.pushEvent(statsWriteService, currentFlowId, new Event.JobProgressEvent(node));

        if (node.getJob().getMapReduceJobState().isComplete()) {
          completedStepNames.add(step.getName());
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to handle onStepRunning event", e);
    }
  }

  @Override
  public void onStepStopping(FlowStep step) {
  }
}

