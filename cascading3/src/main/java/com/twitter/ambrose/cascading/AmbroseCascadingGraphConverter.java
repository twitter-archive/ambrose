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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

import org.jgrapht.Graphs;
import org.jgrapht.DirectedGraph;

import com.twitter.ambrose.model.DAGNode;
import com.twitter.ambrose.model.Job;

import cascading.flow.planner.BaseFlowStep;
import cascading.flow.planner.graph.ElementGraph;

/**
 * This class is responsible for converting the DirectedGraph object used to represent a flow
 * into a map of DagNodes
 *
 * @author Ahmed Eshra
 */
public class AmbroseCascadingGraphConverter {

  private final DirectedGraph jobsGraph;
  private final Map<String, DAGNode<CascadingJob>> dagNamesMap;

  /**
   * Constructs instance of the graph converter.
   *
   * @param graph input list of cascading flow steps.
   */
  public AmbroseCascadingGraphConverter(
      DirectedGraph graph,
      Map<String, DAGNode<CascadingJob>> dagNamesMap
  ) {
    this.jobsGraph = graph;
    this.dagNamesMap = dagNamesMap;
  }

  /**
   * Converts the flowStep that generated from cascading to a Map of DAGNode and its name to be used
   * to build Ambrose Graph.
   */
  public void convert() {
    // returns a set of the nodes contained in this graph
    Set vertices = jobsGraph.vertexSet();

    // create ambrose nodes
    for (Object vertex : vertices) {
      BaseFlowStep step = (BaseFlowStep) vertex;
      CascadingJob job = new CascadingJob();
      job.setFeatures(getNodeFeatures(step));
      String name = step.getName();
      DAGNode<CascadingJob> node = new DAGNode<CascadingJob>(name, job);
      dagNamesMap.put(name, node);
    }

    // loop again to set the successors for each node after nodes are created
    for (Object vertex : vertices) {
      BaseFlowStep step = (BaseFlowStep) vertex;
      String name = step.getName();
      DAGNode<CascadingJob> node = dagNamesMap.get(name);
      node.setSuccessors(getNodeSuccessors(vertex));
    }
  }

  /**
   * Retrieves array of simple class names of nodes within a particular flow step.
   *
   * @param step step in main flow.
   * @return list step's graph's vertex names.
   */
  protected String[] getNodeFeatures(BaseFlowStep step) {
    ElementGraph graph = step.getElementGraph();
    Object[] vertices = graph.vertexSet().toArray();
    String[] returnedFeatures = new String[vertices.length];
    for (int i = 0; i < returnedFeatures.length; i++) {
      returnedFeatures[i] = vertices[i].getClass().getSimpleName();
    }
    return returnedFeatures;
  }

  /**
   * Return a Collection of successor nodes of a certain vertex.
   *
   * @param vertex the step or node its successors nodes will be returned.
   * @return collection of successor DAGNodes for each node.
   */
  protected Collection<DAGNode<? extends Job>> getNodeSuccessors(Object vertex) {
    Collection<DAGNode<? extends Job>> nodeSuccessors = Sets.newHashSet();
    List successorNodes = Graphs.successorListOf(jobsGraph, vertex);
    for (Object node : successorNodes) {
      BaseFlowStep step = (BaseFlowStep) node;
      String name = step.getName();
      nodeSuccessors.add(dagNamesMap.get(name));
    }
    return nodeSuccessors;
  }
}
