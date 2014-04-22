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

import cascading.flow.planner.BaseFlowStep;
import com.twitter.ambrose.model.DAGNode;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jgrapht.Graphs;
import org.jgrapht.graph.SimpleDirectedGraph;

/**
 * This class is responsible for converting the SimpleDirectedGraph object used 
 * to represent a flow into a map of DagNodes
 *
 * @author Ahmed Eshra
 */
public class AmbroseCascadingGraphConverter {

  /* Input list of cascading flow steps to be generated to Ambrose DAGNode */
  protected SimpleDirectedGraph jobsGraph;
  /* Output Map of the generated DAGNode and their names */
  protected Map<String, DAGNode<CascadingJob>> dagNamesMap;

  /*EmbeddedAmbroseCascadingProgressNotificationListener
   * Constractor of AmbroseCascadingGraphConvertor
   *
   * @param graph FlowStepGraph (which implement SimpleDirectedGraph)
   * @param nodesMap Map of DAGNodes to be sent to ambrose
   */
  public AmbroseCascadingGraphConverter(SimpleDirectedGraph graph, Map<String, DAGNode<CascadingJob>> nodesMap) {
    this.jobsGraph = graph;
    this.dagNamesMap = nodesMap;
  }

  /*
   * Convert the flowStep that generated from cascading to a Map of DAGNode and its name
   * to be used to build Ambrose Graph
   *
   */
  public void convert() {
    //  Returns a set of the nodes contained in this graph.
    Set vetices = jobsGraph.vertexSet();

    for (Object flowStep : vetices) {
      BaseFlowStep baseFlowStep = (BaseFlowStep) flowStep;
      String nodeName = baseFlowStep.getName();
      String[] features = getNodeFeatures(baseFlowStep.getGraph());
      // create a new DAGNode of this flowStep
      CascadingJob job = new CascadingJob();
      job.setFeatures(features);
      DAGNode newNode = new DAGNode(nodeName, job);
      // Add the new node to the Map of <nodeName, DAGNodes>
      dagNamesMap.put(nodeName, newNode);
    }

    // Loop again to set the successors for each node after nodes are created.
    for (Object flowStep : vetices) {
      String nodeName = ((BaseFlowStep) flowStep).getName();
      //set the successors of this node using getNodeSuccessors method
      ((DAGNode) dagNamesMap.get(nodeName)).setSuccessors(getNodeSuccessors(flowStep));
    }
  }

  /*
   * Return the features for each node, which are the names of mapper and reducer jobs
   * for each node.
   *
   * @param graph inner jobs graph for each node in the main graph.
   *
   * @return a list of inner jobs names which defined the parent job features
   */
  protected String[] getNodeFeatures(SimpleDirectedGraph graph) {
    Set vertices = graph.vertexSet();
    String[] returnedFeatures = new String[vertices.size()];
    for (int i = 0; i < returnedFeatures.length; i++) {
      returnedFeatures[i] = vertices.toArray()[i].getClass().getSimpleName();
    }
    return returnedFeatures;
  }

  /*
   * Return a Collection of successor nodes of a certain flowStep
   *
   * @param flowStep the step or node its successors nodes will be returned
   *
   * @return collection of successor DAGNodes for each node.
   */
  protected Collection<DAGNode> getNodeSuccessors(Object flowStep) {
    Collection<DAGNode> nodeSuccessors = new HashSet<DAGNode>();
    // Graphs, used to get the successor nodes using
    // successorListOf(DirectedGraph, vertex) method
    List successorNodes = Graphs.successorListOf(jobsGraph, flowStep);

    for (Object node : successorNodes) {
      String nodeName = ((BaseFlowStep) node).getName();
      nodeSuccessors.add(dagNamesMap.get(nodeName));
    }
    return nodeSuccessors;
  }
}
