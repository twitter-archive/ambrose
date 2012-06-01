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
package com.twitter.ambrose.service.impl;

import azkaban.common.utils.Props;
import azkaban.workflow.Flow;
import azkaban.workflow.flow.DagLayout;
import azkaban.workflow.flow.FlowNode;
import azkaban.workflow.flow.SugiyamaLayout;
import com.twitter.ambrose.service.DAGNode;
import com.twitter.ambrose.service.DAGTransformer;

import java.util.Arrays;
import java.util.Collection;

/**
 * Transformer that wraps Azkaban's SugiyamaLayout to create level, X and Y values for the nodes in
 * the DAG.
 * @author billg
 */
public class SugiyamaLayoutTransformer implements DAGTransformer {

  private boolean landscape;

  /**
   * Create an instance of this class to generate top-down coordinates
   */
  public SugiyamaLayoutTransformer() {
    this(false);
  }

  /**
   * Create an instance of this class to generate coordinates. If <pre>landscape=true</pre>, then
   * the graph will layout from left to right. Otherwise it will layout from top to bottom.
   */
  public SugiyamaLayoutTransformer(boolean landscape) {
    this.landscape = landscape;
  }

  @Override
  public Collection<DAGNode> transform(Collection<DAGNode> nodes) {
    Flow flow = new Flow("sample flow", new Props());

    for(DAGNode node : nodes) {
      for(String successor : node.getSuccessorNames()) {
        flow.addDependencies(successor, Arrays.asList(node.getName()));
      }
    }

    flow.validateFlow(); // this sets levels
    flow.printFlow();
    if (!flow.isLayedOut()) {
      DagLayout layout = new SugiyamaLayout(flow);
      layout.setLayout();
    }

    for(DAGNode node : nodes) {
      FlowNode flowNode = flow.getFlowNode(node.getName());

      // invert X/Y if we're rendering in landscape
      if (landscape) {
        node.setX(flowNode.getY());
        node.setY(flowNode.getX());
      }
      else {
        node.setX(flowNode.getX());
        node.setY(flowNode.getY());
      }

      node.setDagLevel(flowNode.getLevel());
    }

    return nodes;
  }
}
