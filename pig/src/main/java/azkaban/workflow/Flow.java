/*
 * Copyright 2010 LinkedIn, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.workflow;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import azkaban.common.utils.Props;
import azkaban.workflow.flow.Dependency;
import azkaban.workflow.flow.FlowNode;

/**
 * Contains the graph of a flow. 
 * @author Richard Park
 */
public class Flow extends WorkUnit {
	private boolean isLayedOut = false;
	private LinkedHashMap<String,FlowNode> flowItems = new LinkedHashMap<String, FlowNode>();
	
	private ArrayList<String> errorMessages = new ArrayList<String>();
	private boolean hasValidated = false;
	
	private long timestamp;
	private long layoutTimestamp;
	
	public Flow(String id, Flow toClone) {
		super(id, toClone);
		
		isLayedOut = toClone.isLayedOut;
		for (Map.Entry<String, FlowNode> node : toClone.flowItems.entrySet()) {
			FlowNode cloned = new FlowNode(node.getValue());
			flowItems.put(node.getKey(), cloned);
		}
		
		this.timestamp = toClone.timestamp;
		this.layoutTimestamp = toClone.layoutTimestamp;
		this.hasValidated = toClone.hasValidated;
	}
	
	public Flow(String id, Props prop) {
		super(id, prop);
	}
	
	public void setLastModifiedTime(long timestamp) {
		this.timestamp = timestamp;
	}
	
	public long getLastModifiedTime() {
		return timestamp;
	}
	
	public void setLastLayoutModifiedTime(long timestamp) {
		this.layoutTimestamp = timestamp;
	}
	
	public long getLastLayoutModifiedTime() {
		return layoutTimestamp;
	}
	
	public List<Dependency> getDependencies() {
		ArrayList<Dependency> dependency = new ArrayList<Dependency>();
		
		for (FlowNode node: flowItems.values()) {
			for (String dependents : node.getDependents() ) {
				dependency.add(new Dependency(node, flowItems.get(dependents)));
			}
		}
		
		return dependency;
	}
	
	public void setPosition(String id, float x, float y) {
		FlowNode node = flowItems.get(id);
		node.setPosition(x, y);
	}
	
	public void setStatus(String id, String status) {
		FlowNode node = flowItems.get(id);
		node.setStatus(status);
	}
	
	/**
	 * Add job and its dependencies to the flow graph.
	 * 
	 * @param id
	 * @param dependency
	 */
	public void addDependencies(String id, List<String> dependency) {
		FlowNode node = flowItems.get(id);
		if (node == null) {
			node = new FlowNode(id);
			flowItems.put(id, node);
		}

		if (node.getDependencies() != null) {
			errorMessages.add("Job " + id + " has multiple dependency entries in this flow.");
		}
		HashSet<String> set = new HashSet<String>();
		set.addAll(dependency);
		node.setDependencies(set);
		
		// Go through the node's dependencies and add the node as a dependent.
		for (String dep: dependency) {
			if (dep.equals(id)) {
				errorMessages.add("Job " + id + " has defined itself as a dependency.");
				continue;
			}
			
			FlowNode parentNode = flowItems.get(dep);
			if (parentNode == null) {
				parentNode = new FlowNode(dep);
				flowItems.put(dep, parentNode);
			}
			parentNode.addDependent(id);
		}
	}
	
	/**
	 * Checks flow for cyclical errors and validates the jobs within this flow.
	 * 
	 * @return
	 */
	public boolean validateFlow() {
		if (hasValidated) {
			return errorMessages.isEmpty();
		}
		hasValidated = true;
		// Find root nodes without dependencies and then employ breath first search to find cycles in the DAG
		ArrayList<FlowNode> topNodes = new ArrayList<FlowNode>();
		for (FlowNode node : flowItems.values()) {
			if (node.getDependencies() == null || node.getDependencies().isEmpty()) {
				topNodes.add(node);
			}
		}

		HashSet<String> visited = new HashSet<String>();

		for (FlowNode node: topNodes) {
			visited.add(node.getAlias());
			if (!graphLayer(visited, node, 0)) {
				break;
			}
			visited.remove(node.getAlias());
		}
		
		return errorMessages.isEmpty();
	}

	// Find cycles and mark the node level.
	private boolean graphLayer(HashSet<String> visited, FlowNode node, int level) {
		int currentLevel = Math.max(level, node.getLevel());
		node.setLevel(currentLevel);

		for(String dep : node.getDependents()) {
			if (visited.contains(dep)) {
				errorMessages.add("Found cycle at " + node.getAlias());
				return false;
			}

			visited.add(dep);
			if (!graphLayer(visited, flowItems.get(dep), currentLevel + 1)) {
				return false;
			}

			visited.remove(dep);
		}

		return true;
	}
	
	public List<String> errorMessages() {
		return errorMessages;
	}
	
	public List<FlowNode> getFlowNodes() {
		return new ArrayList<FlowNode>(flowItems.values());
	}
	
	public FlowNode getFlowNode(String alias) {
		return flowItems.get(alias);
	}
    
	public void setNodeProps(String id, Props prop) {
		FlowNode node = flowItems.get(id);
		
		if (node == null) {
			node = new FlowNode(id);
			flowItems.put(id, node);
		}
		
		node.setProps(prop);
	}
	
	public void printFlow() {
		System.out.println("Flow " + this.getId());
		for (FlowNode flow: flowItems.values()) {
			System.out.print(" " + flow.getLevel() + " Job " + flow.getAlias() + " ->[");
			for (String dependents: flow.getDependents()) {
				System.out.print(dependents + ", ");
			}
			System.out.print("]\n");
		}
	}

	public void setLayedOut(boolean isLayedOut) {
		this.isLayedOut = isLayedOut;
	}

	public boolean isLayedOut() {
		return isLayedOut;
	}
}
