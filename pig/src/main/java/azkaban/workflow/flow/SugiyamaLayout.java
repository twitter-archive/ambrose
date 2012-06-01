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
package azkaban.workflow.flow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import azkaban.workflow.Flow;
import azkaban.workflow.flow.FlowNode;

public class SugiyamaLayout extends DagLayout {
	HashMap<Integer, ArrayList<Node>> levelMap = new HashMap<Integer, ArrayList<Node>>();
	LinkedHashMap<String, JobNode> nodesMap = new LinkedHashMap<String, JobNode>();
	
	public static final float LEVEL_HEIGHT = 120;
	public static final float LEVEL_WIDTH = 80;
	public static final float LEVEL_WIDTH_ADJUSTMENT = 5;
	
	public SugiyamaLayout(Flow flow) {
		super(flow);
	}
	
	public void setLayout() {
		nodesMap.clear();
		int longestWord = 0;
		for(FlowNode flowNode : flow.getFlowNodes()) {
			longestWord = Math.max(flowNode.getAlias().length(), longestWord);
			nodesMap.put(flowNode.getAlias(), new JobNode(flowNode));
		}
		
		for(JobNode jobNode : nodesMap.values()) {
			layerNodes(jobNode);
		}
		
		// Re-arrange top down!
		int size = levelMap.size();
		ArrayList<Node> list = levelMap.get(0);
		float count = 0;
		for (Node node: list) {
			node.setPosition(count);
			count +=1;
		}
		for (int i = 1; i < size; ++i) {
			//barycenterMethodUncross(levelMap.get(i), true);
			barycenterMethodUncross(levelMap.get(i));
		}	
		
		float widthAdjustment = LEVEL_WIDTH + longestWord*LEVEL_WIDTH_ADJUSTMENT;
		// Adjust level
		for (Map.Entry<Integer, ArrayList<Node>> entry: levelMap.entrySet()) {
			ArrayList<Node> nodes = entry.getValue();
			Integer level = entry.getKey();
			
			float offset = -((float)nodes.size()/2);
			for (Node node: nodes) {
				if (node instanceof JobNode) {
					FlowNode flowNode = ((JobNode) node).getFlowNode();
					flowNode.setPosition(offset*widthAdjustment, level*LEVEL_HEIGHT);
				}

				offset += 1;
			}
			
		}
		
		flow.setLayedOut(true);
	}
	
	private void uncrossLayer(ArrayList<Node> free, boolean topDown) {
		// Using median method
		if (topDown) {
			for (Node node: free) {
				float median = getMedian(node.getDependencies());
				System.out.println("getMedian " + median);
				node.setPosition(median);
			}
		}
		else {
			for (Node node: free) {
				float median = getMedian(node.getDependents());
				//System.out.println("getMedian " + median);
				node.setPosition(median);
			}
		}
		
		Collections.sort(free);
	}
	
	private void barycenterMethodUncross(ArrayList<Node> free) {
		int numOnLevel = free.size();
		for (Node node: free) {
			float average = findAverage(node.getDependencies());
			node.setPosition(average);
			node.setNumOnLevel( numOnLevel );
		}
	
		Collections.sort(free);
		reorder(free);
	}
	
	private void reorder(ArrayList<Node> nodes) {
		int count = 1;
		for (int i = 0; i < nodes.size(); ++i) {
			Node node = nodes.get(i);
			node.setPosition(i);
		}
	}
	
	private float findAverage(List<Node> nodes) {
		float sum = 0;
		for (Node node : nodes) {
			sum += node.getPosition();
		}
		
		return sum/(nodes.size());
	}
	
	private float getMedian(List<Node> dependents) {
		int length = dependents.size();
		if (length == 0) {
			return 0;
		}
		
		float[] position = new float[length];
		int i = 0;
		for (Node dep: dependents) {
			position[i] = dep.getPosition();
			++i;
		}
		
		Arrays.sort(position);
		int index = position.length / 2;
		if ((length % 2) == 1) {
			return position[index];
		}
		else {
			return (position[index-1] + position[index])/2;
		}
	}
	
	private void layerNodes(JobNode node) {
		FlowNode flowNode = node.getFlowNode();
		int level = node.getLevel();

		ArrayList<Node> levelList = levelMap.get(level);
		if (levelList == null) {
			levelList = new ArrayList<Node>();
			levelMap.put(level, levelList);
		}
		
		levelList.add(node);
		
		for (String dep : flowNode.getDependents() ) {
			JobNode depNode = nodesMap.get(dep);
			if (depNode.getLevel() - node.getLevel() > 1) {
				addDummyNodes(node, depNode);
			}
			else {
				depNode.addDependecy(node);
				node.addDependent(depNode);
			}
		}
	}
	
	private void addDummyNodes(JobNode from, JobNode to) {
		int fromLevel = from.getLevel();
		int toLevel = to.getLevel();
		
		Node fromNode = from;
		for (int i = fromLevel+1; i < toLevel; ++i) {
			DummyNode dummyNode = new DummyNode(i);
			
			ArrayList<Node> levelList = levelMap.get(i);
			if (levelList == null) {
				levelList = new ArrayList<Node>();
				levelMap.put(i, levelList);
			}
			levelList.add(dummyNode);
			
			dummyNode.addDependecy(fromNode);
			fromNode.addDependent(dummyNode);
			fromNode = dummyNode;
		}
		
		to.addDependecy(from);
		fromNode.addDependent(to);
	}
	
	private class Node implements Comparable {
		private List<Node> dependents = new ArrayList<Node>();
		private List<Node> dependencies = new ArrayList<Node>();
		private float position = 0;
		private int numOnLevel = 0;
		
		public void addDependent(Node dependent) {
			dependents.add(dependent);
		}
		
		public void addDependecy(Node dependency) {
			dependencies.add(dependency);
		}
		
		public List<Node> getDependencies() {
			return dependencies;
		}
		
		public List<Node> getDependents() {
			return dependents;
		}
		
		public float getPosition() {
			return position;
		}
		
		private void setPosition(float pos) {
			position = pos;
		}

		@Override
		public int compareTo(Object arg0) {
			// TODO Auto-generated method stub
			Node other = (Node)arg0;
			Float pos = position;
			
			int comp = pos.compareTo(other.position);
			if ( comp == 0) {
				// Move larger # one to center.
				//int midpos = numOnLevel / 2;
				
				
//				// First priority... # of out nodes.
//				if (this.dependents.size() > other.dependents.size()) {
//					return 1;
//				}
//				else if (this.dependents.size() < other.dependents.size()) {
//					return -1;
//				}
//				
//				// Second priority... # of out nodes
//				if (this.dependencies.size() > other.dependencies.size()) {
//					return 1;
//				}
//				else if (this.dependencies.size() < other.dependencies.size()) {
//					return -1;
//				}
//				
//				if (this instanceof DummyNode) {
//					if (arg0 instanceof DummyNode) {
//						return 0;
//					}
//					return -1;
//				}
//				else if (arg0 instanceof DummyNode){
//					return 1;
//				}
			}
			
			return comp;
		}

		public void setNumOnLevel(int numOnLevel) {
			this.numOnLevel = numOnLevel;
		}

		public int getNumOnLevel() {
			return numOnLevel;
		}
		
	}
	
	private class JobNode extends Node {
		private FlowNode flowNode;
		public JobNode(FlowNode flowNode) {
			this.flowNode = flowNode;
		}
		
		public FlowNode getFlowNode() {
			return flowNode;
		}
		
		public int getLevel() {
			return flowNode.getLevel();
		}
	}
	
	private class DummyNode extends Node {
		private int level = 0;
		public DummyNode(int level) {
			this.level = level;
		}
	}
	
	public class LayoutData {
		final FlowNode flowNode;
		public LayoutData(FlowNode flow) {
			flowNode = flow;
		}
	}
}