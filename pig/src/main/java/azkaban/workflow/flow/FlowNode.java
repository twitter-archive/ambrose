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

import java.util.HashSet;
import java.util.Set;

import java.awt.geom.Point2D;

import azkaban.common.utils.Props;


public class FlowNode {
	public static final String NORMAL = "normal";
	public static final String DISABLED = "disabled";
	public static final String RUNNING = "running";
	public static final String FAILED = "failed";
	public static final String SUCCEEDED = "succeeded";
	
	private String alias;
	private Set<String> dependencies = new HashSet<String>();
	private Set<String> dependents = new HashSet<String>();
	private Props prop = new Props();
	private int level = 0;
	private String status = NORMAL;
	private Point2D position = new Point2D.Double();
	
	public FlowNode(String alias) {
		this.alias = alias;
	}
	
	public FlowNode(FlowNode other) {
		this.alias = other.alias;
		this.dependencies.addAll(other.dependencies);
		this.dependents.addAll(other.dependents);
		this.prop = Props.clone(other.getProps());
		this.level = other.level;
		this.status = other.status;
		this.position = other.position;
	}
	
	public void setProps(Props prop) {
		this.prop = prop;
	}
	
	public Props getProps() {
		return this.prop;
	}
	
	public void setDependencies(Set<String> dependencies) {
		this.dependencies = dependencies;
	}
	
	public Set<String> getDependencies() {
		return dependencies;
	}
	
	public Set<String> getDependents() {
		return dependents;
	}
	
	public void addDependent(String dependent) {
		dependents.add(dependent);
	}
	
	public String getAlias() {
		return alias;
	}
	
	public int getLevel() {
		return level;
	}
	
	public void setLevel(int level) {
		this.level = level;
	}
	
	public void setPosition(double x, double y) {
		position = new Point2D.Double(x, y);
	}
	
	public double getX() {
		return position.getX();
	}
	
	public double getY() {
		return position.getY();
	}
	
	public String getStatus() {
		return status;
	}
	
	public void setStatus(String status) {
		this.status = status;
	}
}