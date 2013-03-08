package com.twitter.ambrose.service;

import java.util.Collection;
import java.util.HashSet;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * Class that represents a physical operator node in the subgraph of the mapper/reducer physical plan.
 * The job name must not be null. At DAG creation time
 * the jobID will probably be null. Ideally this will be set on the node when the job is started. 
 *
 */
@JsonSerialize(
  include = JsonSerialize.Inclusion.NON_NULL
)
public class MRSubgNode {    
	private String name;
	private String jobId;
	private int levels;
	private Collection<MRSubgNode> children;
	private Collection<String> childrenNames;
	private String details;		

	public String getDetails() {
		return details;
	}

	public void setDetails(String details) {
		this.details = details;
	}

	public MRSubgNode(String name) {
		this.name = name;
		setLevels(0);
	}

	@JsonCreator
	public MRSubgNode(@JsonProperty("name") String name,
			@JsonProperty("children") Collection<MRSubgNode> children,
			@JsonProperty("details") String details)
			{
		this.name = name;
		this.children = children;
		this.details = details;
	}

	public String getName() {
		return name;
	}

	@JsonIgnore
	public String getJobId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public synchronized Collection<MRSubgNode> getChildren() {
		return children;
	}

	public synchronized void setChildren(Collection<MRSubgNode> children) {
		Collection<String> childrenNames = new HashSet<String>();
		if (children != null) {
			for (MRSubgNode node : children) {
				childrenNames.add(node.getName());
			}
		}
		this.children = children;
		this.childrenNames = childrenNames;
	}

	@JsonIgnore
	public synchronized Collection<String> getChildrenNames() {
		return childrenNames;
	}

	public void setLevels(int levels) {
		this.levels = levels + 1;
	}

	@JsonIgnore
	public int getLevels() {
		return levels;
	}

}
