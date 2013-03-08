package com.twitter.ambrose.service;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * Class that represents subgraph's "mapper/reducer" root.
 */
@JsonSerialize (
	include=JsonSerialize.Inclusion.NON_NULL
)
public class MRNode {
	private String name;
  private String jobId;
  private MRSubgNode tree;
  private String childrenNames;
  private int level = 0;

	public int getLevel() {
		return level;
	}

	public void setLevel(int level) {
		this.level = level;
	}

	public MRNode(String name ) {
	    this.name = name;
	}
	  
  @JsonCreator
  public MRNode(@JsonProperty("name") String name,
                 @JsonProperty("children") MRSubgNode children,
                 @JsonProperty("level") int level
                 ) {
    this.name = name;
    this.tree = children;
    this.level = level;
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

  public synchronized MRSubgNode getTree() {
    return tree;
  }
  
  public synchronized void setTree(MRSubgNode children) {	   
    if (children != null) {
    	this.tree = children;
	    this.childrenNames = children.getName();
    }
    
  }

  @JsonIgnore
  public synchronized String getChildrenNames() { 
    return childrenNames; 
  }

}
