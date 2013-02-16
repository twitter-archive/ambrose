package DotGraphParser;

import com.twitter.ambrose.service.DAGNode;


public class CascadingEdge {
	public DAGNode source;
	public DAGNode destination;
	public String label;
	
	public CascadingEdge(DAGNode src, DAGNode dest){
		this.source = src;
		this.destination = dest;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}
	
	
}
