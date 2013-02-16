package DotGraphParser;


import com.twitter.ambrose.service.DAGNode;
import DotGraphParser.CascadingEdge;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.lang.management.GarbageCollectorMXBean;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;



public class DotParser {
  
  protected List<DAGNode> graphNodes;
  protected Map<String,String> nodeNamesMap;
  protected List <CascadingEdge> graphEdges;
  
  /** This is the input containing DOT file  to be parsed */
  protected Reader m_input;
  
  /**  This holds the name of the graph if there is any otherwise it is null */
  protected String m_graphName;
  
  /**
   *
   *  Dot parser Constructor
   *
   * @param input - The input, if passing in a string then
   *                encapsulate that in String reader object
   * @param nodes - Vector to put in GraphNode objects,
   *                corresponding to the nodes parsed in from
   *                the input
   * @param edges - Vector to put in GraphEdge objects,
   *                corresponding to the edges parsed in from
   *                the input
   */
  
  public DotParser(Reader input, List<DAGNode> nodes, Map<String,String> names,List<CascadingEdge> edges) {
	  this.graphNodes = nodes;
          this.nodeNamesMap = names;
	  this.graphEdges = edges;
	  this.m_input = input;
  }
  
  
  /**
   * This method parses the string or the InputStream that we
   * passed in through the constructor and builds up the
   * m_nodes and m_edges vectors
   *
   * @return - returns the name of the graph
   */
  public String parse() {
    StreamTokenizer tk = new StreamTokenizer(new BufferedReader(m_input));
    setSyntax(tk);
    
    graph(tk);
    
    return m_graphName;
  }
  
  
  /**
   * This method sets the syntax of the StreamTokenizer.
   * i.e. set the whitespace, comment and delimit chars.
   *
   */
  protected void setSyntax(StreamTokenizer tk) {
    tk.resetSyntax();
    tk.eolIsSignificant(false);
    tk.slashStarComments(true);
    tk.slashSlashComments(true);
    tk.whitespaceChars(0,' ');
    tk.wordChars(' '+1,'\u00ff');
    tk.ordinaryChar('[');
    tk.ordinaryChar(']');
    tk.ordinaryChar('{');
    tk.ordinaryChar('}');
    tk.ordinaryChar('-');
    tk.ordinaryChar('>');
    tk.ordinaryChar('/');
    tk.ordinaryChar('*');
    tk.quoteChar('"');
    tk.whitespaceChars(';',';');
    tk.ordinaryChar('=');
  }
  
  /*************************************************************
   *
   * Following methods parse the DOT input and mimic the DOT
   * language's grammar in their structure
   *
   *************************************************************
   */
  protected void graph(StreamTokenizer tk) {
    try {
      tk.nextToken();
      if(tk.ttype==tk.TT_WORD) {
        if(tk.sval.equalsIgnoreCase("digraph")) {
          tk.nextToken();
          if(tk.ttype==tk.TT_WORD) {
            m_graphName = tk.sval;
            tk.nextToken();
          }
          
          while(tk.ttype!='{') {
            System.err.println("Error at line "+tk.lineno()+" ignoring token "+
            tk.sval);
            tk.nextToken();
            if(tk.ttype==tk.TT_EOF)
              return;
          }
          stmtList(tk);
        }
        else if(tk.sval.equalsIgnoreCase("graph"))
          System.err.println("Error. Undirected graphs cannot be used");
        else
          System.err.println("Error. Expected graph or digraph at line "+
          tk.lineno());
      }
    }
    catch(Exception ex) { ex.printStackTrace(); }
    
    for(int i=0; i<graphNodes.size(); i++){
    	System.out.println(graphNodes.get(i).getName());
        graphNodes.get(i).setSuccessors(new HashSet<DAGNode>());
    }
    for(int i=0; i<graphEdges.size(); i++) {

      CascadingEdge e  = (CascadingEdge)graphEdges.get(i);
      DAGNode srcNode  = searchNodesByName(e.source.getName());
      System.out.println("=============="+e.source.getName()+"========!@0"+srcNode);
      DAGNode destNode = searchNodesByName(e.destination.getName());
      System.out.println("==============="+e.destination.getName()+"=======!@0"+destNode);

      for(int j=0; j<graphEdges.size(); j++){
          System.out.println(graphEdges.get(j).source.getName());
          System.out.println(graphEdges.get(j).destination.getName());
      }

      if(srcNode.getSuccessors() == null){
       Collection<DAGNode> successorNames = new HashSet<DAGNode>();
       successorNames.add(destNode);
       srcNode.setSuccessors(successorNames);
      }else{
       srcNode.getSuccessors().add(destNode);
       srcNode.getSuccessorNames().add(destNode.getName());
      }
    }
  }
  
  
  protected void stmtList(StreamTokenizer tk) throws Exception{
    tk.nextToken();
    if(tk.ttype=='}' || tk.ttype==tk.TT_EOF)
      return;
    else {
      stmt(tk);
      stmtList(tk);
    }
  }
  
  
  protected void stmt(StreamTokenizer tk) {
    //tk.nextToken();
    
    if(tk.sval.equalsIgnoreCase("graph") || tk.sval.equalsIgnoreCase("node") ||
    tk.sval.equalsIgnoreCase("edge") )
      ; //attribStmt(k);
    else {
      try {
    	DAGNode tempNode = nodeID(tk);
        tk.nextToken();
        if(tk.ttype == '['){
          int nodeindex = graphNodes.size() - 1;
          nodeStmt(tk, tempNode);
        }
        else if(tk.ttype == '-'){
          //System.out.println("################"+this.graphNodes.size());
          
          edgeStmt(tk, tempNode);
        }
        else
          System.err.println("error at lineno "+tk.lineno()+" in stmt");
      }
      catch(Exception ex) {
        System.err.println("error at lineno "+tk.lineno()+" in stmtException");
        ex.printStackTrace();
      }
    }
  }
  
  
  protected DAGNode nodeID(StreamTokenizer tk) throws Exception{
      System.out.println("=======nodeId======="+tk.sval);
	DAGNode node = searchNodesById(tk.sval);
    if(tk.ttype=='"' || tk.ttype==tk.TT_WORD || (tk.ttype>='a' && tk.ttype<='z')
    || (tk.ttype>='A' && tk.ttype<='Z') || tk.ttype==tk.TT_NUMBER) {
      if(graphNodes!=null && node==null ) {
    	  node = new DAGNode(tk.sval, null, new String[1], "Cascading");
        //System.out.println("Added node >"+tk.sval+"<");
      }
    }
    else
    { throw new Exception(); }
    
    return node;
    //tk.nextToken();
  }
  
  
  protected void nodeStmt(StreamTokenizer tk, DAGNode node)
  throws Exception {
	  
    tk.nextToken();
    
    if(tk.ttype==']' || tk.ttype==tk.TT_EOF)
      return;
    else if(tk.ttype==tk.TT_WORD) {
      
      if(tk.sval.equalsIgnoreCase("label")) {
 
        tk.nextToken();
        if(tk.ttype=='=') {
          tk.nextToken();
          if(tk.ttype==tk.TT_WORD || tk.ttype=='"'){
            //node.name = tk.sval;
                node.getFeatures()[0] = tk.sval;
                String newNodeName = getNodeNameFromLabel(tk.sval);
                nodeNamesMap.put(node.getName(), newNodeName);
          	graphNodes.add(new DAGNode(newNodeName, null, node.getFeatures(), "Cascading"));
          }
          else {
            System.err.println("couldn't find label at line "+tk.lineno());
            tk.pushBack();
          }
        }
        else {
          System.err.println("couldn't find label at line "+tk.lineno());
          tk.pushBack();
        }
      }
      
    }
    nodeStmt(tk, node);
  }
  
  
  protected void edgeStmt(StreamTokenizer tk, DAGNode node)
  throws Exception {

    tk.nextToken();
    
    CascadingEdge edge=null;
    if(tk.ttype=='>') {
      tk.nextToken();
      if(tk.ttype=='{') {
        while(true) {
          tk.nextToken();
          if(tk.ttype=='}')
            break;
          else {
            DAGNode srcNode = node;
            DAGNode destNode = nodeID(tk);
              System.out.println("========edge======="+tk.sval+"++++++"+destNode.getName());
            edge = new CascadingEdge(srcNode, destNode);
            
            if( graphEdges!=null && !(graphEdges.contains(edge)) ) {
            	graphEdges.add( edge );
              //System.out.println("Added edge from "+
              //                  ((GraphNode)(m_nodes.elementAt(nindex))).ID+
              //                  " to "+
              //	        ((GraphNode)(m_nodes.elementAt(e.dest))).ID);
            }
          }
        }
      }
      else {
        DAGNode srcNode = node;
        DAGNode destNode =nodeID(tk);
        edge = new CascadingEdge(srcNode, destNode);
        if( graphEdges!=null && !(graphEdges.contains(edge)) ) {
        	graphEdges.add( edge );
          //System.out.println("Added edge from "+
          //                 ((GraphNode)(m_nodes.elementAt(nindex))).ID+" to "+
          //		     ((GraphNode)(m_nodes.elementAt(e.dest))).ID);
        }
      }
    }
    else if(tk.ttype=='-') {
      System.err.println("Error at line "+tk.lineno()+
      ". Cannot deal with undirected edges");
      if(tk.ttype==tk.TT_WORD)
        tk.pushBack();
      return;
    }
    else {
      System.err.println("Error at line "+tk.lineno()+" in edgeStmt");
      if(tk.ttype==tk.TT_WORD)
        tk.pushBack();
      return;
    }
    
    tk.nextToken();
    
    if(tk.ttype=='[')
      edgeAttrib(tk, edge);
    else
      tk.pushBack();
  }
  
  
  protected void edgeAttrib(StreamTokenizer tk, final CascadingEdge e)
  throws Exception {
    tk.nextToken();
    
    if(tk.ttype==']' || tk.ttype==tk.TT_EOF)
      return;
    else if(tk.ttype==tk.TT_WORD) {
      
      if(tk.sval.equalsIgnoreCase("label")) {
        
        tk.nextToken();
        if(tk.ttype=='=') {
          tk.nextToken();
          if(tk.ttype==tk.TT_WORD || tk.ttype=='"')
        	e.setLabel(tk.sval);
          else {
            System.err.println("couldn't find label at line "+tk.lineno());
            tk.pushBack();
          }
        }
        else {
          System.err.println("couldn't find label at line "+tk.lineno());
          tk.pushBack();
        }
      }
    }
    edgeAttrib(tk, e);
  }

  protected String getNodeNameFromLabel(String label){
      String name = "";
      boolean finish = false;
      int i = 1;
      while(!finish){
          char c = label.charAt(i);
          if(c != ']'){
              name += c;
              i++;
          } else{
              finish = true;
          }
      }
      return name;
  }
  protected DAGNode searchNodesById(String id) {
    DAGNode node = null;
    boolean found = false;
    int i = 0;
    System.out.println(id);
    String nodeName = nodeNamesMap.get((String)id);
    System.out.println(nodeName);
    if( nodeName != null){
        while(!found && i < graphNodes.size()){
            System.out.println();
            if (graphNodes.get(i).getName().equals(nodeName)){
                node = graphNodes.get(i);
                found = true;
            }
            i++;
        }
    }
    return node;
    }
    protected DAGNode searchNodesByName(String name) {
        DAGNode node = null;
        boolean found = false;
        int i = 0;
        System.out.println(name);
        while(!found && i < graphNodes.size()){
            System.out.println();
            if (graphNodes.get(i).getName().equals(name)){
                node = graphNodes.get(i);
                found = true;
            }
            i++;
        }

        return node;
  }
  
} // DotParser