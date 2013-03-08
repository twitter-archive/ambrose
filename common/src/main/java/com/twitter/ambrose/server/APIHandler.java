package com.twitter.ambrose.server;

import java.util.*;
import com.twitter.ambrose.service.MRNode;
import com.twitter.ambrose.service.MRSubgNode;

import java.io.IOException;
import java.util.Collection;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.mortbay.jetty.HttpConnection;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.handler.AbstractHandler;

import com.twitter.ambrose.service.DAGNode;
import com.twitter.ambrose.service.StatsReadService;
import com.twitter.ambrose.service.WorkflowEvent;
import com.twitter.ambrose.util.JSONUtil;

/**
 * Handler for the API data responses.
 *
 * @author billg
 */
public class APIHandler extends AbstractHandler {
  private static final String QUERY_PARAM_WORKFLOW_ID = "workflowId";
  private static final String QUERY_PARAM_LAST_EVENT_ID = "lastEventId";

  private static final String MIME_TYPE_HTML = "text/html";
  private static final String MIME_TYPE_JSON = "application/json";

  private StatsReadService statsReadService;
  private Map<String, MRNode> mappers = null;
  private Map<String, MRNode> reducers = null;
  Map<String, List<MRNode>> m = new HashMap<String, List<MRNode>>();

  public APIHandler(StatsReadService statsReadService) {
    this.statsReadService = statsReadService;
  }

  @Override
  public void handle(String target,
                     HttpServletRequest request,
                     HttpServletResponse response,
                     int dispatch) throws IOException, ServletException {

    if (target.endsWith("/dag")) {
      response.setContentType(MIME_TYPE_JSON);
      response.setStatus(HttpServletResponse.SC_OK);

      Collection<DAGNode> nodes =
        statsReadService.getDagNodeNameMap(request.getParameter(QUERY_PARAM_WORKFLOW_ID)).values();

      sendJson(request, response, nodes.toArray(new DAGNode[nodes.size()]));
    } else if (target.endsWith("/events")) {
      response.setContentType(MIME_TYPE_JSON);
      response.setStatus(HttpServletResponse.SC_OK);
      Integer sinceId = request.getParameter(QUERY_PARAM_LAST_EVENT_ID) != null ?
              Integer.parseInt(request.getParameter(QUERY_PARAM_LAST_EVENT_ID)) : -1;

      Collection<WorkflowEvent> events =
        statsReadService.getEventsSinceId(request.getParameter(QUERY_PARAM_WORKFLOW_ID), sinceId);

      sendJson(request, response, events.toArray(new WorkflowEvent[events.size()]));
    }
    else if (target.endsWith(".html")) {
      response.setContentType(MIME_TYPE_HTML);
      // this is because the next handler will be picked up here and it doesn't seem to
      // handle html well. This is jank.
    } else if ( target.endsWith("-subg") ) {
    	String jobId = target.substring(target.lastIndexOf("/")+1 , target.lastIndexOf('-') );
        mappers = statsReadService.getMappersSubgraph(); 
        reducers = statsReadService.getReducersSubgraph();    	  
    	if ( !m.containsKey(jobId) ) {
            ArrayList<MRNode> mr = new ArrayList<MRNode>();	    	    	 	    	 
            mr.add(mappers.get(jobId));
            mr.add(reducers.get(jobId));
            m.put(jobId, mr); 
          }
    	  
    	  Collection<MRNode> subs = m.get(jobId);        	  
    	  sendJson(request, response, subs );
      }
  }
  private static void sendJson(HttpServletRequest request,
                               HttpServletResponse response, Object object) throws IOException {
    JSONUtil.writeJson(response.getWriter(), object);
    response.getWriter().close();
    setHandled(request);
  }

  private static void setHandled(HttpServletRequest request) {
    Request base_request = (request instanceof Request) ?
        (Request)request : HttpConnection.getCurrentConnection().getRequest();
    base_request.setHandled(true);
  }
}
