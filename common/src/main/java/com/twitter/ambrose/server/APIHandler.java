package com.twitter.ambrose.server;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Charsets;

import org.mortbay.jetty.HttpConnection;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.handler.AbstractHandler;

import com.twitter.ambrose.model.DAGNode;
import com.twitter.ambrose.model.Event;
import com.twitter.ambrose.model.Job;
import com.twitter.ambrose.model.PaginatedList;
import com.twitter.ambrose.model.WorkflowSummary;
import com.twitter.ambrose.service.StatsReadService;
import com.twitter.ambrose.service.WorkflowIndexReadService;
import com.twitter.ambrose.util.JSONUtil;

/**
 * Handler for the API data responses.
 *
 * @author billg
 */
public class APIHandler extends AbstractHandler {
  private static void sendJson(HttpServletRequest request,
      HttpServletResponse response, Object object) throws IOException {
    JSONUtil.writeJson(response.getWriter(), object);
    response.getWriter().close();
    setHandled(request);
  }

  private static void setHandled(HttpServletRequest request) {
    Request base_request = (request instanceof Request) ?
        (Request) request : HttpConnection.getCurrentConnection().getRequest();
    base_request.setHandled(true);
  }

  private static final String QUERY_PARAM_CLUSTER = "cluster";
  private static final String QUERY_PARAM_USER_ID = "userId";
  private static final String QUERY_PARAM_STATUS = "status";
  private static final String QUERY_PARAM_START_KEY = "startKey";
  private static final String QUERY_PARAM_WORKFLOW_ID = "workflowId";
  private static final String QUERY_PARAM_LAST_EVENT_ID = "lastEventId";
  private static final String MIME_TYPE_HTML = "text/html";
  private static final String MIME_TYPE_JSON = "application/json";
  private WorkflowIndexReadService workflowIndexReadService;
  private StatsReadService<Job> statsReadService;

  public APIHandler(WorkflowIndexReadService workflowIndexReadService,
      StatsReadService<Job> statsReadService) {
    this.workflowIndexReadService = workflowIndexReadService;
    this.statsReadService = statsReadService;
  }

  @Override
  public void handle(String target,
      HttpServletRequest request,
      HttpServletResponse response,
      int dispatch) throws IOException, ServletException {

    if (target.endsWith("/workflows")) {
      response.setContentType(MIME_TYPE_JSON);
      response.setStatus(HttpServletResponse.SC_OK);
      String cluster = request.getParameter(QUERY_PARAM_CLUSTER);
      String userId = request.getParameter(QUERY_PARAM_USER_ID);
      String status = request.getParameter(QUERY_PARAM_STATUS);
      String startKey = request.getParameter(QUERY_PARAM_START_KEY);

      PaginatedList<WorkflowSummary> workflows =
          workflowIndexReadService.getWorkflows(
              cluster,
              status != null ? WorkflowSummary.Status.valueOf(status) : null,
              userId, 10,
              startKey != null ? startKey.getBytes(Charsets.UTF_8) : null);
      sendJson(request, response, workflows);

    } else if (target.endsWith("/dag")) {
      response.setContentType(MIME_TYPE_JSON);
      response.setStatus(HttpServletResponse.SC_OK);

      Map<String, DAGNode<Job>> dagNodeNameMap =
          statsReadService.getDagNodeNameMap(request.getParameter(QUERY_PARAM_WORKFLOW_ID));
      Collection<DAGNode<Job>> nodes = dagNodeNameMap.values();
      sendJson(request, response, nodes.toArray(new DAGNode[nodes.size()]));

    } else if (target.endsWith("/events")) {
      response.setContentType(MIME_TYPE_JSON);
      response.setStatus(HttpServletResponse.SC_OK);
      Integer sinceId = request.getParameter(QUERY_PARAM_LAST_EVENT_ID) != null ?
          Integer.parseInt(request.getParameter(QUERY_PARAM_LAST_EVENT_ID)) : -1;

      Collection<Event> events =
          statsReadService.getEventsSinceId(request.getParameter(QUERY_PARAM_WORKFLOW_ID), sinceId);
      sendJson(request, response, events.toArray(new Event[events.size()]));

    } else if (target.endsWith(".html")) {
      response.setContentType(MIME_TYPE_HTML);
      // this is because the next handler will be picked up here and it doesn't seem to
      // handle html well. This is jank.
    }
  }
}
