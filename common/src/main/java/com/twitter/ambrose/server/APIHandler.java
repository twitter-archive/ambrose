package com.twitter.ambrose.server;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.collect.Lists;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;

import org.mortbay.jetty.HttpConnection;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.ambrose.model.DAGNode;
import com.twitter.ambrose.model.Event;
import com.twitter.ambrose.model.Job;
import com.twitter.ambrose.model.PaginatedList;
import com.twitter.ambrose.model.WorkflowSummary;
import com.twitter.ambrose.model.WorkflowSummary.Status;
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

  private static String normalize(String value) {
    if (value != null) {
      value = value.trim();
      if (value.isEmpty()) {
        value = null;
      }
    }
    return value;
  }

  private static <T extends Enum<T>> T getEnum(String value, Class<T> enumClass, T defaultValue) {
    T out = defaultValue;
    if (value != null) {
      try {
        out = Enum.valueOf(enumClass, value);
      } catch (IllegalArgumentException e) {
        // ignore
      }
    }
    return out;
  }

  private static byte[] getBytes(String value, byte[] defaultValue) {
    byte[] out = defaultValue;
    if (value != null) {
      out = BaseEncoding.base64().decode(value);
    }
    return out;
  }

  private static int getInt(String value, int defaultValue) {
    int out = defaultValue;
    if (value != null) {
      try {
        out = Integer.parseInt(value);
      } catch (NumberFormatException e) {
        // ignore
      }
    }
    return out;
  }

  private static final Logger LOG = LoggerFactory.getLogger(APIHandler.class);
  private static final String QUERY_PARAM_CLUSTER = "cluster";
  private static final String QUERY_PARAM_USER = "user";
  private static final String QUERY_PARAM_STATUS = "status";
  private static final String QUERY_PARAM_START_KEY = "startKey";
  private static final String QUERY_PARAM_WORKFLOW_ID = "workflowId";
  private static final String QUERY_PARAM_LAST_EVENT_ID = "lastEventId";
  private static final String QUERY_PARAM_MAX_EVENTS = "maxEvents";
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

    if (target.endsWith("/clusters")) {
      sendJson(request, response, workflowIndexReadService.getClusters());

    } else if (target.endsWith("/workflows")) {
      String cluster = normalize(request.getParameter(QUERY_PARAM_CLUSTER));
      String user = normalize(request.getParameter(QUERY_PARAM_USER));
      String statusParam = normalize(request.getParameter(QUERY_PARAM_STATUS));
      Status status = getEnum(statusParam, Status.class, null);
      String startRowParam = normalize(request.getParameter(QUERY_PARAM_START_KEY));
      byte[] startRow = getBytes(startRowParam, null);

      LOG.info("Submitted request for cluster={}, user={}, status={}, startRow={}", cluster, user,
          status, startRowParam);
      PaginatedList<WorkflowSummary> workflows = workflowIndexReadService.getWorkflows(
          cluster, status, user, 10, startRow);

      response.setContentType(MIME_TYPE_JSON);
      response.setStatus(HttpServletResponse.SC_OK);
      sendJson(request, response, workflows);

    } else if (target.endsWith("/dag")) {
      String workflowId = normalize(request.getParameter(QUERY_PARAM_WORKFLOW_ID));

      LOG.info("Submitted request for workflowId={}", workflowId);
      Map<String, DAGNode<Job>> dagNodeNameMap =
          statsReadService.getDagNodeNameMap(workflowId);
      Collection<DAGNode<Job>> nodes = Lists.newArrayList();
      if (dagNodeNameMap != null) {
        nodes = dagNodeNameMap.values();
      }

      response.setContentType(MIME_TYPE_JSON);
      response.setStatus(HttpServletResponse.SC_OK);
      sendJson(request, response, nodes.toArray(new DAGNode[nodes.size()]));

    } else if (target.endsWith("/events")) {
      String lastEventIdParam = normalize(request.getParameter(QUERY_PARAM_LAST_EVENT_ID));
      Integer lastEventId = getInt(lastEventIdParam, -1);
      
      Integer maxEvents = getInt(request.getParameter(QUERY_PARAM_MAX_EVENTS), -1);

      Collection<Event> events = statsReadService
          .getEventsSinceId(request.getParameter(QUERY_PARAM_WORKFLOW_ID), lastEventId, maxEvents);

      response.setContentType(MIME_TYPE_JSON);
      response.setStatus(HttpServletResponse.SC_OK);
      sendJson(request, response, events.toArray(new Event[events.size()]));

    } else if (target.endsWith(".html")) {
      response.setContentType(MIME_TYPE_HTML);
      // this is because the next handler will be picked up here and it doesn't seem to
      // handle html well. This is jank.
    }
  }
}
