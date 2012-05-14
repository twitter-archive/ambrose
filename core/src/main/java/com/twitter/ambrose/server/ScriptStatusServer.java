/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.twitter.ambrose.server;

import com.twitter.ambrose.service.DAGNode;
import com.twitter.ambrose.service.DAGTransformer;
import com.twitter.ambrose.service.StatsReadService;
import com.twitter.ambrose.service.WorkflowEvent;
import com.twitter.ambrose.service.impl.SugiyamaLayoutTransformer;
import com.twitter.ambrose.util.JSONUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mortbay.jetty.HttpConnection;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;
import org.mortbay.jetty.webapp.WebAppContext;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.List;

/**
 * Lite weight app server that serves both the JSON API and the Ambrose web pages powered from the
 * JSON. The port defaults to 8080 but can be overridden with the <pre>ambrose.port.number</pre>
 * system property.
 * <P>
 * The JSON API supports the following URIs:
 * <ul>
 *   <li><pre>/dag</pre> returns the DAG of the workflow</li>
 *   <li><pre>/events</pre> returns all events since the start of the workflow. Optionally the
 *   <pre>sinceId=&;teventId></pre> query param can be used to return only events since a given
 *   event id</li>
 * </ul>
 * </P>
 * <P>
 * </P>
 * @author billg
 */
public class ScriptStatusServer implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(ScriptStatusServer.class);

  private static final String SLASH = "/";
  private static final String WEB_RESOURCES_PATH = "web";
  private static final String QUERY_PARAM_WORKFLOW_ID = "workflowId";
  private static final String QUERY_PARAM_SINCE = "sinceId";

  private static final String MIME_TYPE_HTML = "text/html";
  private static final String MIME_TYPE_JSON = "application/json";

  private static final String PORT_PARAM = "ambrose.port";
  private static final String DEFAULT_PORT = "8080";

  private int port;
  private StatsReadService statsReadService;
  private Server server;
  private Thread serverThread;

  public ScriptStatusServer(StatsReadService statsReadService) {
    this.statsReadService = statsReadService;
    this.port = getConfiguredPort();
  }

  private static int getConfiguredPort() {
    String port = System.getProperty(PORT_PARAM, DEFAULT_PORT);
    try {
      return Integer.parseInt(port);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(String.format("Invalid port passed for %s: %s", PORT_PARAM, port), e);
    }
  }

  /**
   * Starts the server in it's own daemon thread.
   */
  public void start() {
    try {
      LOG.info(String.format("Starting ambrose web server on port %s. "
        + "Browse to http://localhost:%s/ to see job progress.", port, port));
      serverThread = new Thread(this);
      serverThread.setDaemon(true);
      serverThread.start();
    } catch (Exception e) {
      LOG.error("Could not start ScriptStatusServer", e);
    }
  }

  /**
   * Run the server in the current thread.
   */
  @Override
  public void run() {

    server = new Server(port);

    // this needs to be loaded via the jar'ed resources, not the relative dir
    URL resourcesUrl = this.getClass().getClassLoader().getResource(WEB_RESOURCES_PATH);
    HandlerList handler = new HandlerList();
    handler.setHandlers(new Handler[]{new APIHandler(), new WebAppContext(resourcesUrl.toExternalForm(), SLASH)});
    server.setHandler(handler);
    server.setStopAtShutdown(false);

    try {
      server.start();
      server.join();
    } catch (Exception e) {
      LOG.error("Error launching ScriptStatusServer on port " + port, e);
    }
  }

  /**
   * Stop the server.
   */
  public void stop() {
    if (server != null) {
      try {
        server.stop();
      } catch (Exception e) {
        LOG.warn("Error stopping Jetty server", e);
      }
    }
  }

  public class APIHandler extends AbstractHandler {
    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request,
        HttpServletResponse response) throws IOException, ServletException {
      if (target.endsWith("/dag")) {
        response.setContentType(MIME_TYPE_JSON);
        response.setStatus(HttpServletResponse.SC_OK);

        Collection<DAGNode> nodes =
          statsReadService.getDagNodeNameMap(request.getParameter(QUERY_PARAM_WORKFLOW_ID)).values();

        // add the x, y coordinates
        DAGTransformer dagTransformer = new SugiyamaLayoutTransformer(true);
        dagTransformer.transform(nodes);

        sendJson(request, response, nodes.toArray(new DAGNode[nodes.size()]));
      } else if (target.endsWith("/events")) {
        response.setContentType(MIME_TYPE_JSON);
        response.setStatus(HttpServletResponse.SC_OK);
        Integer sinceId = request.getParameter(QUERY_PARAM_SINCE) != null ?
                Integer.getInteger(request.getParameter(QUERY_PARAM_SINCE)) : -1;

        Collection<WorkflowEvent> events =
          statsReadService.getEventsSinceId(request.getParameter(QUERY_PARAM_WORKFLOW_ID), sinceId);

        sendJson(request, response, events.toArray(new WorkflowEvent[events.size()]));
      }
      else if (target.endsWith(".html")) {
        response.setContentType(MIME_TYPE_HTML);
        // this is because the next handler will be picked up here and it doesn't seem to
        // handle html well. This is jank.
      }
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
        (Request)request : AbstractHttpConnection.getCurrentConnection().getRequest();
    base_request.setHandled(true);
  }
}
