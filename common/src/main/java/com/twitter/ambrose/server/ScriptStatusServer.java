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

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URL;

import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.handler.DefaultHandler;
import org.mortbay.jetty.handler.HandlerList;
import org.mortbay.jetty.handler.ResourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.ambrose.model.Job;
import com.twitter.ambrose.service.StatsReadService;
import com.twitter.ambrose.service.WorkflowIndexReadService;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Light weight application server that serves both the JSON API and the Ambrose web pages powered
 * from the JSON. The port defaults to {@value #PORT_DEFAULT} but can be overridden with the {@value
 * #PORT_PARAM} system property. For a random port to be used, set {@value #PORT_PARAM} to zero or
 * {@value #PORT_RANDOM}.
 * <p/>
 * The JSON API supports the following URIs:
 * <pre>
 *   <ul>
 *     <li><code>/clusters</code> - Returns map from cluster id to name.</li>
 *     <li><code>/workflows</code> - Returns workflow summaries.</li>
 *     <li><code>/jobs</code> - Returns a workflow's jobs.</li>
 *     <li><code>/events</code> - Returns all workflow events.</li>
 *   </ul>
 * </pre>
 */
public class ScriptStatusServer implements Runnable {
  private static int getConfiguredPort() {
    String port = System.getProperty(PORT_PARAM, PORT_DEFAULT);
    if (PORT_RANDOM.equalsIgnoreCase(port)) {
      port = "0";
    }
    try {
      return Integer.parseInt(port);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(String.format(
          "Parameter '%s' value '%s' is not a valid port number", PORT_PARAM, port), e);
    }
  }

  /**
   * Name of system property used to configure port on which to bind HTTP server.
   */
  public static final String PORT_PARAM = "ambrose.port";
  /**
   * Default port on which to bind HTTP server.
   */
  public static final String PORT_DEFAULT = "8080";
  /**
   * Value of {@link #PORT_PARAM} used to signal a random port should be used.
   */
  public static final String PORT_RANDOM = "random";
  private static final Logger LOG = LoggerFactory.getLogger(ScriptStatusServer.class);
  private final WorkflowIndexReadService workflowIndexReadService;
  private final StatsReadService<Job> statsReadService;
  private final int port;
  private Server server;
  private Thread serverThread;

  public ScriptStatusServer(WorkflowIndexReadService workflowIndexReadService,
      StatsReadService<Job> statsReadService) {
    this.workflowIndexReadService = workflowIndexReadService;
    this.statsReadService = statsReadService;
    this.port = getConfiguredPort();
  }

  public int getPort() {
    return port;
  }

  /**
   * Starts the server in it's own daemon thread.
   */
  public void start() {
    try {
      LOG.info("Starting Ambrose web server on port {}", port);
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
    // override newServerSocket to log local port once bound
    Connector connector = new SocketConnector() {
      @Override
      protected ServerSocket newServerSocket(String host, int port, int backlog)
          throws IOException {
        ServerSocket ss = super.newServerSocket(host, port, backlog);
        int localPort = ss.getLocalPort();
        LOG.info("Ambrose web server listening on port {}", localPort);
        LOG.info("Browse to http://localhost:{}/ to see job progress", localPort);
        return ss;
      }
    };
    connector.setPort(port);
    server = new Server();
    server.setConnectors(new Connector[]{connector});

    // this needs to be loaded via the jar'ed resources, not the relative dir
    URL resourceUrl = checkNotNull(
        APIHandler.class.getClassLoader().getResource("web"), "Failed to find resource 'web'");
    ResourceHandler resourceHandler = new ResourceHandler();
    resourceHandler.setWelcomeFiles(new String[]{ "workflow.html" });
    resourceHandler.setResourceBase(resourceUrl.toExternalForm());
    HandlerList handler = new HandlerList();
    handler.setHandlers(new Handler[] {
        resourceHandler,
        new APIHandler(workflowIndexReadService, statsReadService),
        new DefaultHandler()
    });

    server.setHandler(handler);
    server.setStopAtShutdown(false);

    try {
      server.start();
      server.join();
    } catch (Exception e) {
      LOG.error("Error launching ScriptStatusServer", e);
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
}
