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

import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.HandlerList;
import org.mortbay.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.ambrose.service.StatsReadService;

/**
 * Lite weight app server that serves both the JSON API and the Ambrose web pages powered from the
 * JSON. The port defaults to 8080 but can be overridden with the <pre>ambrose.port</pre>
 * system property. For a random port to be used, set <pre>ambrose.port=RANDOM</pre>.
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
  private static final String ROOT_PATH = "web";

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

    if ("RANDOM".equalsIgnoreCase(port)) {
      try {
        ServerSocket socket = new ServerSocket(0);
        int randomPort = socket.getLocalPort();
        socket.close();
        return randomPort;
      } catch (IOException e) {
        throw new IllegalArgumentException("Could not find random port for Ambmrose server", e);
      }
    }

    try {
      return Integer.parseInt(port);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(String.format("Invalid port passed for %s: %s", PORT_PARAM, port), e);
    }
  }

  public int getPort() { return port; };

  /**
   * Starts the server in it's own daemon thread.
   */
  public void start() {
    try {
      LOG.info(String.format("Starting ambrose web server on port %s. "
        + "Browse to http://localhost:%s/web/workflow.html to see job progress.", port, port));
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
    URL resourcesUrl = this.getClass().getClassLoader().getResource(ROOT_PATH);
    HandlerList handler = new HandlerList();
    handler.setHandlers(new Handler[]{new APIHandler(statsReadService),
      new WebAppContext(resourcesUrl.toExternalForm(), SLASH)});
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
}
