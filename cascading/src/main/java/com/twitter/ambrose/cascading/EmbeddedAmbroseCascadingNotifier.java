/*
Copyright ......

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
package com.twitter.ambrose.cascading;

import cascading.flow.Flow;

import com.twitter.ambrose.model.Job;
import com.twitter.ambrose.server.ScriptStatusServer;
import com.twitter.ambrose.service.impl.InMemoryStatsService;


import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * AmbroseCascadingNotifier which buffers workflow stats in memory and starts an internal web server
 * which hosts the Ambrose dashboard.
 * <p>
 * To use this class with cascading, start cascading as follows:
 * <pre>
 * EmbeddedAmbroseCascadingNotifier server = new EmbeddedAmbroseCascadingNotifier();
 * FlowStepJob.setJobNotifier(server);
 * flow.addListener(server);
 * flow.complete();
 * </pre>
 *
 * @see com.twitter.ambrose.service.impl.InMemoryStatsService
 * @see com.twitter.ambrose.server.ScriptStatusServer
 */
public class EmbeddedAmbroseCascadingNotifier extends AmbroseCascadingNotifier {

  private static final Log LOG = LogFactory.getLog(AmbroseCascadingNotifier.class);
  private static final String POST_SCRIPT_SLEEP_SECS_PARAM = "ambrose.post.script.sleep.seconds";
  private static final int POST_SCRIPT_SLEEP_SECS_DEFAULT = 10;
  private final InMemoryStatsService<Job> service;
  private final ScriptStatusServer server;
  private final int sleepTimeSeconds;

  public EmbeddedAmbroseCascadingNotifier() {
    super(new InMemoryStatsService<Job>());
    this.service = (InMemoryStatsService<Job>) getStatsWriteService();
    this.server = new ScriptStatusServer(service, service);
    this.server.start();
    this.sleepTimeSeconds = getSleepTimeSeconds();
  }

  private int getSleepTimeSeconds() {
    String sleepTimeParam = System.getProperty(
        POST_SCRIPT_SLEEP_SECS_PARAM,
        String.valueOf(POST_SCRIPT_SLEEP_SECS_DEFAULT)
    );
    int sleepTimeSeconds = 0;
    try {
      sleepTimeSeconds = Integer.parseInt(sleepTimeParam);
    } catch (NumberFormatException e) {
      LOG.error(String.format(
          "Parameter '%s' value '%s' is not an integer",
          POST_SCRIPT_SLEEP_SECS_PARAM, sleepTimeParam), e);
    }
    if (sleepTimeSeconds <= 0) {
      sleepTimeSeconds = POST_SCRIPT_SLEEP_SECS_DEFAULT;
    }
    return sleepTimeSeconds;
  }

  @Override
  public void onCompleted(Flow flow) {
    super.onCompleted(flow);

    try {
      service.flushJsonToDisk();
    } catch (IOException e) {
      LOG.error("Couldn't write json to disk", e);
    }

    try {
      // sleeping keeps the app server running for a period after the job is done. if no sleep time
      // is set, still sleep for 10 seconds just to let the client finish it's polling, since it
      // doesn't stop until it get all the job complete events.
      LOG.info(String.format(
          "Job complete but sleeping for %s seconds to keep the embedded Ambrose server running. Hit ctrl-c to exit.",
          sleepTimeSeconds
      ));
      Thread.sleep(sleepTimeSeconds * 1000);
    } catch (InterruptedException e) {
      LOG.error("Sleep interrupted", e);
    }

    server.stop();
  }
}
