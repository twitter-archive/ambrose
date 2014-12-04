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

import com.twitter.ambrose.server.ScriptStatusServer;
import com.twitter.ambrose.service.impl.InMemoryStatsService;


import java.io.IOException;

/**
 * Subclass of AmbroseCascadingNotifier where cascading run inside. Stats are collected using by this class via InMemoryStatsService,
 * which is what serves stats to ScriptStatusServer.
 * <P>
 * To use this class with cascading, start cascading as follows:
 * <pre>
 * EmbeddedAmbroseCascadingNotifier server = new EmbeddedAmbroseCascadingNotifier();
 * FlowStepJob.setJobNotifier(server);
 * flow.addListener(server);
 * flow.complete();
 * </pre>
 * </P>
 */
public class EmbeddedAmbroseCascadingNotifier
             extends AmbroseCascadingNotifier {

  private static final String POST_SCRIPT_SLEEP_SECS_PARAM = "ambrose.post.script.sleep.seconds";
  private InMemoryStatsService service;
  private ScriptStatusServer server;

  public EmbeddedAmbroseCascadingNotifier() {
    super(new InMemoryStatsService());
    this.service = (InMemoryStatsService) getStatsWriteService();
    this.server = new ScriptStatusServer(service, service);
    this.server.start();
  }

  @Override
  public void onCompleted(Flow flow) {
    super.onCompleted(flow);

    // sleeping keeps the app server running for a period after the job is done. if no sleep time
    // is set, still sleep for 10 seconds just to let the client finish it's polling, since it
    // doesn't stop until it get all the job complete events.
    String sleepTime = System.getProperty(POST_SCRIPT_SLEEP_SECS_PARAM, "10");

    try {
      int sleepTimeSeconds = Integer.parseInt(sleepTime);
      log.info("Job complete but sleeping for " + sleepTimeSeconds
        + " seconds to keep the CascadingStats REST server running. Hit ctrl-c to exit.");
      service.flushJsonToDisk();
      Thread.sleep(sleepTimeSeconds * 1000);
      server.stop();

    } catch (NumberFormatException e) {
      log.warn(POST_SCRIPT_SLEEP_SECS_PARAM + " param is not a valid number, not sleeping: " + sleepTime);
    } catch (IOException e) {
      log.warn("Couldn't write json to disk", e);
    } catch (InterruptedException e) {
      log.warn("Sleep interrupted", e);
    }
  }
}