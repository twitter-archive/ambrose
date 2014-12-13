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
package com.twitter.ambrose.pig;

import java.io.IOException;

import com.twitter.ambrose.model.Job;
import com.twitter.ambrose.server.ScriptStatusServer;
import com.twitter.ambrose.service.impl.InMemoryStatsService;

/**
 * Subclass of AmbrosePigProgressNotificationListener that starts a ScriptStatusServer embedded in
 * the running Pig client VM. Stats are collected using by this class via InMemoryStatsService,
 * which is what serves stats to ScriptStatusServer.
 * <p/>
 * To use this class with pig, start pig as follows:
 * <pre>
 * $ pig \
 * -Dpig.notification.listener=\
 * com.twitter.ambrose.pig.EmbeddedAmbrosePigProgressNotificationListener \
 * -f path/to/script.pig
 * </pre>
 * Additional {@code -D} options can be set as system as system properties. Note that these must be
 * set via {@code PIG_OPTS}. For example, {@code export PIG_OPTS=-Dambrose.port=8188}.
 * <pre>
 *   <ul>
 *     <li><code>{@value ScriptStatusServer#PORT_PARAM}</code> - Port for the ambrose server to
 * listen on. Defaults to {@value ScriptStatusServer#PORT_DEFAULT}.</li>
 *     <li><code>{@value #POST_SCRIPT_SLEEP_SECS_PARAM}</code> - Number of seconds to keep the VM
 * running after the script is complete.</li>
 *   </ul>
 * </pre>
 */
public class EmbeddedAmbrosePigProgressNotificationListener
    extends AmbrosePigProgressNotificationListener {
  private static final String POST_SCRIPT_SLEEP_SECS_PARAM = "ambrose.post.script.sleep.seconds";
  private InMemoryStatsService<Job> service;
  private ScriptStatusServer server;

  public EmbeddedAmbrosePigProgressNotificationListener() {
    super(new InMemoryStatsService<Job>());
    this.service = (InMemoryStatsService<Job>) getStatsWriteService();
    this.server = new ScriptStatusServer(service, service);
    this.server.start();
  }

  @Override
  public void launchCompletedNotification(String scriptId, int numJobsSucceeded) {
    super.launchCompletedNotification(scriptId, numJobsSucceeded);

    // sleeping keeps the app server running for a period after the job is done. if no sleep time
    // is set, still sleep for 10 seconds just to let the client finish it's polling, since it
    // doesn't stop until it get all the job complete events.
    String sleepTime = System.getProperty(POST_SCRIPT_SLEEP_SECS_PARAM, "10");

    try {
      int sleepTimeSeconds = Integer.parseInt(sleepTime);

      log.info("Job complete but sleeping for " + sleepTimeSeconds
          + " seconds to keep the PigStats REST server running. Hit ctrl-c to exit.");
      service.flushJsonToDisk();
      Thread.sleep(sleepTimeSeconds * 1000);
      server.stop();

    } catch (NumberFormatException e) {
      log.warn(POST_SCRIPT_SLEEP_SECS_PARAM + " param is not a valid number, not sleeping: " +
          sleepTime);
    } catch (IOException e) {
      log.warn("Couldn't write json to disk", e);
    } catch (InterruptedException e) {
      log.warn("Sleep interrupted", e);
    }
  }

}
