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

import com.twitter.ambrose.server.ScriptStatusServer;
import com.twitter.ambrose.service.impl.InMemoryStatsService;
import org.apache.pig.tools.pigstats.PigStatsUtil;

import java.io.IOException;

/**
 * Sublclass of AmbrosePigProgressNotificationListener that starts a ScriptStatusServer embedded in
 * the running Pig client VM. Stats are collected using by this class via InMemoryStatsService,
 * which is what serves stats to ScriptStatusServer.
 * <P>
 * To use this class with pig, start pig as follows:
 * <pre>
 * $ bin/pig \
 *  -Dpig.notification.listener=com.twitter.ambrose.pig.EmbeddedAmbrosePigProgressNotificationListener \
 *  -f path/to/script.pig
 * </pre>
 * Additional <pre>-D</pre> options can be set as system as system properties. Note that these must
 * be set via <pre>PIG_OPTS</pre>. For example, <pre>export PIG_OPTS=-Dambrose.port.number=8188</pre>.
 * <ul>
 *   <li><pre>ambrose.port.number</pre> (default=8080) port for the ambrose tool to listen on.</li>
 *   <li><pre>ambrose.post.script.sleep.seconds</pre> number of seconds to keep the VM running after
 *   the script is complete. This is useful to keep Ambrose up once the job is done.</li>
 * </ul>
 * </P>
 * @author billg
 */
public class EmbeddedAmbrosePigProgressNotificationListener
             extends AmbrosePigProgressNotificationListener {

  private InMemoryStatsService service;
  private ScriptStatusServer server;
  private static final String POST_SCRIPT_SLEEP_SECS_PARAM = "ambrose.post.script.sleep.seconds";

  public EmbeddedAmbrosePigProgressNotificationListener() {
    super(new InMemoryStatsService());
    this.service = (InMemoryStatsService)getStatsWriteService();

    this.server = new ScriptStatusServer(service);
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
      // if sleep time is long, display stats so users watching std out can tell things are done.
      // if sleep time is short though, don't bother, since they'll get displayed by Pig after the
      // sleep.
      if (sleepTimeSeconds > 10) {
        PigStatsUtil.displayStatistics();
      }

      log.info("Job complete but sleeping for " + sleepTimeSeconds
        + " seconds to keep the PigStats REST server running. Hit ctrl-c to exit.");
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
