/*
Copyright 2014 Twitter, Inc.

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
package com.twitter.ambrose.service.impl.hraven;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.twitter.ambrose.model.DAGNode;
import com.twitter.ambrose.model.Event;

/**
 * Utility class to test working with Hraven
 */
@SuppressWarnings("rawtypes")
public class TestHRavenStatsReadService {
  /**
   * Main method for testing reading from hraven
   */
  public static void main(String[] args) throws IOException {

    //cluster!userName!appId!runId!timestamp!flowId
    String workflowId = args[0];
    HRavenStatsReadService service = new HRavenStatsReadService();

    Map<String, DAGNode> dagMap = service.getDagNodeNameMap(workflowId);
    if (dagMap == null) {
      print("No dagNodeNameMap found for " + workflowId);
    } else {
      print(String.format("Found %d dapMap entries", dagMap.size()));
      for (Map.Entry<String, DAGNode> entry : dagMap.entrySet()) {
        DAGNode node = entry.getValue();
        String jobId = node.getJob() != null ? node.getJob().getId() : null;
        print(String.format("%s: nodeName=%s jobId=%s successors=%s",
            entry.getKey(), node.getName(), jobId, node.getSuccessorNames()));
      }
    }

    List<Event> events = service.getEventsSinceId(workflowId, -1);
    print(String.format("Found %d events", events.size()));
    for (Event event : events) {
      print(String.format("%d %d %s %s",
          event.getId(), event.getTimestamp(), event.getType(), event.getPayload()));
    }
  }

  private static void print(String object) { System.out.println(object); }
}

