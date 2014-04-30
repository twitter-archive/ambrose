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

import com.twitter.ambrose.model.PaginatedList;
import com.twitter.ambrose.model.WorkflowSummary;
import com.twitter.ambrose.model.WorkflowSummary.Status;

public class TestHRavenWorkflowIndexReadService {
  /**
   * Main method
   */
  public static void main(String[] args) throws IOException {
    String cluster = args.length > 0 ? args[0] : "dw@smf1";
    Status status = args.length > 1
        ? Status.valueOf(args[1]) : Status.SUCCEEDED;
        String username = args.length > 2 ? args[2] : null;

        HRavenWorkflowIndexReadService service = new HRavenWorkflowIndexReadService();
        PaginatedList<WorkflowSummary> paginatedList =
            service.getWorkflows(cluster, status, username, 10, null);

        print(String.format("Found %d flows", paginatedList.getResults().size()));
        for (WorkflowSummary summary : paginatedList.getResults()) {
          print(String.format("%s %s %s %d", summary.getName(), summary.getId(),
              summary.getStatus(), summary.getProgress()));
        }
  }

  private static void print(String object) { System.out.println(object); }
}
