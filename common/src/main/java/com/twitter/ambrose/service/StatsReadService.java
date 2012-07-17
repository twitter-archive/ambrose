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
package com.twitter.ambrose.service;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Service that serves the DAGNode map and push events. Implementations of this service might read
 * these objects from a location that StatsWriteService wrote to (i.e. disk, memory, a socket, a DB,
 * etc.).
 *
 * @author billg
 */
public interface StatsReadService {

  /**
   * Get a map of all DAGNodes in the workflow.
   * @param workflowId the id of the workflow being fetched
   * @return a Map of DAGNodes where the key is the DAGNode name
   */
  public Map<String, DAGNode> getDagNodeNameMap(String workflowId) throws IOException;

  /**
   * Get all events for a given workflow since eventId. To get the entire list of events, pass a
   * negative eventId.
   *
   * @param workflowId the id of the workflow being accessed
   * @param eventId the eventId that all returned events will be greater than
   * @return a Collection of WorkflowEvents, ordered by eventId ascending
   */
  public Collection<WorkflowEvent> getEventsSinceId(String workflowId, int eventId) throws IOException;
}
