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

import com.twitter.ambrose.model.DAGNode;
import com.twitter.ambrose.model.Event;
import com.twitter.ambrose.model.Job;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Service that accepts the DAGNode map and push events. Implementations of this service might write
 * these objects somewhere where they can be retrieved later by a StatsReadService (i.e. to disk, to
 * memory, to a socket, to a DB, etc.).
 *
 * @author billg
 */
public interface StatsWriteService<T extends Job> {
  
  /**
   * Initialize the StatsWriteService from configuration properties
   * @param properties configuration properties
   */
  public void initWriteService(Properties properties) throws IOException;

  /**
   * Send a map of all DAGNodes in the workflow. The structure of the DAG is assumed to be immutable.
   * For that reason, visualization clients are expected to request the DAG once for initial
   * rendering and then poll for events after that. For that reason this method only makes sense to
   * be called once. Subsequent calls to modify the DAG will likely go unnoticed.
   * @param workflowId the id of the workflow being updated
   * @param dagNodeNameMap a Map of DAGNodes where the key is the DAGNode name
   */
  public void sendDagNodeNameMap(String workflowId, Map<String, DAGNode<T>> dagNodeNameMap) throws IOException;

  /**
   * Push an events for a given workflow.
   *
   * @param workflowId the id of the workflow being updated
   * @param event the event bound to the workflow
   */
  public void pushEvent(String workflowId, Event event) throws IOException;
}
