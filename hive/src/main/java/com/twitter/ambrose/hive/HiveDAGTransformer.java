/*
Copyright 2013, Lorand Bendig

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
package com.twitter.ambrose.hive;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.ExecDriver;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.api.Adjacency;
import org.apache.hadoop.hive.ql.plan.api.Graph;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;

import com.twitter.ambrose.model.DAGNode;
import com.twitter.ambrose.model.Job;

/**
 * Creates a DAGNode representation from a Hive query plan
 * 
 * <pre>
 * This involves:
 * - collecting aliases, features for a given job
 * - getting dependencies between jobs
 * - creating DAGNodes for each job
 * </pre>
 * 
 * @author Lorand Bendig <lbendig@gmail.com>
 * 
 */
public class HiveDAGTransformer {

  private static final Log LOG = LogFactory.getLog(HiveDAGTransformer.class);
  private static final Pattern SUBQUERY_ALIAS = Pattern.compile("-subquery\\d+\\:([^\\-]+)");
  private static final String PENDING_JOB = "N/A";
  private static final String TEMP_JOB_ID = "temp. intermediate data";

  private final String tmpDir;
  private final String localTmpDir;
  private final QueryPlan queryPlan;
  private final List<ExecDriver> allTasks;
  private Map<String, DAGNode<Job>> nodeIdToDAGNode;

  private final Configuration conf;

  private static final String[] EMPTY_ARR = {};

  public HiveDAGTransformer(HookContext hookContext) {

    conf = hookContext.getConf();
    tmpDir = AmbroseHiveUtil.getJobTmpDir(conf);
    localTmpDir = AmbroseHiveUtil.getJobTmpLocalDir(conf);
    queryPlan = hookContext.getQueryPlan();
    allTasks = Utilities.getMRTasks(queryPlan.getRootTasks());
    if (!allTasks.isEmpty()) {
      createNodeIdToDAGNode();
    }
  }

  /**
   * Constructs DAGNodes for each Hive MR task
   * 
   * @return nodeId - DAGNode pairs
   */
  public Map<String, DAGNode<Job>> getNodeIdToDAGNode() {
    return nodeIdToDAGNode;
  }

  public int getTotalMRJobs() {
    return allTasks.size();
  }

  /**
   * Constructs DAGNodes for each Hive MR task
   */
  private void createNodeIdToDAGNode() {

    // creates DAGNodes: each node represents a MR job
    nodeIdToDAGNode = new ConcurrentSkipListMap<String, DAGNode<Job>>();
    for (Task<? extends Serializable> task : allTasks) {
      if (task.getWork() instanceof MapredWork) {
        DAGNode<Job> dagNode = asDAGNode(task);
        nodeIdToDAGNode.put(dagNode.getName(), dagNode);
      }
    }

    // get job dependencies
    Map<String, List<String>> nodeIdToDependencies = getNodeIdToDependencies();

    // wire DAGNodes
    for (Map.Entry<String, List<String>> entry : nodeIdToDependencies.entrySet()) {
      String nodeId = entry.getKey();
      List<String> successorIds = entry.getValue();
      DAGNode<Job> dagNode = nodeIdToDAGNode.get(nodeId);
      List<DAGNode<? extends Job>> dagSuccessors = new ArrayList<DAGNode<? extends Job>>(
          successorIds.size());

      for (String sId : successorIds) {
        DAGNode<Job> successor = nodeIdToDAGNode.get(sId);
        dagSuccessors.add(successor);
      }
      dagNode.setSuccessors(dagSuccessors);
    }
  }

  /**
   * Converts job properties to a DAGNode representation
   * 
   * @param task
   * @return
   */
  private DAGNode<Job> asDAGNode(Task<? extends Serializable> task) {

    MapredWork mrWork = (MapredWork) task.getWork();
    List<String> indexTableAliases = getAllJobAliases(mrWork.getPathToAliases());
    String[] features = getFeatures(mrWork.getAllOperators(), task.getTaskTag());
    String[] displayAliases = getDisplayAliases(indexTableAliases);

    // DAGNode's name of a workflow is unique among all workflows
    DAGNode<Job> dagNode = new DAGNode<Job>(AmbroseHiveUtil.getNodeIdFromNodeName(conf,
        task.getId()), new HiveJob(displayAliases, features));
    // init empty successors
    dagNode.setSuccessors(new ArrayList<DAGNode<? extends Job>>());
    return dagNode;
  }

  /**
   * Get all job aliases displayed on the GUI
   * 
   * @param indexTableAliases
   * @return
   */
  private String[] getDisplayAliases(List<String> indexTableAliases) {
    if (indexTableAliases.isEmpty()) {
      return EMPTY_ARR;
    }
    Set<String> result = new HashSet<String>();
    for (String alias : indexTableAliases) {
      if (alias.startsWith(tmpDir) || alias.startsWith(localTmpDir)) {
        result.add(TEMP_JOB_ID);
      }
      else if (alias.contains("subquery")) {
        Matcher m = SUBQUERY_ALIAS.matcher(alias);
        String dot = "";
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
          sb.append(dot);
          dot = ".";
          sb.append(m.group(1));
        }
        result.add(sb.toString());
      }
      else if (!alias.contains(":")) {
        result.add(alias);
      }
      else {
        String[] parts = alias.split(":");
        if (parts.length == 2) {
          result.add(parts[1]);
        }
        else {
          result.add(PENDING_JOB);
        }
      }
    }
    return result.toArray(new String[result.size()]);
  }

  /**
   * Creates job feature list: consists of a tasktag and a set of operators
   * 
   * @param ops
   * @param taskTagId
   * @return
   */
  private String[] getFeatures(List<Operator<?>> ops, int taskTagId) {
    if (ops == null) {
      return EMPTY_ARR;
    }
    Set<String> features = new HashSet<String>();
    for (Operator<?> op : ops) {
      OperatorType opType = op.getType();
      // some operators are discarded
      if (!skipType(opType)) {
        features.add(opType.toString());
      }
    }

    // if taskTag is other than 'NO_TAG', include it in the feature list
    if (taskTagId == Task.NO_TAG) {
      return features.toArray(new String[features.size()]);
    }
    String[] result = features.toArray(new String[features.size() + 1]);
    result[result.length - 1] = TaskTag.get(taskTagId);
    return result;
  }

  private boolean skipType(OperatorType opType) {
    return (opType == OperatorType.FILESINK || opType == OperatorType.REDUCESINK || opType == OperatorType.TABLESCAN);
  }

  /**
   * Gets all job aliases
   * 
   * @param pathToAliases
   * @return
   */
  private List<String> getAllJobAliases(LinkedHashMap<String, ArrayList<String>> pathToAliases) {
    if (pathToAliases == null || pathToAliases.isEmpty()) {
      return Collections.emptyList();
    }
    List<String> result = new ArrayList<String>();
    for (List<String> aliases : pathToAliases.values()) {
      if (aliases != null && !aliases.isEmpty()) {
        result.addAll(aliases);
      }
    }
    return result;
  }

  /**
   * Collects dependencies for each node
   * 
   * @return
   */
  private Map<String, List<String>> getNodeIdToDependencies() {
    Map<String, List<String>> result = new ConcurrentHashMap<String, List<String>>();
    try {
      Graph stageGraph = queryPlan.getQueryPlan().getStageGraph();
      if (stageGraph == null) {
        return result;
      }
      List<Adjacency> adjacencies = stageGraph.getAdjacencyList();
      if (adjacencies == null) {
        return result;
      }
      for (Adjacency adj : adjacencies) {
        String nodeId = AmbroseHiveUtil.getNodeIdFromNodeName(conf, adj.getNode());
        if (!nodeIdToDAGNode.containsKey(nodeId)) {
          continue;
        }
        List<String> children = adj.getChildren();
        if (children == null || children.isEmpty()) {
          return result; // TODO check!
        }
        List<String> filteredAdjacencies = getMRAdjacencies(children, nodeIdToDAGNode);
        result.put(nodeId, filteredAdjacencies);
      }
    }
    catch (IOException e) {
      LOG.error("Couldn't get queryPlan!", e);
    }
    return result;
  }

  /**
   * Filters adjacency children not being MR jobs
   * 
   * @param adjChildren
   * @param nodeIdToDAGNode
   * @return list of nodeIds referring to MR jobs
   */
  private List<String> getMRAdjacencies(List<String> adjChildren,
      Map<String, DAGNode<Job>> nodeIdToDAGNode) {
    List<String> result = new ArrayList<String>();
    for (String nodeName : adjChildren) {
      String nodeId = AmbroseHiveUtil.getNodeIdFromNodeName(conf, nodeName);
      if (nodeIdToDAGNode.containsKey(nodeId)) {
        result.add(nodeId);
      }
    }
    return result;
  }

}
