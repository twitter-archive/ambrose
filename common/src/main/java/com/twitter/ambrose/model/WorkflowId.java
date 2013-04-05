package com.twitter.ambrose.model;

/**
 * WorkflowId sent to the client is a composite of a bunch of information required to later fetch
 * the DAG and events.
 */
public final class WorkflowId {
  public static WorkflowId parseString(String workflowId) {
    String[] parts = workflowId.split(DELIM, 6);
    return new WorkflowId(parts[0], parts[1], parts[2],
        Long.parseLong(parts[3]), Long.parseLong(parts[4]), parts[5]);
  }

  private static final String DELIM = "!";
  private static final String PATTERN = "%s!%s!%s!%d!%d!%s";
  private final String cluster;
  private final String userId;
  private final String appId;
  private long runId;
  private long timestamp;
  private String flowId;

  public WorkflowId(String cluster, String userId, String appId,
      long runId, long timestamp, String flowId) {
    this.cluster = cluster;
    this.userId = userId;
    this.appId = appId;
    this.runId = runId;
    this.timestamp = timestamp;
    this.flowId = flowId;
  }

  public String getCluster() {
    return cluster;
  }

  public String getUserId() {
    return userId;
  }

  public String getAppId() {
    return appId;
  }

  public long getRunId() {
    return runId;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public String getFlowId() {
    return flowId;
  }

  public String toId() {
    return String.format(PATTERN, cluster, userId, appId, runId, timestamp, flowId);
  }
}
