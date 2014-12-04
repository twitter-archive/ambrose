package com.twitter.ambrose.model;

/**
 * Holds WorkflowSummary info.
 */
public class WorkflowSummary {
  /**
   * Represents whether a workflow is running or completed.
   */
  public static enum Status {
    RUNNING,
    SUCCEEDED,
    FAILED
  }

  private String id;
  private String userId;
  private String name;
  private int progress;
  private Status status;
  private long createdAt;

  /**
   * Constructs a new WorkflowSummary.
   */
  public WorkflowSummary(String id, String userId, String name, Status status, int progress,
      long createdAt) {
    this.id = id;
    this.userId = userId;
    this.name = name;
    this.status = status;
    this.progress = progress;
    this.createdAt = createdAt;
  }

  public WorkflowSummary() {
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getUserId() {
    return userId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getProgress() {
    return progress;
  }

  public void setProgress(int progress) {
    this.progress = progress;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public long getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(long createdAt) {
    this.createdAt = createdAt;
  }
}
