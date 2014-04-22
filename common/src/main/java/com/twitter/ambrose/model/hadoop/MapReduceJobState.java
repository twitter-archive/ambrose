package com.twitter.ambrose.model.hadoop;

import com.fasterxml.jackson.annotation.JsonCreator;

import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapred.TaskReport;

import java.io.IOException;

/**
 * Container that holds state of a MapReduce job
 */
public class MapReduceJobState {
  private String jobId;
  private String jobName;
  private String trackingURL;
  private boolean isComplete;
  private boolean isSuccessful;
  private float mapProgress;
  private float reduceProgress;
  private long jobStartTime;
  private long jobLastUpdateTime;

  private int totalMappers;
  private int finishedMappersCount;

  private int totalReducers;
  private int finishedReducersCount;

  @JsonCreator
  public MapReduceJobState() { }

  public MapReduceJobState(RunningJob runningJob,
                           TaskReport[] mapTaskReport,
                           TaskReport[] reduceTaskReport) throws IOException {
    jobId = runningJob.getID().toString();
    jobName = runningJob.getJobName();
    trackingURL = runningJob.getTrackingURL();
    isComplete = runningJob.isComplete();
    isSuccessful = runningJob.isSuccessful();
    mapProgress = runningJob.mapProgress();
    reduceProgress = runningJob.reduceProgress();

    totalMappers = mapTaskReport.length;
    totalReducers = reduceTaskReport.length;

    for (TaskReport report : mapTaskReport) {
      if (report.getStartTime() < jobStartTime || jobStartTime == 0L) {
        jobStartTime = report.getStartTime();
      }

      TIPStatus status = report.getCurrentStatus();
      if (status != TIPStatus.PENDING && status != TIPStatus.RUNNING) {
        finishedMappersCount++;
      }
    }

    for (TaskReport report : reduceTaskReport) {
      if (jobLastUpdateTime < report.getFinishTime()) { jobLastUpdateTime = report.getFinishTime(); }

      TIPStatus status = report.getCurrentStatus();
      if (status != TIPStatus.PENDING && status != TIPStatus.RUNNING) {
        finishedReducersCount++;
      }
    }

    // If not all the reducers are finished.
    if (finishedReducersCount != reduceTaskReport.length || jobLastUpdateTime == 0) {
      jobLastUpdateTime = System.currentTimeMillis();
    }
  }

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public String getTrackingURL() {
    return trackingURL;
  }

  public void setTrackingURL(String trackingURL) {
    this.trackingURL = trackingURL;
  }

  public boolean isComplete() {
    return isComplete;
  }

  public void setComplete(boolean complete) {
    isComplete = complete;
  }

  public boolean isSuccessful() {
    return isSuccessful;
  }

  public void setSuccessful(boolean successful) {
    isSuccessful = successful;
  }

  public float getMapProgress() {
    return mapProgress;
  }

  public void setMapProgress(float mapProgress) {
    this.mapProgress = mapProgress;
  }

  public float getReduceProgress() {
    return reduceProgress;
  }

  public void setReduceProgress(float reduceProgress) {
    this.reduceProgress = reduceProgress;
  }

  public int getTotalMappers() {
    return totalMappers;
  }

  public void setTotalMappers(int totalMappers) {
    this.totalMappers = totalMappers;
  }

  public int getTotalReducers() {
    return totalReducers;
  }

  public void setTotalReducers(int totalReducers) {
    this.totalReducers = totalReducers;
  }

  public int getFinishedMappersCount() {
    return finishedMappersCount;
  }

  public void setFinishedMappersCount(int finishedMappersCount) {
    this.finishedMappersCount = finishedMappersCount;
  }

  public int getFinishedReducersCount() {
    return finishedReducersCount;
  }

  public void setFinishedReducersCount(int finishedReducersCount) {
    this.finishedReducersCount = finishedReducersCount;
  }

  public long getJobStartTime() {
    return jobStartTime;
  }

  public void setJobStartTime(long jobStartTime) {
    this.jobStartTime = jobStartTime;
  }

  public long getJobLastUpdateTime() {
    return jobLastUpdateTime;
  }

  public void setJobLastUpdateTime(long jobLastUpdateTime) {
    this.jobLastUpdateTime = jobLastUpdateTime;
  }
}