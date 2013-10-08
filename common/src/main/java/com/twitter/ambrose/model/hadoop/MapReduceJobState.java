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

  private int totalMappers;
  private long mapStartTime;
  private long mapEndTime;
  private int finishedMappersCount;

  private int totalReducers;
  private long reduceStartTime;
  private long reduceEndTime;
  private int finishedReducersCount;

  @JsonCreator
  public MapReduceJobState() { }

  @SuppressWarnings("deprecation")
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
    mapStartTime = 0L;
    mapEndTime = 0L;
    finishedMappersCount = 0;

    totalReducers = reduceTaskReport.length;
    reduceStartTime = 0L;
    reduceEndTime = 0L;
    finishedReducersCount = 0;

    for (TaskReport report : mapTaskReport) {
      if (mapEndTime < report.getFinishTime()) { mapEndTime = report.getFinishTime(); }
      if (report.getStartTime() < mapStartTime || mapStartTime == 0L) {
        mapStartTime = report.getStartTime();
      }

      TIPStatus status = report.getCurrentStatus();
      if (status != TIPStatus.PENDING && status != TIPStatus.RUNNING) {
        finishedMappersCount++;
      }
    }

    for (TaskReport report : reduceTaskReport) {
      if (reduceEndTime < report.getFinishTime()) { reduceEndTime = report.getFinishTime(); }
      if (report.getStartTime() < reduceStartTime || reduceStartTime == 0L) {
        reduceStartTime = report.getStartTime();
      }

      TIPStatus status = report.getCurrentStatus();
      if (status != TIPStatus.PENDING && status != TIPStatus.RUNNING) {
        finishedReducersCount++;
      }
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

  public long getMapStartTime() {
    return mapStartTime;
  }

  public void setMapStartTime(long mapTaskStartTime) {
    this.mapStartTime = mapTaskStartTime;
  }

  public long getReduceStartTime() {
    return reduceStartTime;
  }

  public void setReduceStartTime(long reduceTaskStartTime) {
    this.reduceStartTime = reduceTaskStartTime;
  }

  public long getMapEndTime() {
    return mapEndTime;
  }

  public void setMapEndTime(long mapTaskEndTime) {
    this.mapEndTime = mapTaskEndTime;
  }

  public long getReduceEndTime() {
    return reduceEndTime;
  }

  public void setReduceEndTime(long reduceTaskEndTime) {
    this.reduceEndTime = reduceTaskEndTime;
  }
}