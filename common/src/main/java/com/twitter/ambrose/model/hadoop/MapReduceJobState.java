package com.twitter.ambrose.model.hadoop;

import org.apache.hadoop.mapred.RunningJob;
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
  private int totalReducers;

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
    totalReducers = reduceTaskReport.length;
  }

  public String getJobId() {
    return jobId;
  }

  public String getJobName() {
    return jobName;
  }

  public String getTrackingURL() {
    return trackingURL;
  }

  public boolean isComplete() {
    return isComplete;
  }

  public boolean isSuccessful() {
    return isSuccessful;
  }

  public float getMapProgress() {
    return mapProgress;
  }

  public float getReduceProgress() {
    return reduceProgress;
  }

  public int getTotalMappers() {
    return totalMappers;
  }

  public int getTotalReducers() {
    return totalReducers;
  }
}
