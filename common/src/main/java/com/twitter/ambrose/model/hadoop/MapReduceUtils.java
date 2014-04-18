package com.twitter.ambrose.model.hadoop;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskReport;

import com.twitter.ambrose.model.DAGNode;
import com.twitter.ambrose.model.Event;
import com.twitter.ambrose.model.Job;
import com.twitter.ambrose.service.StatsWriteService;

public class MapReduceUtils {

  static Log log = LogFactory.getLog(MapReduceUtils.class);

  private static MapReduceJobState getMapReduceJobState(String jobId, JobClient jobClient) {
    try {
      RunningJob runningJob = jobClient.getJob(jobId);
      if (runningJob == null) {
        log.warn("Couldn't find job status for jobId: " + jobId);
        return null;
      }
      JobID jobID = runningJob.getID();
      TaskReport[] mapTaskReport = jobClient.getMapTaskReports(jobID);
      TaskReport[] reduceTaskReport = jobClient.getReduceTaskReports(jobID);

      return new MapReduceJobState(runningJob, mapTaskReport, reduceTaskReport);

    } catch (Exception e) {
      log.warn("Couldn't find job status for jobId: " + jobId);
    }
    return null;
  }
  
  public static void addMapReduceJobState(MapReduceJob job, JobClient jobClient) {
    MapReduceJobState state = getMapReduceJobState(job.getId(), jobClient);
    // only set if we can successfully get it
    if (state != null) {
      job.setMapReduceJobState(state);
    }
  }
  
  /**
   * Get the configurations at the beginning of the job flow, it will contain information
   * about the map/reduce plan and decoded pig script.
   * @param runningJob
   * @return Properties - configuration properties of the job
   */
  public void setJobConfFromFile(MapReduceJob job, JobClient jobClient) {
    Properties jobConfProperties = new Properties();
    try {
      RunningJob runningJob = jobClient.getJob(job.getId());
      if (runningJob == null) {
        log.warn("Couldn't find job status for jobId: " + job.getId());
      }
      
      log.info("RunningJob Configuration File location: " + runningJob.getJobFile());
      Path path = new Path(runningJob.getJobFile());
      
      Configuration conf = new Configuration(false);
      FileSystem fileSystem = FileSystem.get(new Configuration());
      InputStream inputStream = fileSystem.open(path);
      conf.addResource(inputStream);

      Iterator<Map.Entry<String, String>> iter = conf.iterator();
      while (iter.hasNext()) {
          Map.Entry<String, String> entry = iter.next();
          jobConfProperties.put(entry.getKey(), entry.getValue());
      }
    } catch (FileNotFoundException e) {
      log.warn("Configuration file not found for old jobsflows.");
    } catch (Exception e) {
      log.warn("Error occurred when retrieving configuration info." + e.getMessage());
    }
    job.setConfiguration(jobConfProperties);
  }
  
  public static void sendDagNodeNameMap(StatsWriteService statsWriteService, String scriptId, Map dagNodeNameMap) {
    try {
      statsWriteService.sendDagNodeNameMap(scriptId, dagNodeNameMap);
    } catch (IOException e) {
      log.error("Couldn't send dag to StatsWriteService", e);
    }
  }

  public static void pushEvent(StatsWriteService statsWriteService, String scriptId, Event event) {
    try {
      statsWriteService.pushEvent(scriptId, event);
    } catch (IOException e) {
      log.error("Couldn't send event to StatsWriteService", e);
    }
  }
}
