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
package com.twitter.ambrose.model.hadoop;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Enumeration;
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

/**
 * Helper utilities to get information about a mapreduce job.
 * APIs-
 * * Progress information of a job by communicating with jobtracker
 * * Provides api to get configuration of the job
 * 
 * @author amokashi
 *
 */
public class MapReduceHelper {

  private static Log log = LogFactory.getLog(MapReduceHelper.class);

  private MapReduceJobState getMapReduceJobState(MapReduceJob job, JobClient jobClient) {
    try {
      RunningJob runningJob = jobClient.getJob(JobID.forName(job.getId()));
      if (runningJob == null) {
        log.warn("Couldn't find job status for jobId: " + job.getId());
        return null;
      }
      JobID jobID = runningJob.getID();
      TaskReport[] mapTaskReport = jobClient.getMapTaskReports(jobID);
      TaskReport[] reduceTaskReport = jobClient.getReduceTaskReports(jobID);

      return new MapReduceJobState(runningJob, mapTaskReport, reduceTaskReport);

    } catch (Exception e) {
      log.warn("Couldn't find job status for jobId: " + job.getId());
    }
    return null;
  }

  /**
   * set the mapreduce statistics by querying the jobtracker
   * This method only sets the mapreduce statistics if they are queried successfully
   * @param job Mapreduce job to query mapreduce statistics for
   * @param jobClient 
   */
  public void addMapReduceJobState(MapReduceJob job, JobClient jobClient) {
    MapReduceJobState state = getMapReduceJobState(job, jobClient);
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
      RunningJob runningJob = jobClient.getJob(JobID.forName(job.getId()));
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

  public static Configuration toConfiguration(Properties properties) {
    assert properties != null;
    final Configuration config = new Configuration(false);
    final Enumeration<Object> iter = properties.keys();
    while (iter.hasMoreElements()) {
      final String key = (String) iter.nextElement();
      final String val = properties.getProperty(key);
      config.set(key, val);
    }
    return config;
  }
}
