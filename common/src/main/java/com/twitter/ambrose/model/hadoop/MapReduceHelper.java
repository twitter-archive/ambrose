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

import java.io.InputStream;
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

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Helper utilities to get information about a mapreduce job.
 *
 * @author amokashi
 */
public class MapReduceHelper {

  private static final Log LOG = LogFactory.getLog(MapReduceHelper.class);

  private RunningJob getRunningJob(MapReduceJob job, JobClient jobClient) throws Exception {
    RunningJob runningJob = jobClient.getJob(JobID.forName(job.getId()));
    if (runningJob == null) {
      throw new Exception(String.format("Failed to retrieve job with id '%s'", job.getId()));
    }
    return runningJob;
  }

  private MapReduceJobState getMapReduceJobState(MapReduceJob job, JobClient jobClient)
      throws Exception {
    RunningJob runningJob = getRunningJob(job, jobClient);
    JobID jobID = runningJob.getID();
    TaskReport[] mapTaskReport = jobClient.getMapTaskReports(jobID);
    TaskReport[] reduceTaskReport = jobClient.getReduceTaskReports(jobID);
    return new MapReduceJobState(runningJob, mapTaskReport, reduceTaskReport);
  }

  /**
   * Sets the mapreduce statistics by querying the jobtracker. This method only sets the mapreduce
   * statistics if they are queried successfully.
   *
   * @param job job whose state should be retrieved.
   * @param jobClient client with which to retrieve job stats.
   */
  public void addMapReduceJobState(MapReduceJob job, JobClient jobClient) {
    try {
      job.setMapReduceJobState(getMapReduceJobState(job, jobClient));
    } catch (Exception e) {
      LOG.warn("Failed to retrieve job state", e);
    }
  }

  /**
   * Get the configurations at the beginning of the job flow, it will contain information about the
   * map/reduce plan and decoded pig script.
   *
   * @param job job whose configuration should be fetched.
   * @param jobClient client with which to retrieve job configuration.
   */
  public void setJobConfFromFile(MapReduceJob job, JobClient jobClient) {
    try {
      RunningJob runningJob = getRunningJob(job, jobClient);
      String jobFile = runningJob.getJobFile();
      LOG.info(String.format("Loading RunningJob configuration file '%s'", jobFile));
      Path path = new Path(jobFile);
      FileSystem fileSystem = FileSystem.get(new Configuration());
      InputStream inputStream = fileSystem.open(path);
      Configuration conf = new Configuration(false);
      conf.addResource(inputStream);
      job.setConfiguration(toProperties(conf));
    } catch (Exception e) {
      LOG.warn("Error occurred when retrieving configuration info", e);
    }
  }

  /**
   * Converts properties to configuration.
   *
   * @param properties properties to convert.
   * @return configuration containing values from properties.
   */
  public static Configuration toConfiguration(Properties properties) {
    checkNotNull(properties);
    final Configuration config = new Configuration(false);
    for (Object keyObj : properties.keySet()) {
      final String key = (String) keyObj;
      final String val = properties.getProperty(key);
      config.set(key, val);
    }
    return config;
  }

  /**
   * Converts configuration to properties.
   *
   * @param conf configuration to convert.
   * @return properties containing values from conf.
   */
  public static Properties toProperties(Configuration conf) {
    checkNotNull(conf);
    Properties properties = new Properties();
    for (Map.Entry<String, String> entry : conf) {
      properties.put(entry.getKey(), entry.getValue());
    }
    return properties;
  }
}
