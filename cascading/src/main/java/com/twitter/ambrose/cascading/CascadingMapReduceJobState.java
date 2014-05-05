/*
 *  Copyright 2013 ahmedmohsen.
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  under the License.
 */

package com.twitter.ambrose.cascading;

import java.io.IOException;

import org.apache.hadoop.mapred.TaskReport;

import cascading.stats.hadoop.HadoopStepStats;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.twitter.ambrose.model.hadoop.MapReduceJobState;

/**
 * A wrapper that contains related flow step job statistics
 * @author Ahmed Mohsen
 */
@SuppressWarnings("deprecation")
public class CascadingMapReduceJobState extends MapReduceJobState {

  @JsonCreator
  public CascadingMapReduceJobState() { }

  public CascadingMapReduceJobState(HadoopStepStats stats, TaskReport[] mapTaskReport,
    TaskReport[] reduceTaskReport) throws IOException{
    super(stats.getRunningJob(), mapTaskReport, reduceTaskReport);

    //overwrite default values
    //for some reseaon mapTaskReport and reduceTaskReport didn't work for me
    setJobId(stats.getJobID());
    setJobName(stats.getName());
    setTrackingURL(stats.getStatusURL());
    setComplete(stats.isFinished());
    setSuccessful(stats.isSuccessful());
    setMapProgress(stats.getMapProgress());
    setReduceProgress(stats.getReduceProgress());

    setTotalMappers(stats.getNumMapTasks());
    setTotalReducers(stats.getNumReduceTasks());

    setJobStartTime(stats.getStartTime());
    setJobLastUpdateTime(stats.getFinishedTime());

    // If not all the reducers are finished.
    if ( getJobLastUpdateTime() == 0) {
      setJobLastUpdateTime(System.currentTimeMillis());
    }
  }
}
