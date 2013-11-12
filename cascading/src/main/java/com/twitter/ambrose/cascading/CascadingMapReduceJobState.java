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

import cascading.stats.hadoop.HadoopStepStats;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.twitter.ambrose.model.hadoop.MapReduceJobState;
import com.twitter.ambrose.util.JSONUtil;
import java.io.IOException;
import org.apache.hadoop.mapred.TaskReport;

/**
 * A wrapper that contains related flow step job statistics
 * @author Ahmed Mohsen
 */
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

  /**
   * This is a hack to get around how the json library requires subtype info to
   * be defined on the super-class, which doesn't always have access to the
   * subclasses at compile time. Since the mixinAnnotations method replaces the
   * existing annotation, this means that an action like this will need to be
   * taken once upon app startup to register all known types. If this action
   * happens multiple times, calls will override each other.
   *
   * @see com.twitter.ambrose.pig.HiveJob#mixinJsonAnnotations()
   */
  public static void mixinJsonAnnotations() {

    JSONUtil.mixinAnnotatons(MapReduceJobState.class, AnnotationMixinClass.class);
  }

  @JsonSubTypes({
      @JsonSubTypes.Type(value = com.twitter.ambrose.model.hadoop.MapReduceJobState.class, name = "default"),
      @JsonSubTypes.Type(value = com.twitter.ambrose.cascading.CascadingMapReduceJobState.class, name = "cascading") })
  private static class AnnotationMixinClass {}
}
