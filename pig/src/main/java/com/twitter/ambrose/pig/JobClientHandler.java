/*
Copyright 2012 Twitter, Inc.

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
package com.twitter.ambrose.pig;

import org.apache.hadoop.mapred.JobClient;
import org.apache.pig.tools.pigstats.PigStats;

/**
 * This interface is used to share the JobClient and JobGraph between different class and different
 * threads.
 *
 * @author gzhang
 */
@Deprecated
public interface JobClientHandler {
  /**
   * Grab the JobClient from the current thread and sets it to be used in another thread.
   * @param jobClient
   */
  public void setJobClient(JobClient jobClient);

  /**
   * Grab the JobGraph from the current thread and sets it to be used in another thread.
   * @param jobGraph
   */
  public void setJobGraph(PigStats.JobGraph jobGraph);
}
