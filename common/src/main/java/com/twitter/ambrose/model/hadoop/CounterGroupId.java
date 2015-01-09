package com.twitter.ambrose.model.hadoop;

import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * Common Hadoop counter group identifiers. Counter group names have changed from one version of
 * Hadoop to the next. We include multiple group names to reflect all historical names, with most
 * recent name first.
 *
 * @see <a href="https://issues.apache.org/jira/browse/HADOOP-5717">HADOOP-5717</a>
 */
public enum CounterGroupId {
  JOB(
      "org.apache.hadoop.mapreduce.JobCounter",
      "org.apache.hadoop.mapred.JobInProgress$Counter"
  ), TASK(
      "org.apache.hadoop.mapreduce.TaskCounter",
      "org.apache.hadoop.mapred.Task$Counter"
  ), FILE_SYSTEM(
      "org.apache.hadoop.mapreduce.FileSystemCounter",
      "FileSystemCounters"
  );

  public final List<String> groupNames;

  CounterGroupId(String... groupNames) {
    this.groupNames = ImmutableList.copyOf(groupNames);
  }
}
