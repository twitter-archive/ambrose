/*
Copyright 2013, Lorand Bendig

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
package com.twitter.ambrose.hive;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.RunningJob;

import com.twitter.ambrose.model.hadoop.CounterGroup;

/**
 * Utility for Ambrose-Hive related operations
 *
 * @author Lorand Bendig <lbendig@gmail.com>
 *
 */
public class AmbroseHiveUtil {

  private static final Pattern STAGEID_PATTERN = Pattern.compile("^.*\\((Stage\\-\\d+)\\)$",
      Pattern.DOTALL);

  private AmbroseHiveUtil() {
    throw new AssertionError("shouldn't be instantiated!");
  }

  /**
   * Constructs the jobTracker url based on the jobId.
   *
   * @param jobID
   * @param conf
   * @return
   * @see org.apache.hadoop.hive.hwi#getJobTrackerURL(String)
   */
  public static String getJobTrackerURL(String jobID, HiveConf conf) {
    String jt = conf.get("mapred.job.tracker");
    String jth = conf.get("mapred.job.tracker.http.address");
    String[] jtparts = null;
    String[] jthttpParts = null;
    if (jt.equalsIgnoreCase("local")) {
      jtparts = new String[2];
      jtparts[0] = "local";
      jtparts[1] = "";
    }
    else {
      jtparts = jt.split(":");
    }
    if (jth.contains(":")) {
      jthttpParts = jth.split(":");
    }
    else {
      jthttpParts = new String[2];
      jthttpParts[0] = jth;
      jthttpParts[1] = "";
    }
    return jtparts[0] + ":" + jthttpParts[1] + "/jobdetails.jsp?jobid=" + jobID + "&refresh=30";
  }

  /**
   * Constructs counter groups from job runtime statistics. Hive mangles Hadoop Counter data,
   * forming counter names with format "$groupName::$counterName".
   *
   * @param counterNameToValue mangled hadoop counters from hive.
   * @return counter groups by name.
   */
  public static Map<String, CounterGroup> counterGroupInfoMap(Map<String, Double> counterNameToValue) {
    Counters counters = new Counters();
    for (Map.Entry<String, ? extends Number> entry : counterNameToValue.entrySet()) {
      String key = entry.getKey();
      Number value = entry.getValue();
      String[] cNames = key.split("::");
      String groupName = cNames[0];
      String counterName = cNames[1];
      Counter counter = counters.findCounter(groupName, counterName);
      counter.setValue(value.longValue());
    }
    return CounterGroup.counterGroupsByName(counters);
  }

  public static String asDisplayId(String queryId, String jobIDStr, String nodeId) {
    String stageName = nodeId.substring(0, nodeId.indexOf('_'));
    String wfIdLastPart = queryId.substring(queryId.lastIndexOf('-') + 1, queryId.length());
    String displayJobId = String.format(jobIDStr + " (%s, query-id: ...%s)", stageName,
        wfIdLastPart);
    return displayJobId;
  }

  public static String getNodeIdFromNodeName(Configuration conf, String nodeName) {
    return nodeName + "_" + getHiveQueryId(conf);
  }

  /**
   * Returns the nodeId of the given running job <br>
   * Example: Stage-1_[queryId]
   *
   * @param conf
   * @param runningJob
   * @return
   */
  public static String getNodeIdFromJob(Configuration conf, RunningJob runningJob) {
    return getNodeIdFromJobName(conf, runningJob.getJobName());
  }

  /**
   * Retrieves the nodeId from the Hive SQL command <br>
   *
   * @param conf
   * @param jobName
   * @return
   */
  private static String getNodeIdFromJobName(Configuration conf, String jobName) {
    Matcher matcher = STAGEID_PATTERN.matcher(jobName);
    if (matcher.find()) {
      return getNodeIdFromNodeName(conf, matcher.group(1));
    }
    return null;
  }

  /**
   * Returns the Hive query id which identifies the current workflow <br>
   * Format: hive_[queryId]
   *
   * @param conf
   * @return
   */
  public static String getHiveQueryId(Configuration conf) {
    return HiveConf.getVar(conf, ConfVars.HIVEQUERYID);
  }

  /**
   * Gets the temporary directory of the given job
   *
   * @param conf
   * @param isLocal true to resolve local temporary directory
   * @return
   */
  public static String getJobTmpDir(Configuration conf, boolean isLocal) {
    String fsName = HiveConf.getVar(conf, ConfVars.HADOOPFS);
    if (fsName.endsWith("/")) {
      fsName = fsName.substring(0, fsName.length() - 1);
    }
    return fsName + HiveConf.getVar(conf,
        (isLocal ? ConfVars.LOCALSCRATCHDIR : ConfVars.SCRATCHDIR), "");
  }

  /**
   * Gets (non-accessible) field of a class
   *
   * @param clazz
   * @param fieldName
   * @return
   * @throws Exception
   */
  public static Field getInternalField(Class<?> clazz, String fieldName) throws Exception {
    Field field = clazz.getDeclaredField(fieldName);
    field.setAccessible(true);
    return field;
  }

  /**
   * Compares two float values
   *
   * @param f1
   * @param f2
   * @return true if f1 and f2 are equal
   */
  public static boolean isEqual(float f1, float f2) {
    final float delta = 0.001f;
    return (Math.abs(f1 - f2) < delta) ? true : false;
  }
}
