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

/**
 * This module defines a util class used to get the metrics and counter for a job.
 */
define(['lib/jquery'], function($) {
  // core Ambrose object, util methods
  return {
    getHDFSWrittenFromCounter : function(data) {
      if (data && data.counterGroupMap && data.counterGroupMap.FileSystemCounters
          && data.counterGroupMap.FileSystemCounters.counterInfoMap
          && data.counterGroupMap.FileSystemCounters.counterInfoMap.HDFS_BYTES_WRITTEN
          && data.counterGroupMap.FileSystemCounters.counterInfoMap.HDFS_BYTES_WRITTEN.value) {
        return data.counterGroupMap.FileSystemCounters.counterInfoMap.HDFS_BYTES_WRITTEN.value;
      }
      return null;
    },

    getHDFSWrittenFromMetrics : function(data) {
      if (data && data.metrics && data.metrics.hdfsBytesWritten) {
        return data.metrics.hdfsBytesWritten;
      }
      return null;
    },

    getReduceOutputRecords : function(data) {
      if (data && data.counterGroupMap
          && data.counterGroupMap["org.apache.hadoop.mapred.Task$Counter"]
          && data.counterGroupMap["org.apache.hadoop.mapred.Task$Counter"].counterInfoMap
          && data.counterGroupMap["org.apache.hadoop.mapred.Task$Counter"].counterInfoMap.REDUCE_OUTPUT_RECORDS
          && data.counterGroupMap["org.apache.hadoop.mapred.Task$Counter"].counterInfoMap.REDUCE_OUTPUT_RECORDS.value) {
        return data.counterGroupMap["org.apache.hadoop.mapred.Task$Counter"].counterInfoMap.REDUCE_OUTPUT_RECORDS.value;
      }
      return null;
    },

    getReduceOutputRecordsFromMetrics : function(data) {
      if (data && data.metrics && data.metrics.reduceOutputRecords) {
        return data.metrics.reduceOutputRecords;
      }
      return null;
    },

    getMapInputRecords : function(data) {
      if (data && data.counterGroupMap
          && data.counterGroupMap["org.apache.hadoop.mapred.Task$Counter"]
          && data.counterGroupMap["org.apache.hadoop.mapred.Task$Counter"].counterInfoMap
          && data.counterGroupMap["org.apache.hadoop.mapred.Task$Counter"].counterInfoMap.MAP_INPUT_RECORDS
          && data.counterGroupMap["org.apache.hadoop.mapred.Task$Counter"].counterInfoMap.MAP_INPUT_RECORDS.value) {
        return data.counterGroupMap["org.apache.hadoop.mapred.Task$Counter"].counterInfoMap.MAP_INPUT_RECORDS.value;
      }
      return null;
    },

    getMapInputRecordsFromMetrics : function(data) {
      if (data && data.metrics && data.metrics.mapInputRecords) {
        return data.metrics.mapInputRecords;
      }
      return null;
    },

    getHDFSReadFromCounter : function(data) {
      if (data && data.counterGroupMap && data.counterGroupMap.FileSystemCounters
          && data.counterGroupMap.FileSystemCounters.counterInfoMap
          && data.counterGroupMap.FileSystemCounters.counterInfoMap.HDFS_BYTES_READ
          && data.counterGroupMap.FileSystemCounters.counterInfoMap.HDFS_BYTES_READ.value) {
        return data.counterGroupMap.FileSystemCounters.counterInfoMap.HDFS_BYTES_READ.value;
      }
      return null;
    }
  };
});
