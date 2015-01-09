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
define(['lib/jquery', 'ambrose/PathExpression'], function($, PathExpression) {

  function path(s) {
    return PathExpression(s, { elementSeparator: '/' });
  }

  function counter(p, s) {
    return path('counterGroupMap/' + p + '/counterInfoMap/' + s + '/value');
  }

  function jobCounter(s) {
    return counter('(org.apache.hadoop.mapreduce.JobCounter|org.apache.hadoop.mapred.JobInProgress$Counter)', s);
  }

  function taskCounter(s) {
    return counter('(org.apache.hadoop.mapreduce.TaskCounter|org.apache.hadoop.mapred.Task$Counter)', s);
  }

  function fileSystemCounter(s) {
    return counter('(org.apache.hadoop.mapreduce.FileSystemCounter|FileSystemCounters)', s);
  }

  var mapInputRecordsCounter = taskCounter('MAP_INPUT_RECORDS');
  var reduceOutputRecordsCounter = taskCounter('REDUCE_OUTPUT_RECORDS');
  var fileBytesReadCounter = fileSystemCounter('FILE_BYTES_READ');
  var fileBytesWrittenCounter = fileSystemCounter('FILE_BYTES_WRITTEN');
  var hdfsBytesReadCounter = fileSystemCounter('HDFS_BYTES_READ');
  var hdfsBytesWrittenCounter = fileSystemCounter('HDFS_BYTES_WRITTEN');

  // JobData module
  return {
    getFileBytesRead: function(data) {
      return fileBytesReadCounter.value(data);
    },

    getFileBytesWritten: function(data) {
      return fileBytesWrittenCounter.value(data);
    },

    getHdfsBytesRead: function(data) {
      return hdfsBytesReadCounter.value(data);
    },

    getHdfsBytesWritten: function(data) {
      return hdfsBytesWrittenCounter.value(data);
    },

    getMapInputRecords: function(data) {
      return mapInputRecordsCounter.value(data);
    },

    getReduceOutputRecords: function(data) {
      return reduceOutputRecordsCounter.value(data);
    },
  };

});
