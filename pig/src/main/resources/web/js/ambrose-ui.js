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
var AMBROSE = window.AMBROSE || {};

/**
 * The main Ambrose UI. Other view elements can bind to the events that this object triggers, which
 * are listed at the bottom of this module.
 */
AMBROSE.ui = function(options) {
  // storage for job data and lookup
  var ui;
  var url = window.location.href;

  var jobs = [];
  var selectedJob;
  var jobsByName = {}, jobsByJobId = {}, indexByName = {}, nameByIndex = {};
  var scriptProgress = 0;

  var loadDagIntervalId, pollIntervalId;
  var MAX_NULL_EVENTS = 10;
  var consecutiveNullEvents = 0;
  var lastProcessedEventId = -1;

  // handle demo data
  if (url.indexOf('?localdata=small') != -1) {
    dagURL = "data/small-dag.json";
    eventsURL = "data/small-events.json";
  } else if (url.indexOf('?localdata=large') != -1) {
    dagURL = "data/large-dag.json";
    eventsURL = "data/large-events.json";
  } else {
    dagURL = "dag";
    eventsURL = "events";
  }

  function buildJobIndex(jobs) {
    // Compute a unique index for each job name
    n = 0;
    jobs.forEach(function(job) {
      jobsByName[job.name] = job;
      if (!(job.name in indexByName)) {
        nameByIndex[n] = job.name;
        indexByName[job.name] = job.index = n++;
      }
    });
  }

  /**
   * Looks up job with data.name or data.jobId and updates contents of job with
   * fields from data.
   */
  function updateJobData(data) {
    // get job associated with data
    var job, id;
    if (data.name != null) {
      id = data.name;
      job = jobsByName[id];
    } else {
      id = data.jobId;
      job = jobsByJobId[id];
    }

    // check for job retrieval failure
    if (job == null) {
      alert("Job with id '" + id + "' not found");
      return;
    }

    // copy data into job
    $.each(data, function(key, value) {
      job[key] = value;
    });
    return job
  }

  // private members and methods above, public below
  return {

    // Retrieves snapshot of current DAG of jobs from the server
    loadDag: function() {
      // load dag data and initialize
      d3.json(dagURL, function(data) {
        if (data == null) {
          alert("Failed to load dag data");
          return;
        }
        jobs = data;

        buildJobIndex(jobs);

        $(ui).trigger( "dagLoaded", {"jobs": jobs} );
        $(ui).trigger( "jobSelected", {"job": jobs[0], "jobs": jobs} );
        clearTimeout(loadDagIntervalId);
        ui.startEventPolling();
      });
    },

    startEventPolling: function() {
      pollIntervalId = setInterval('ui.pollEvents()', 1000);
      return pollIntervalId;
    },

    stopEventPolling: function() {
      clearInterval(pollIntervalId);
      return pollIntervalId;
    },

    // select a job
    selectJob: function(job) {
      selectedJob = job;
      $(ui).trigger( "jobSelected", {"job": job} );
    },

    // get the selected job
    selectedJob: function() {
      return selectedJob;
    },

    // get the selected job
    totalJobs: function() {
      return jobs.length;
    },

    // is job selected?
    isSelected: function(job) {
      return job === selectedJob;
    },

    // display an error
    error: function(msg) {
      d3.select('#scriptStatusDialog').text(msg);
    },

    // display info
    info: function(msg) {
      d3.select('#scriptStatusDialog').text(msg);
    },

    /**
     * Poll the server for new events
     */
    pollEvents: function() {
      // are we there yet?
      var scriptDone = false;
      if (scriptProgress == 100) {
        scriptDone = true;
        for (var i = 0; i < jobs.length; i++) {
          var job = jobs[i];
          if(job.status != "COMPLETE" && job.status != "FAILED") {
            scriptDone = false;
            break;
          }
        }
      }

      // stop polling for events if all jobs are done
      if (scriptDone) {
        ui.info("script finished");
        ui.stopEventPolling();
        return;
      }

      d3.json(eventsURL + "?lastEventId=" + lastProcessedEventId, function(events) {
        // test for error
        if (events == null) {
          consecutiveNullEvents = consecutiveNullEvents + 1;
          if (consecutiveNullEvents >= MAX_NULL_EVENTS) {
              ui.stopEventPolling();
              ui.error(MAX_NULL_EVENTS + " consecutive requests for events have failed. Stopping event polling.");
          }
          else {
              ui.error("No events found")
          }
          return
        }
        consecutiveNullEvents = 0;
        var eventsHandledCount = 0;
        events.forEach(function(event) {
            var eventId = event.eventId;
            if (eventId <= lastProcessedEventId || eventsHandledCount > 0) {
                return;
            }

            if (!event.eventData || !event.eventType) {
              ui.error("Invalid event data returned from the server: " + event);
              return;
            }

            var data = {"event": event};
            if (event.eventType.indexOf('JOB_') == 0) {
              // job complete and job failed return a jobData object
              if (event.eventData.jobId == null && event.eventData.jobData.jobId != null) {
                event.eventData.jobId = event.eventData.jobData.jobId;
              }
              data["job"] = updateJobData(event.eventData);
            }

            console.log('trigger', event.eventType);
            $(ui).trigger( event.eventType, data);
            lastProcessedEventId = eventId;
            eventsHandledCount++;
        });
      });
    },

    // initilizes the UI by fetching and rendering the dag, then polling for new events.
    init: function() {
      ui = this;
      // these are the events the ui supports that can be bound to as follows
      $(this).bind( "dagLoaded", function(event, data) { });   // data: { "jobs": jobs }
      $(this).bind( "jobUpdated", function(event, data) { });  // data: { "job": job, "jobs": jobs }
      $(this).bind( "jobSelected", function(event, data) { }); // data: { "job": job }

      // these are the events that are returned from the server that can be bounded to as follows
      // data: { "event": event}
      $(this).bind( "WORKFLOW_PROGRESS", function(event, data) {
        scriptProgress = data.event.eventData.scriptProgress;
        ui.info('script progress: ' + scriptProgress + '%');
        $('#progressbar div').width(scriptProgress + '%')
      });

      // data: { "event": event, "job": job}
      $(this).bind( "JOB_STARTED", function(event, data) {
        var job = data.job;
        if (job == null) return;
        ui.info(job.jobId + ' started');
        job.jobId = data.event.eventData.jobId;
        job.status = "RUNNING";
        jobsByJobId[job.jobId] = job;
        ui.selectJob(job);
      });

      $(this).bind( "JOB_PROGRESS", function(event, data) {
        var job = data.job;
        if (job == null) return;
        if (job.isComplete == "true") {
          if (job.isSuccessful == "true") {
            job.status = "COMPLETE";
          } else {
            job.status = "FAILED";
          }
        }
        jobsByJobId[job.jobId] = job;
      });

      $(this).bind( "JOB_FINISHED", function(event, data) {
        var job = data.job;
        if (job == null) return;
        ui.info(job.jobId + ' complete');
        job.status = "COMPLETE";

    //    TODO: Andy, what's this for?
    //    var i = job.index + 1;
    //    if (i < jobs.length) {
    //      $(ui).selectJob(jobs[i]);
    //    }
      });

      $(this).bind( "JOB_FAILED", function(event, data) {
        var job = data.job;
        if (job == null) return;
        ui.info(job.jobId + ' failed');
        job.status = "FAILED";
      });
      loadDagTimeoutId = setTimeout('ui.loadDag()', 500);
    }
  }
}

// "static" helper utilities
AMBROSE.util = function() {
  return {
    comma_join: function(array) {
      if  (array != null) {
        return array.join(", ");
      }
      return '';
    },
    task_progress_string: function(totalTasks, taskProgress) {
      if (totalTasks == null || taskProgress == null) {
        return ''
      }
      return totalTasks + ' (' + d3.round(taskProgress * 100, 0) + '%)'
    },
    value: function(value) {
      if (value == null) {
        return '';
      }
      return value;
    }
  };
}();
