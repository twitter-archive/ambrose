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
 * This module defines the Workflow class, instances of which use a Client to retrieve jobs and
 * events from an Ambrose server. The Workflow acts as a controller and owner of job
 * state. Callbacks may be bound to events triggered on the Workflow to react to state changes.
 */
define(['jquery', 'uri', './core', './client', './graph'], function(
  $, URI, Ambrose, Client, Graph
) {
  // Maximum number of consecutive client failures before event polling is stopped.
  var MAX_CLIENT_FAILURES = 10;

  // return data.job, throwing error if it's undefined
  function getJob(data) {
    var job = data.job;
    if (job == null) throw new Error('Data contains no job entry');
    return job;
  }

  // Workflow ctor
  var Workflow = Ambrose.Workflow = function(id, client) {
    return new Ambrose.Workflow.fn.init(id, client);
  };

  /**
   * Workflow prototype.
   */
  Workflow.fn = Workflow.prototype = {
    /**
     * Constructs a new Workflow.
     *
     * @param id unique id of this workflow. If null, defaults to value of current url's
     * 'workflowId' parameter.
     * @param client client to use when requesting Workflow state from server. If null, defaults to
     * new client instance constructed with default arguments.
     */
    init: function(id, client) {
      if (id == null) {
        var uri = new URI(window.location.href);
        var params = uri.search(true);
        if (params) id = params.workflowId;
      }
      if (client == null) client = Client();
      this.$this = $(this);
      this.id = id;
      this.client = client;
      this.progress = 0;
      this.jobs = [];
      this.jobsByName = {};
      this.jobsById = {};
      this.lastEventId = -1;
      this.selectedJob = null;
    },

    /**
     * Triggers an event on this Workflow. Any event handlers bound to this Workflow for the given
     * event will be invoked.
     *
     * @param event name of event.
     * @param data array of extra arguments, if any, to pass to handler functions.
     * @return this.
     */
    trigger: function(event, data) {
      this.$this.trigger(event, data);
      return this;
    },

    /**
     * Binds callback to the given Workflow event.
     *
     * @param event name of event.
     * @param callback the callback to invoke when the Workflow event is triggered.
     * @return this.
     */
    on: function(event, callback) {
      this.$this.on(event, callback);
      return this;
    },

    /**
     * Initiates asynchronous request for Workflow jobs from server. On error the 'error.loadJobs'
     * event is triggered.
     *
     * @return Promise configured with error and success callbacks which update state of this
     * Workflow.
     */
    loadJobs: function() {
      console.log('Loading jobs:', this);

      // define callbacks
      var self = this;
      var handleError = function(textStatus, errorThrown) {
        console.error('Failed to load jobs:', self, textStatus, errorThrown);
        self.trigger('error.loadJobs', [null, textStatus, errorThrown]);
      };
      var handleSuccess = function(data, textStatus) {
        if (data == null) {
          handleError(textStatus, 'Data is null');
          return;
        }

        // reset job indices
        var jobs = self.jobs = data;
        var jobsByName = self.jobsByName = {};
        var jobsById = self.jobsById = {};

        // initialize job indices
        $.each(jobs, function(i, job) {
          // index job by name
          var name = job.name;
          if (name in jobsByName) {
            console.error("Multiple jobs found with name '" + name + "':", self);
            return;
          }
          jobsByName[name] = job;

          // index job by id (if defined)
          if (job.jobId) {
            var id = job.id = job.jobId;
            delete job.jobId;
            if (id in jobsById) {
              console.error("Multiple jobs found with id '" + id + "':", self);
              return;
            }
            jobsById[id] = job;
          }
        });

        // initialize parent links
        $.each(jobs, function(i, job) {
          var name = job.name;
          $.each(job.successorNames, function(i, childName) {
            var child = jobsByName[childName];
            var predecessorNames = child.predecessorNames || (child.predecessorNames = []);
            predecessorNames.push(name);
          });
        });

        // build job graph and sort
        var jobGraph = self.jobGraph = Graph({
          data: jobs,
          getId: function(d) { return d.name; },
          getParentIds: function(d) { return d.predecessorNames; },
        });
        jobGraph.sort();
        jobs = self.jobs = $.map(jobGraph.nodesByTopologicalIndex, function(n) { return n.data; });

        self.trigger('jobsLoaded', [jobs, textStatus, null]);
      };

      // initiate request
      return this.client.getJobs()
        .error(function(jqXHR, textStatus, errorThrown) {
          handleError(textStatus, errorThrown);
        })
        .success(function(data, textStatus, jqXHR) {
          handleSuccess(data, textStatus);
        });
    },

    /**
     * Starts event polling if not already started.
     *
     * @param frequency poll events at this frequency (ms). Defaults to 1000.
     * @param maxEvents max number of events to process on each request. Defaults to 1.
     * @return this.
     */
    startEventPolling: function(frequency, maxEvents) {
      if (this.eventPollingIntervalId != null) return;
      if (frequency == null) frequency = 1000;
      if (maxEvents == null) maxEvents = 1;
      console.info('Starting event polling:', this);
      this.clientFailureCount = 0;
      var self = this;
      this.eventPollingIntervalId = setInterval(function() {
        self.pollEvents(maxEvents);
      }, frequency);
      this.trigger('eventPollingStarted');
      return this;
    },

    /**
     * Stops event polling if running.
     *
     * @return this.
     */
    stopEventPolling: function() {
      if (this.eventPollingIntervalId == null) return;
      console.info('Stopping event polling:', this);
      clearInterval(this.eventPollingIntervalId);
      this.eventPollingIntervalId = null;
      this.trigger('eventPollingStopped');
      return this;
    },

    /**
     * Initiates asynchronous request for new Workflow events from server. First, tests Workflow
     * completion status. If complete, event polling is stopped and 'workflowComplete' event is
     * triggered. Otherwise, events from last event id are requested. On request failure,
     * 'error.pollEvents' event is triggered. On success, one or more Workflow events are processed
     * sequentially, triggering events in set {'workflowProgress', 'jobStarted', 'jobProgress',
     * 'jobComplete', 'jobFailed'}.
     *
     * @param maxEvents max number of events to process. Defaults to 1.
     * @return Promise configured with error and success callbacks which update state of this
     * Workflow and trigger events.
     */
    pollEvents: function(maxEvents) {
      // stop polling if all jobs are done
      if (this.isComplete()) {
        console.info('Workflow complete:', this);
        this.stopEventPolling();
        this.trigger('workflowComplete');
        return;
      }

      // TODO Integer.MAX_VALUE
      if (maxEvents == null) maxEvents = 999999;

      // error handler
      var self = this;
      var handleError = function(textStatus, errorThrown) {
        console.error('Failed to poll events:', self, textStatus, errorThrown);
        if (++self.clientFailureCount > MAX_CLIENT_FAILURES)
          self.stopEventPolling();
        self.trigger('error.pollEvents', [null, textStatus, errorThrown]);
      };

      // success handler
      var handleSuccess = function(data, textStatus) {
        if (data == null) {
          handleError(textStatus, 'Data is null');
          return;
        }

        // reset client failure count
        self.clientFailureCount = 0;

        // process events
        var eventCount = 0;
        $.each(data, function(i, event) {
          // validate event data
          var id = event.eventId;
          var type = event.eventType;
          var job = event.eventData;
          if (!id || !type || !job) {
            console.error('Invalid event data:', self, event);
            return;
          }

          // skip events we've already processed
          if (id <= self.lastEventId) return;

          // don't process more than specified number of events
          if (eventCount > maxEvents) return;

          // check for workflow event
          if (type == 'WORKFLOW_PROGRESS') {
            self.setProgress(event.eventData.workflowProgress);
            return;
          }

          // collect job data; JOB_FINISHED and JOB_FAILED events contain job.jobData
          if (job.jobId == null && job.jobData != null && job.jobData.jobId != null) {
            var jobData = job.jobData;
            delete job.jobData;
            $.extend(true, job, jobData);
          }
          job.id = job.jobId;
          delete job.jobId;

          // retrieve and update job with new data
          job = self.updateJob(job);
          self.jobsById[job.id] = job;

          // process job event
          switch (type) {
          case 'JOB_STARTED':
            console.info('Job started:', self, job);
            job.status = 'RUNNING';
            break;
          case 'JOB_PROGRESS':
            console.info('Job progress:', self, job);
            if (job.isComplete == 'true') {
              if (job.isSuccessful == 'true') {
                job.status = 'COMPLETE';
              } else {
                job.status = 'FAILED';
              }
            }
            break;
          case 'JOB_FINISHED':
            // TODO(Andy Schlaikjer): rename JOB_FINISHED to JOB_COMPLETE in server
            type = 'JOB_COMPLETE';
            console.info('Job complete:', self, job);
            job.status = 'COMPLETE';
            break;
          case 'JOB_FAILED':
            console.info('Job failed:', self, job);
            job.status = 'FAILED';
            break;
          default:
            console.error("Unsupported event type '" + type + "':", self);
            return;
          }

          // update state and trigger event
          eventCount++;
          self.lastEventId = id;
          self.trigger(type.toLowerCase().camelCase(), [job, event]);
        });

        // update state and trigger event
        self.trigger('eventsPolled', [data, textStatus, null]);
      };

      // initiate request
      return this.client.getEvents(this.id, this.lastEventId)
        .error(function(jqXHR, textStatus, errorThrown) {
          handleError(textStatus, errorThrown);
        })
        .success(function(data, textStatus, jqXHR) {
          handleSuccess(data, textStatus);
        });
    },

    /**
     * @return true if this Workflow is complete, false otherwise.
     */
    isComplete: function() {
      var complete = false;
      if (this.progress == 100) {
        complete = true;
        for (var i = 0; i < this.jobs.length; i++) {
          var job = this.jobs[i];
          if (job.status == 'COMPLETE' || job.status == 'FAILED') {
            complete = false;
            break;
          }
        }
      }
      return complete;
    },

    /**
     * Updates existing job whose id matches that defined by data.id, then updates contents of
     * job to include all of data's fields. Any existing job fields are overwritten.
     *
     * @param data object containing job id and other fields.
     * @return the updated job object.
     * @throws Error if no existing job with matching id exists.
     */
    updateJob: function(data) {
      return $.extend(this.findJob(data), data);
    },

    /**
     * Retrieves existing job object given data containing job name or id fields.
     *
     * @param data object containing name and/or id fields used to uniquely identify the job to
     * retrieve.
     * @return job object.
     * @throws Error if data is null or contains neither name or id fields, or no job with
     * matching name or id exists.
     */
    findJob: function(data) {
      var field, ref, job;
      if (data.name != null) {
        field = 'name';
        ref = data.name;
        job = this.jobsByName[ref];
      } else if (data.id != null) {
        field = 'id';
        ref = data.id;
        job = this.jobsById[ref];
      } else {
        throw new Error('Data contains neither name nor id fields');
      }
      if (job == null) throw new Error("Job with " + field + " '" + ref + "' not found");
      return job;
    },

    /**
     * Sets this Workflow's percent completion and triggers 'progress' event.
     *
     * @param progress int in rage [0, 100].
     * @return this.
     */
    setProgress: function(progress) {
      this.progress = progress;
      this.trigger('workflowProgress');
      return this;
    },

    /**
     * Selects the given job and triggers 'jobSelected' event.
     *
     * @param data object containing either 'name' or 'id' fields with which to find job.
     * @return selected job.
     * @throws Error if no job exists with associated name or id.
     */
    selectJob: function(data) {
      var job = this.findJob(data);
      job.selected = true;
      if (this.selectedJob) this.selectedJob.selected = false;
      this.selectedJob = job;
      this.trigger('jobSelected', [job]);
      return job;
    },
  };

  // bind prototype to ctor
  Workflow.fn.init.prototype = Workflow.fn;
  return Workflow;
});
