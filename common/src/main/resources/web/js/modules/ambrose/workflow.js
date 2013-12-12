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
define(['lib/jquery', 'lib/uri', './core', './client', './graph'], function(
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
      this.current = {
        selected: null,
        mouseover: null,
      };
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
      this.trigger('loadingJobs');
      console.log('Loading jobs');

      // define callbacks
      var self = this;
      var handleError = function(textStatus, errorThrown) {
        console.error('Failed to load jobs:', textStatus, errorThrown);
        self.trigger('error.loadJobs', [null, textStatus, errorThrown]);
      };
      var handleSuccess = function(data, textStatus) {
        if (data == null) {
          handleError(textStatus, 'Data is null');
          return;
        }

        // reset job indices
        var jobs = self.jobs = [];
        var jobsByName = self.jobsByName = {};
        var jobsById = self.jobsById = {};

        // initialize job indices
        $.each(data, function(i, node) {
          // retrieve job from node
          var job = node.job;

          // Clean the DAG job data for correct animation dispaly.
          if (job.mapReduceJobState) { job.mapReduceJobState = null; }
          if (job.counterGroupMap) { job.counterGroupMap = null; }
          if (job.configuration) { job.configuration = null; }
          if (job.metrics) { job.metrics = null; }

          jobs.push(job);

          // index job by name
          var name = job.name = node.name;
          if (name in jobsByName) {
            console.error("Multiple jobs found with name '" + name + "':", self);
            return;
          }
          jobsByName[name] = job;

          // index job by id (if defined)
          if (job.id) {
            var id = job.id;
            if (id in jobsById) {
              console.error("Multiple jobs found with id '" + id + "':", self);
              return;
            }
            jobsById[id] = job;
          }
        });

        // initialize parent-child references
        $.each(data, function(i, node) {
          var job = node.job;
          var name = job.name;
          if (!job.parentNames) job.parentNames = [];
          // TODO(Andy Schlaikjer): Rename node.successorNames to childNames
          $.each(job.childNames = node.successorNames, function(i, childName) {
            var child = jobsByName[childName];
            if (!child) {
              console.error("No job with name '" + childName + "' exists", self);
              return;
            }
            var parentNames = child.parentNames || (child.parentNames = []);
            parentNames.push(name);
          });
        });

        // build job graph and sort
        var graph = self.graph = Graph({
          data: jobs,
          getId: function(d) { return d.name; },
          getParentIds: function(d) { return d.parentNames; },
        });
        graph.sort();
        graph.addPseudoNodes();
        jobs = self.jobs = $.map(graph.nodesByTopologicalIndex, function(node, i) {
          var job = node.data;
          job.index = i;
          job.node = node;
          return job;
        });

        self.trigger('jobsLoaded', [jobs, textStatus, null]);
      };

      // initiate request
      return this.client.getJobs(self.id)
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
     * @param maxEvents max number of events to process on each request. Defaults to -1 (no limit).
     * @return this.
     */
    startEventPolling: function(frequency, maxEvents) {
      var self = this;
      if (self.eventPollingIntervalId != null) return;
      if (frequency == null) frequency = 1000;
      if (maxEvents == null) maxEvents = -1;
      console.info('Starting event polling');
      self.clientFailureCount = 0;
      var pollEvents = function() { self.pollEvents(maxEvents); };
      self.eventPollingIntervalId = setInterval(pollEvents, frequency);
      self.trigger('eventPollingStarted');
      // poll once right now to kick things off
      pollEvents();
      return this;
    },

    /**
     * Stops event polling if running.
     *
     * @return this.
     */
    stopEventPolling: function() {
      if (this.eventPollingIntervalId == null) return;
      console.info('Stopping event polling');
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
     * @param maxEvents max number of events to process. Defaults to -1.
     * @return Promise configured with error and success callbacks which update state of this
     * Workflow and trigger events.
     */
    pollEvents: function(maxEvents) {
      if (maxEvents == null) maxEvents = -1;

      // stop polling if all jobs are done
      if (this.isComplete()) {
        console.info('Workflow complete');
        this.stopEventPolling();
        this.trigger('workflowComplete');
        return;
      }

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
          handleError(textStatus, 'data is null');
          return;
        }

        // reset client failure count
        self.clientFailureCount = 0;

        // process events
        var eventCount = 0;
        $.each(data, function(i, event) {
          // validate event data
          var id = event.id;
          var type = event.type;
          var data = event.payload;

          self.trigger('jobPolled', [data]);

          if (!id || !type || !data) {
            console.error('Invalid event data:', self, event);
            return;
          }

          // skip events we've already processed
          if (id <= self.lastEventId) return;

          // don't process more than specified number of events
          if (maxEvents > 0 && eventCount >= maxEvents) return;

          // check for workflow event
          if (type == 'WORKFLOW_PROGRESS') {
            self.setProgress(data.workflowProgress);
            return;
          }

          // collect job data
          var node = data;
          var job = node.job;
          job.name = node.name;

          // retrieve and update job with new data
          job = self.updateJob(job);
          self.jobsById[job.id] = job;

          // process job event
          switch (type) {
          case 'JOB_STARTED':
            console.info('Job started:', job);
            job.status = 'RUNNING';
            break;
          case 'JOB_PROGRESS':
            console.info('Job progress:', job);
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
            console.info('Job complete:', job);
            job.status = 'COMPLETE';
            break;
          case 'JOB_FAILED':
            console.info('Job failed:', job);
            job.status = 'FAILED';
            break;
          default:
            console.error("Unsupported event type '" + type + "':", self, event);
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
      if (this.progress == '100') {
        complete = true;
        var jobs = this.jobs;
        var i = -1, n = jobs.length; while (++i < n) {
          var job = jobs[i];
          var status = job.status;
          if (status != 'COMPLETE' && status != 'FAILED') {
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
      this.trigger('workflowProgress', [progress]);
      return this;
    },

    /**
     * Updates current "mouseover" job. The mouse may be hovering only one job at a time. If you
     * mouseover the same job twice, the second call does nothing. If you mouseover null, any
     * current mouseover job is cleared.
     *
     * @param data object containing either 'name' or 'id' fields with which to find job. If null, clears any current "mouseover" job.
     * @return selected job.
     * @throws Error if no job exists with associated name or id.
     */
    mouseOverJob: function(data) {
      var job = null;
      if (data != null) job = this.findJob(data);
      var prev = this.current.mouseover;
      if (job === prev) return;
      if (prev != null) prev.mouseover = false;
      if (job != null) job.mouseover = true;
      this.current.mouseover = job;

      //console.debug('Job mouse over:', job, prev);
      this.trigger('jobMouseOver', [job, prev]);
      return job;
    },

    /**
     * Updates current "selected" job. Only one job may be selected at a time. If you select the
     * same job twice, the second select will deselect the job. If you select null, any selected job
     * is deselected.
     *
     * @param data object containing either 'name' or 'id' fields with which to find job. If null, clears any current "selected" job.
     * @return selected job.
     * @throws Error if no job exists with associated name or id.
     */
    selectJob: function(data) {
      var job = null;
      if (data != null) job = this.findJob(data);
      var prev = this.current.selected;
      if (prev != null) prev.selected = false;
      if (job === prev) job = null;
      else if (job != null) job.selected = true;
      this.current.selected = job;

      this.trigger('jobSelected', [job, prev]);
      return job;
    },

    /**
     * Reload jobs and poll for events at limited rate.
     */
    replay: function() {
      var self = this;
      self.stopEventPolling();
      self.loadJobs().done(function() {
        self.startEventPolling(1000, 1);
      });
    },

    /**
     * Reload jobs and poll for as many events as possible.
     */
    jumpToEnd: function() {
      var self = this;
      self.stopEventPolling();
      self.loadJobs().done(function() {
        self.startEventPolling(1000, -1);
      });
    },
  };

  // bind prototype to ctor
  Workflow.fn.init.prototype = Workflow.fn;
  return Workflow;
});
