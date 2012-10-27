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
 * Ambrose module "ui" containing core event handling. Other view elements can
 * bind to the events this object triggers, which are listed at the bottom of
 * this module.
 */
define(['jquery', 'ambrose', 'ambrose-graph', 'd3'], function($, ambrose, graph) {
  var ui = ambrose.ui = function() {
    return new ambrose.ui.fn.init();
  };

  var _workflowId, _clusterName, _workflowName, _userName;
  var _dagUrl, _eventsUrl;
  var _workflowProgress = 0;
  var _jobGraph, _jobs, _jobsByName, _jobsByJobId, _indexByName, _nameByIndex;
  var _selectedJob;
  var _pollIntervalId;
  var _lastProcessedEventId = -1;
  var _consecutiveNullEvents = 0;
  var MAX_NULL_EVENTS = 10;

  var _statusDialog = $('#scriptStatusDialog');
  var _progressBar = $('#progressbar div');

  function _info(msg) {
    _statusDialog.text(msg);
  };

  function _error(msg) {
    _statusDialog.text(msg);
  };

  function _fatal(msg) {
    alert(msg);
  };

  /**
   * Requests job graph from "dag" end-point. On success, job data is parsed,
   * state is updated, the 'dagLoaded' event is triggered, the first job is
   * selected, and event polling is initiated.
   */
  function _loadDag() {
    var ui = this;
    _info('loading job graph');
    d3.json(_dagUrl.unicode(), function(data) {
      // handle failure
      if (data == null) {
        ui.fatal('Failed to load dag data');
        return;
      }

      // save jobs data and build indices
      _initializeJobs(data);

      // trigger event
      ui.trigger('dagLoaded', {jobs: _jobs});

      // select first chart tab
      $('#vizGroup > div:first-child').addClass('active');

      // display chart tabs if more than one
      if ($('#vizTabs > li').length > 1) {
        $('#vizTabs').show();
      } else {
        $('#vizTabs').hide();
      }

      // select first job
      if (_jobs.length > 0) {
        ui.selectJob(_jobs[0]);
      }

      // begin polling server for events
      _startEventPolling.call(ui);
    });
  };

  /**
   * Resets internal jobs data and rebuilds job indices. Job id to job map is
   * reset. Note that job ids and names are independent; A map-reduce job
   * receives an id only after it has been successfully submitted to a Job
   * Tracker for processing.
   */
  function _initializeJobs(jobs) {
    // initialize name to job map
    _jobsByName = {};
    $.each(jobs, function(i, job) {
      var name = job.name;
      if (name in _jobsByName) {
        console.error("Multiple jobs found with name '" + name + "'")
        return;
      }
      _jobsByName[name] = job;
    });

    // initialize parent links
    $.each(jobs, function(j, job) {
      var name = job.name;
      $.each(job.successorNames, function(i, childName) {
        var child = _jobsByName[childName];
        var predecessorNames = child.predecessorNames || (child.predecessorNames = []);
        predecessorNames.push(name);
      });
    });

    // build graph and sort
    _jobGraph = graph({
      data: jobs,
      getId: function(d) { return d.name; },
      getParentIds: function(d) { return d.predecessorNames; },
    });
    _jobGraph.sort();

    // initialize remaining indices
    _jobs = [];
    _jobsByJobId = {};
    _indexByName = {};
    _nameByIndex = {};
    $.each(_jobGraph.nodesByTopologicalIndex, function(i, node) {
      var job = node.data;
      var name = job.name;
      _indexByName[name] = job.index = i;
      _nameByIndex[i] = name;
      _jobs.push(job);
    });
  }

  function _startEventPolling() {
    // TODO: this next line breaks things when restarting from firebug
    ui = this;
    _pollIntervalId = setInterval(function() { _pollEvents.call(ui); }, 1000);
    _info('event polling started');
  }

  function _stopEventPolling() {
    clearInterval(_pollIntervalId);
    _info('event polling stopped');
  }

  function _pollEvents() {
    var ui = this;

    // are we there yet?
    var scriptDone = false;
    if (_workflowProgress == 100) {
      scriptDone = true;
      for (var i = 0; i < _jobs.length; i++) {
        var job = _jobs[i];
        if(job.status != 'COMPLETE' && job.status != 'FAILED') {
          scriptDone = false;
          break;
        }
      }
    }

    // stop polling for events if all jobs are done
    if (scriptDone) {
      ui.info('script finished');
      _stopEventPolling.call(ui);
      return;
    }

    // request new events from end-point
    var uri = _eventsUrl
      .removeSearch([ 'lastEventId' ])
      .addSearch({ lastEventId: _lastProcessedEventId })
      .unicode();
    d3.json(uri, function(events) {
      // test for error
      if (events == null) {
        _consecutiveNullEvents++;
        if (_consecutiveNullEvents >= MAX_NULL_EVENTS) {
          _stopEventPolling.call(ui);
          ui.error(MAX_NULL_EVENTS + ' consecutive event requests have failed. Stopping event polling.');
        } else {
          ui.error('No events found');
        }
        return;
      }

      // reset state
      _consecutiveNullEvents = 0;
      var eventsHandledCount = 0;

      // process next event
      // TODO: Allow client to process multiple events here
      events.forEach(function(event) {
        // validate event data
        var id = event.eventId;
        var type = event.eventType;
        var job = event.eventData;
        if (!id || !type || !job) {
          ui.error('Invalid event data returned from the server: ' + event);
          return;
        }

        // skip events we've already processed
        if (id <= _lastProcessedEventId || eventsHandledCount > 0) {
          return;
        }

        // collect event data
        if (type.indexOf('JOB_') == 0) {
          // JOB_FINISHED and JOB_FAILED events contain job.jobData
          if (job.jobId == null && job.jobData != null && job.jobData.jobId != null) {
            var jobData = job.jobData;
            delete job.jobData;
            $.extend(true, job, jobData);
          }
          job = _updateJob(job);
        }

        // trigger event and update state
        ui.trigger(type, {event: event, job: job});
        _lastProcessedEventId = id;
        eventsHandledCount++;
      });
    });
  }

  /**
   * Finds existing job whose id matches that included in data, then updates
   * contents of job to include all of data's fields. Any existing job fields
   * are overwritten.
   */
  function _updateJob(data) {
    var job = _findJob(data);
    return $.extend(job, data);
  }

  /**
   * Retrieves existing job object via data.name or data.jobId, throwing error
   * if data contains no job reference, or no job object is associated with the
   * given reference.
   */
  function _findJob(data) {
    var field, ref, job;
    if (data.name != null) {
      field = 'name';
      ref = data.name;
      job = _jobsByName[ref];
    } else if (data.jobId != null) {
      field = 'id';
      ref = data.jobId;
      job = _jobsByJobId[ref];
    } else {
      throw new Error("Data contains no job name nor id");
    }
    if (job == null) throw new Error("Job with " + field + " '" + id + "' not found");
    return job;
  }

  /**
   * Retrieves data.job, throwing error if it's undefined.
   */
  function _getJob(data) {
    var job = data.job;
    if (job == null) throw new Error("Data contains no job entry");
    return job;
  }

  function _handleWorkflowProgress(event, data) {
    _workflowProgress = data.event.eventData.workflowProgress;
    this.info('script progress: ' + _workflowProgress + '%');
    _progressBar.width(_workflowProgress + '%');
  }

  function _handleJobStarted(event, data) {
    var job = _getJob(data);
    this.info(job.jobId + ' started');
    job.jobId = data.event.eventData.jobId;
    job.status = 'RUNNING';
    _jobsByJobId[job.jobId] = job;
    this.selectJob(job);
  }

  function _handleJobProgress(event, data) {
    var job = _getJob(data);
    if (job.isComplete == 'true') {
      if (job.isSuccessful == 'true') {
        job.status = 'COMPLETE';
      } else {
        job.status = 'FAILED';
      }
    }
    _jobsByJobId[job.jobId] = job;
  }

  function _handleJobFinished(event, data) {
    var job = _getJob(data);
    this.info(job.jobId + ' complete');
    job.status = 'COMPLETE';
  }

  function _handleJobFailed(event, data) {
    var job = _getJob(data);
    this.info(job.jobId + ' failed');
    job.status = 'FAILED';
  }

  /**
   * Define AMBROSE.ui.prototype.
   */
  ui.fn = ui.prototype = {
    /**
     * Default constructor.
     */
    init: function() {
      // initialize end-point urls
      var uri = new URI(window.location.href);
      var params = uri.search(true);

      if (params.localdata) {
        if (params.localdata == 'small') {
          _dagUrl = 'data/small-dag.json';
          _eventsUrl = 'data/small-events.json';
        } else {
          _dagUrl = 'data/large-dag.json';
          _eventsUrl = 'data/large-events.json';
        }
      } else {
        _dagUrl = 'dag';
        _eventsUrl = 'events';
      }

      _dagUrl = new URI(_dagUrl);
      _eventsUrl = new URI(_eventsUrl);

      _workflowId = params.workflowId;
      if (_workflowId) {
        _dagUrl.search({ workflowId: _workflowId });
        _eventsUrl.search({ workflowId: _workflowId });
      }

      // wrap "this" with jQuery for event handling
      this.controller = $(this);

      // bind event handlers
      this.bind('WORKFLOW_PROGRESS', _handleWorkflowProgress);
      this.bind('JOB_STARTED', _handleJobStarted);
      this.bind('JOB_PROGRESS', _handleJobProgress);
      this.bind('JOB_FINISHED', _handleJobFinished);
      this.bind('JOB_FAILED', _handleJobFailed);
    },

    /**
     * Schedules retrieval of job graph from back-end.
     */
    load: function() {
      var ui = this;
      $(document).ready(function() {
        setTimeout(function() {
          // retrieve job graph
          _loadDag.call(ui);
        }, 500);
      });
    },

    bind: function(event, callback) { this.controller.bind(event, callback); },
    trigger: function(event, data) { this.controller.trigger(event, data); },

    info: function(msg) { _info(msg); },
    error: function(msg) { _error(msg); },
    fatal: function(msg) { _fatal(msg); },

    startPolling: function() { _startEventPolling(); },
    stopPolling: function() { _stopEventPolling(); },
    findJobByName: function(name) { return _jobsByName[name]; },
    findJobById: function(jobId) { return _jobsByJobId[jobId]; },
    findJobByIndex: function(index) { return _jobsByName[_nameByIndex[index]]; },
    findJobNameByIndex: function(index) { return _nameByIndex[index]; },
    findJobIndexByName: function(name) { return _indexByName[name]; },
    totalJobs: function() { return _jobs.length; },

    isSelected: function(job) { return job === _selectedJob; },
    selectedJob: function() { return _selectedJob; },
    selectJob: function(job) {
      _selectedJob = job;
      this.trigger('jobSelected', {job: job, jobs: _jobs});
    }
  };

  // set the init function's prototype for later instantiation
  ui.fn.init.prototype = ui.fn;

  return ui;
});
