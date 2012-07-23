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
 * Ambrose module "table" which controls the table view of all jobs in workflow.
 */
define(['jquery', 'ambrose', 'ambrose-util', 'd3'], function($, ambrose, util) {
  var table = ambrose.table = function(ui) {
    return new ambrose.table.fn.init(ui);
  };

  function _handleDagLoaded(event, data) {
    if (this.supportsJob(data)) {
      this.initTable();
      this.loadTable(data.jobs);
    }
  }

  function _handleJobUpdated(event, data) {
    if (this.supportsJob(data)) {
      this.updateTableRow(data.job);
    }
  }

  table.fn = table.prototype = {
    init: function(ui) {
      this.ui = ui;
      this.initTable();
      var table = this;
      ui.bind('dagLoaded', function(event, data) {
        _handleDagLoaded.call(table, event, data);
      });
      ui.bind('JOB_STARTED JOB_PROGRESS JOB_FAILED JOB_FINISHED jobSelected', function(event, data) {
        _handleJobUpdated.call(table, event, data);
      });
    },

    supportsJob: function(data) {
      return (data.event && data.event.runtimeName == 'pig') ||
        (data.job && data.job.runtimeName == 'pig') ||
        (data.jobs && data.jobs[0] && data.jobs[0].runtimeName == 'pig');
    },

    initTable: function() {
      $('#job-summary > thead').empty().append(
        '<tr>'
          + '<th></th>'
          + '<th>Job ID</th>'
          + '<th>Status</th>'
          + '<th>Aliases</th>'
          + '<th>Features</th>'
          + '<th>Mappers</th>'
          + '<th>Reducers</th>'
          + '</tr>'
      );
    },

    loadTable: function(jobs) {
      var table = this;
      var tbody = $('#job-summary > tbody').empty();
      jobs.forEach(function(job) {
        var rowClass = '';
        if (job.index % 2 != 0) rowClass = 'odd';
        tbody.append(
          '<tr id="job-summary-row-num-' + job.index + '">'
            + '<td class="row-job-num">' + (job.index + 1) + '</td>'
            + '<td class="row-job-id"><a class="job-jt-url" target="_blank"></a></td>'
            + '<td class="row-job-status"/>'
            + '<td class="row-job-alias"/>'
            + '<td class="row-job-feature"/>'
            + '<td class="row-job-mappers"/>'
            + '<td class="row-job-reducers"/>'
            + '</tr>'
        );
        $('#job-summary-row-num-' + job.index).bind('click', function() {
          table.ui.selectJob(job);
        });
        table.updateTableRow(job);
      });
    },

    updateTableRow: function(job) {
      var row = $('#job-summary-row-num-' + job.index);
      $('.job-jt-url', row).text(job.jobId).attr('href', job.trackingUrl);
      $('.row-job-status', row).text(util.value(job.status));
      $('.row-job-alias', row).text(util.comma_join(job.aliases));
      $('.row-job-feature', row).text(util.comma_join(job.features));
      $('.row-job-mappers', row).text(util.task_progress_string(job.totalMappers, job.mapProgress));
      $('.row-job-reducers', row).text(util.task_progress_string(job.totalReducers, job.reduceProgress));
    },

    supportsJob: function(data) {
      return (data.event && data.event.runtime == 'pig') ||
             (data.job && data.job.runtime == 'pig') ||
             (data.jobs && data.jobs[0] && data.jobs[0].runtime == 'pig');
    }
  };

  // set the init function's prototype for later instantiation
  table.fn.init.prototype = table.fn;

  return table;
});
