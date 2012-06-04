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

// controls the table view of the list of jobs
AMBROSE.tableView = function () {
  var ui, view;

  // private members and methods above, public below
  return {

    /**
     * Initialize an empty table with the expected structure
     */
    initTable: function() {
      $('#job-summary > thead').empty();
      $('#job-summary > thead:last').append(
        '<tr>' +
         '<th></th>' +
         '<th>Job ID</th>' +
         '<th>Status</th>' +
         '<th>Aliases</th>' +
         '<th>Features</th>' +
         '<th>Mappers</th>' +
         '<th>Reducers</th>' +
        '</tr>');
    },

    loadTable: function(jobs) {
      jobs.forEach(function(job) {
        var rowClass = ''
        if (job.index % 2 != 0) {
          rowClass = 'odd'
        }
        $('#job-summary tr:last').after(
          '<tr id="row-num-' + job.index + '">'+
            '<td class="row-job-num">' + (job.index + 1) + '</td>' +
            '<td class="row-job-id"><a class="job-jt-url" target="_blank"></a></td>' +
            '<td class="row-job-status"/>' +
            '<td class="row-job-alias"/>' +
            '<td class="row-job-feature"/>' +
            '<td class="row-job-mappers"/>' +
            '<td class="row-job-reducers"/>' +
          '</tr>'
        );
        $('#row-num-' + job.index).bind('click', function() {
          $(this.ui).selectJob(job);
        });
        view.updateTableRow(job);
      });
    },

    updateTableRow: function(job) {
      var row = $('#row-num-' + job.index);
      $('.job-jt-url', row).text(job.jobId).attr('href', job.trackingUrl);
      $('.row-job-status', row).text(AMBROSE.util.value(job.status));
      $('.row-job-alias', row).text(AMBROSE.util.comma_join(job.aliases));
      $('.row-job-feature', row).text(AMBROSE.util.comma_join(job.features));
      $('.row-job-mappers', row).text(AMBROSE.util.task_progress_string(job.totalMappers, job.mapProgress));
      $('.row-job-reducers', row).text(AMBROSE.util.task_progress_string(job.totalReducers, job.reduceProgress));
    },

    supportsJob: function(data) {
      return (data.event && data.event.runtime == 'pig') ||
             (data.job && data.job.runtime == 'pig') ||
             (data.jobs && data.jobs[0] && data.jobs[0].runtime == 'pig');
    },

    init: function(ui) {
      this.ui = ui;
      view = this;
      view.initTable();

      $(ui).bind( "dagLoaded", function(event, data) {
        if (view.supportsJob(data)) {
          view.initTable();
          view.loadTable(data.jobs);
        }
      })

      $(ui).bind( "jobSelected JOB_STARTED JOB_PROGRESS JOB_FAILED JOB_FINISHED", function(event, data) {
        if (view.supportsJob(data)) {
          view.updateTableRow(data.job);
        }
      })
    }
  }
}