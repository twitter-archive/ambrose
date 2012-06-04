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

// controls the detail view for the currently selected job
AMBROSE.detailView = function() {
  var ui, view;

  // private members and methods above, public below
  return {
    /**
     * Initialize an empty table with the expected structure
     */
    initTable: function() {
      $('#job-props > thead').empty();
      $('#job-props > tbody').empty();
      $('#job-props > thead:last').append(
        '<tr><th>Property</th><th>Value</th></tr>');
      $('#job-props > tbody:last').append(
        '<tr><td>Number</td><td id="job-n-of-n"></td></tr>' +
        '<tr><td>Job ID</td><td><a id="job-jt-url" target="_blank"></a></td></tr>' +
        '<tr><td>Status</td><td id="job-status"></td></tr>' +
        '<tr><td>Aliases</td><td id="job-aliases"></td></tr>' +
        '<tr><td>Features</td><td id="job-features"></td></tr>' +
        '<tr><td>Mappers</td><td id="job-mapper-status"></td></tr>' +
        '<tr><td>Reducers</td><td id="job-reducer-status"></td></tr>');
    },

    /**
     * Updates table with job data.
     */
    updateJobDialog: function(job, totalJobCount) {
      if (job.index >= 0) {
        $('#job-n-of-n').text((job.index + 1) + ' of ' + totalJobCount);
      } else {
        $('#job-n-of-n').text('');
      }
      $('#job-jt-url').text(job.jobId);
      $('#job-jt-url').attr('href', job.trackingUrl);
      $('#job-aliases').text(AMBROSE.util.comma_join(job.aliases));
      $('#job-features').text(AMBROSE.util.comma_join(job.features));
      $('#job-status').text(job.status);
      $('#job-mapper-status').text(AMBROSE.util.task_progress_string(job.totalMappers, job.mapProgress));
      $('#job-reducer-status').text(AMBROSE.util.task_progress_string(job.totalReducers, job.reduceProgress));
    },

    supportsJob: function(data) {
      return (data.event && data.event.runtime == 'pig') ||
             (data.job && data.job.runtime == 'pig');
    },

    /**
     * Initializes this object to listen for certain events.
     */
    init: function(ui) {
      this.ui = ui;
      view = this;
      view.initTable();
      $(ui).bind( "jobSelected JOB_STARTED JOB_PROGRESS JOB_FAILED JOB_FINISHED", function(event, data) {
        if (ui.isSelected(data.job) && view.supportsJob(data)) {
          //TODO: clear table if view doesn't support it.
          view.initTable();
          view.updateJobDialog(data.job, ui.totalJobs());
        }
      })
    }
  };
};
