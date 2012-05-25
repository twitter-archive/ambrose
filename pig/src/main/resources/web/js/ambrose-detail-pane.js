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
AMBROSE.detailView = function (ui) {
  this.ui = ui;

  /**
   * Updates table with job data.
   */
  function updateJobDialog(job, totalJobCount) {
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
  }

  /**
   * Select the given job and update global state.
   */
  $( this.ui ).bind( "jobSelected JOB_STARTED JOB_PROGRESS JOB_FAILED JOB_FINISHED", function(event, data) {
    if ($(this.ui).isSelected(data.job)) {
      updateJobDialog(data.job, $(this.ui).totalJobs());
    }
  })
}
