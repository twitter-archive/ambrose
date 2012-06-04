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
 * Ambrose module "detail" controls the detail view for the currently selected
 * job.
 */
(function($, d3, ambrose) {
  var detail = ambrose.detail = function(ui) {
    return new ambrose.detail.fn.init(ui);
  };

  function _handleJobSelected(event, data) {
    if (this.supportsJob(data)) {
      this.update(data.job);
    } else {
      this.clear();
    }
  }

  function _handleJobProgress(event, data) {
    var job = data.job;
    if (this.ui.isSelected(job)) {
      if (this.supportsJob(data)) {
        this.update(job);
      } else {
        this.clear();
      }
    }
  }

  /**
   * Define AMBROSE.detail.prototype.
   */
  detail.fn = detail.prototype = {
    /**
     * Constructor initializes public fields and binds to 'jobSelected' event.
     */
    init: function(ui) {
      this.ui = ui;
      var detail = this;
      ui.bind('jobSelected', function(event, data) {
        _handleJobSelected.call(detail, event, data);
      });
      ui.bind('JOB_PROGRESS', function(event, data) {
        _handleJobProgress.call(detail, event, data);
      });
    },

    supportsJob: function(data) {
      return (data.job && data.job.runtimeName == 'pig');
    },

    clear: function() {
      $('#job-props > thead').empty();
      $('#job-props > tbody').empty();
    },

    reset: function() {
      this.clear();
      $('#job-props > thead').append('<tr><th>Property</th><th>Value</th></tr>');
      $('#job-props > thead').append(
        '<tr><td>Number</td><td id="job-n-of-n"></td></tr>'
          + '<tr><td>Job ID</td><td><a id="job-jt-url" target="_blank"></a></td></tr>'
          + '<tr><td>Status</td><td id="job-status"></td></tr>'
          + '<tr><td>Aliases</td><td id="job-aliases"></td></tr>'
          + '<tr><td>Features</td><td id="job-features"></td></tr>'
          + '<tr><td>Mappers</td><td id="job-mapper-status"></td></tr>'
          + '<tr><td>Reducers</td><td id="job-reducer-status"></td></tr>'
      );
    },

    update: function(job) {
      this.reset();
      var totalJobs = this.ui.totalJobs();
      if (job.index >= 0) {
        $('#job-n-of-n').text((job.index + 1) + ' of ' + totalJobs);
      } else {
        $('#job-n-of-n').text('');
      }
      $('#job-jt-url').text(job.jobId).attr('href', job.trackingUrl);
      $('#job-status').text(job.status);
      $('#job-aliases').text(ambrose.util.comma_join(job.aliases));
      $('#job-features').text(ambrose.util.comma_join(job.features));
      $('#job-mapper-status').text(ambrose.util.task_progress_string(job.totalMappers, job.mapProgress));
      $('#job-reducer-status').text(ambrose.util.task_progress_string(job.totalReducers, job.reduceProgress));
    },
  };

  // set the init function's prototype for later instantiation
  detail.fn.init.prototype = detail.fn;

}(jQuery, d3, AMBROSE));
