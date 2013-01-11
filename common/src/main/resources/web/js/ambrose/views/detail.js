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
 * This module defines the Detail view which generates a tabular view of the currently selected job.
 */
define(['jquery', 'd3', '../core', './core'], function($, d3, Ambrose, View) {
  // Detail ctor
  var Detail = View.Detail = function(workflow, container) {
    return new View.Detail.fn.init(workflow, container);
  };

  /**
   * Detail prototype.
   */
  Detail.fn = Detail.prototype = {
    /**
     * Constructor.
     *
     * @param workflow the Workflow instance to bind to.
     * @param container the DOM element in which to render the view.
     * @param params extra options.
     */
    init: function(workflow, container, params) {
      this.workflow = workflow;
      this.container = container = $(container).empty();
      var table = this.table =
        $('<table class="table">'
          + '<thead><tr><th>Property</th><th>Value</th></tr></thead>'
          + '<tbody>'
          + '<tr><td>Number</td><td class="job-num">No job selected</td></tr>'
          + '<tr><td>Job Id</td><td class="job-id"><a class="job-url" href="javascript:void(0);" target="_blank"></a></td></tr>'
          + '<tr><td>Status</td><td class="job-status"></td></tr>'
          + '<tr><td>Aliases</td><td class="job-aliases"></td></tr>'
          + '<tr><td>Features</td><td class="job-features"></td></tr>'
          + '<tr><td>Mappers</td><td class="job-mappers"></td></tr>'
          + '<tr><td>Reducers</td><td class="job-reducers"></td></tr>'
          + '</tbody>'
          + '</table>')
        .appendTo(container);
      $('tr td:first-child', table).width('80px');
      this.fields = {
        jobNum: $('td.job-num', table),
        jobUrl: $('a.job-url', table),
        jobStatus: $('td.job-status', table),
        jobAliases: $('td.job-aliases', table),
        jobFeatures: $('td.job-features', table),
        jobMappers: $('td.job-mappers', table),
        jobReducers: $('td.job-reducers', table),
      };
      var self = this;
      workflow.on('jobSelected', function(event, job, prev) {
        self.refresh(job);
      });
    },

    refresh: function(job) {
      var fields = this.fields;
      if (job == null) {
        $.each(fields, function(n, f) { f.empty(); });
        fields.jobNum.text('No job selected');
        return;
      }
      fields.jobNum.text(1 + (job.topologicalIndex || job.index));
      fields.jobUrl.attr('href', job.trackerUrl || 'javascript:void(0);').text(job.id);
      fields.jobStatus.text(job.status || '');
      fields.jobAliases.text(Ambrose.commaDelimit(job.aliases));
      fields.jobFeatures.text(Ambrose.commaDelimit(job.features));
      fields.jobMappers.text(Ambrose.taskProgressMessage(job.totalMappers, job.mapProgress));
      fields.jobReducers.text(Ambrose.taskProgressMessage(job.totalReducers, job.reduceProgress));
    },
  };

  // bind prototype to ctor
  Detail.fn.init.prototype = Detail.fn;
  return Detail;
});
