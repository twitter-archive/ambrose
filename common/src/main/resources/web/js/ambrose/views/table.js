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
 * This module defines the Table view which generates a dynamic tabular view of a Workflow's jobs.
 */
define(['jquery', 'd3', '../core', './core'], function($, d3, Ambrose, View) {
  // Table ctor
  var Table = View.Table = function(workflow, container) {
    return new View.Table.fn.init(workflow, container);
  };

  /**
   * Table prototype.
   */
  Table.fn = Table.prototype = {
    /**
     * Constructor.
     *
     * @param workflow the Workflow instance to bind to.
     * @param container the DOM element in which to render the view.
     */
    init: function(workflow, container) {
      this.workflow = workflow;
      this.container = $(container);
      this.initTable();
      var self = this;
      workflow.on('jobsLoaded', function(event, data) {
        self.handleJobsLoaded(event, data);
      });
      workflow.on('jobStarted jobProgress jobComplete jobFailed', function(event, data) {
        self.handleJobUpdated(event, data);
      });
      workflow.on('jobSelected jobMouseOver', function(event, job, prev) {
        if (job != null) self.handleJobUpdated(event, job);
        if (prev != null) self.handleJobUpdated(event, prev);
      });
    },

    initTable: function() {
      $('<table class="table table-condensed table-striped ambrose-views-table">'
        + '<thead><tr>'
        + '<th>#</th>'
        + '<th>Identifier</th>'
        + '<th>Status</th>'
        + '<th>Aliases</th>'
        + '<th>Features</th>'
        + '<th>Mappers</th>'
        + '<th>Reducers</th>'
        + '</tr></thead>'
        + '</table>')
        .appendTo(this.container.empty())
        .append(this.tbody = $('<tbody/>'));
    },

    handleJobsLoaded: function(event, data) {
      this.loadTable(data);
    },

    handleJobUpdated: function(event, data) {
      this.updateTableRow(data);
    },

    selectRows: function(jobs) {
      // select rows and bind to jobs data
      return d3.select(this.tbody.get(0)).selectAll('tr')
        .data(jobs, function(job) { return job.id; });
    },

    removeRows: function(tr) {
      // remove rows for missing jobs data
      tr.exit().remove();
    },

    createRows: function(tr) {
      // create rows for new jobs data
      tr = tr.enter().append('tr');
      tr.append('td').attr('class', 'job-num')
        .text(function(job) { return 1 + (job.topologicalIndex || job.index); });
      tr.append('td').attr('class', 'job-id')
        .append('a').attr('class', 'job-url')
        .attr('target', '_blank')
        .attr('href', 'javascript:void(0);');
      tr.append('td').attr('class', 'job-status')
      tr.append('td').attr('class', 'job-aliases')
      tr.append('td').attr('class', 'job-features')
      tr.append('td').attr('class', 'job-mappers')
      tr.append('td').attr('class', 'job-reducers');
    },

    updateRows: function(tr, duration) {
      // update mutable row properties
      duration = duration || 250;
      tr.transition().duration(duration)
        .style('background-color', function(job) {
          if (job.mouseover) return d3.rgb(98, 98, 196).brighter();
          if (job.selected) return d3.rgb(98, 98, 196);
          if (job.status == 'RUNNING') return d3.rgb(98, 196, 98).brighter();
          if (job.status == 'COMPLETE') return d3.rgb(98, 196, 98);
          if (job.status == 'FAILED') return d3.rgb(196, 98, 98);
          return null;
        });
      tr.selectAll('a.job-url')
        .attr('href', function(job) { return job.trackingUrl || 'javascript:void(0);'; })
        .text(function(job) { return job.id; });
      tr.selectAll('td.job-status')
        .text(function (job) { Ambrose.nullToEmpty(job.status); });
      tr.selectAll('td.job-aliases')
        .text(function (job) { Ambrose.commaDelimit(job.aliases); });
      tr.selectAll('td.job-features')
        .text(function (job) { Ambrose.commaDelimit(job.features); });
      tr.selectAll('td.job-mappers')
        .text(function (job) { Ambrose.taskProgressMessage(job.totalMappers, job.mapProgress); });
      tr.selectAll('td.job-reducers')
        .text(function (job) { Ambrose.taskProgressMessage(job.totalReducers, job.reduceProgress); });
    },

    loadTableD3: function(jobs) {
      var tr = this.selectRows(jobs);
      this.removeRows(tr);
      this.createRows(tr);
      this.updateRows(tr);
    },

    updateTableRowD3: function(job) {
      this.updateRows(this.selectRows([job]));
    },

    loadTable: function(jobs) {
      var self = this;
      var tbody = this.tbody.empty();
      this.rows = {};
      $.each(jobs, function(i, job) {
        var tr = $('<tr>'
          + '<td class="job-num">' + (i + 1) + '</td>'
          + '<td class="job-id"><a class="job-url" href="javascript:void(0);" target="_blank"></a></td>'
          + '<td class="job-status"/>'
          + '<td class="job-alias"/>'
          + '<td class="job-feature"/>'
          + '<td class="job-mappers"/>'
          + '<td class="job-reducers"/>'
          + '</tr>')
          .appendTo(tbody)
          .on('click', function() { self.workflow.selectJob(job); })
          .on('mouseover', function() { self.workflow.mouseOverJob(job); })
          .on('mouseout', function() { self.workflow.mouseOverJob(null); });
        self.rows[job.id] = tr;
        self.updateTableRow(job, tr);
      });
    },

    updateTableRow: function(job, tr) {
      // find job row
      if (tr == null) {
        tr = this.rows[job.id];
        if (tr == null) {
          console.error("No row for job id '" + job.id + "' exists");
          return;
        }
      }
      // update data
      $('.job-url', tr).text(job.id).attr('href', job.trackingUrl);
      $('.job-status', tr).text(Ambrose.nullToEmpty(job.status));
      $('.job-alias', tr).text(Ambrose.commaDelimit(job.aliases));
      $('.job-feature', tr).text(Ambrose.commaDelimit(job.features));
      $('.job-mappers', tr).text(Ambrose.taskProgressMessage(job.totalMappers, job.mapProgress));
      $('.job-reducers', tr).text(Ambrose.taskProgressMessage(job.totalReducers, job.reduceProgress));
      // update css
      tr.removeClass();
      if (job.mouseover) tr.addClass('mouseover');
      if (job.selected) tr.addClass('selected');
      if (job.status == 'FAILED') tr.addClass('error');
      if (job.status == 'COMPLETE') tr.addClass('success');
    },
  };

  // bind prototype to ctor
  Table.fn.init.prototype = Table.fn;
  return Table;
});
