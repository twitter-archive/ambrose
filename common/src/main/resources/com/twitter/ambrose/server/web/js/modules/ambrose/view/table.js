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
define(['lib/jquery', 'lib/underscore', 'lib/d3', '../core', './core', 'lib/bootstrap'], function($, _, d3, Ambrose, View) {
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
     * @param params extra options.
     */
    init: function(workflow, container, params) {
      var self = this;
      self.workflow = workflow;
      self.container = $(container);
      self.params = $.extend(true, {}, View.Theme, params);
      self.initTable();

      workflow.on('jobsLoaded', function(event, jobs) {
        var tr = self.selectRows(jobs);
        self.removeRows(tr);
        self.createRows(tr);
        self.updateRows(tr);
      });

      workflow.on('jobStarted jobProgress jobComplete jobFailed', function(event, job) {
        self.updateRows(self.selectRows([job]));
      });

      workflow.on('jobSelected jobMouseOver', function(event, job, prev) {
        self.updateRows(self.selectRows(_.reject([job, prev], _.isNull)));
      });
    },

    initTable: function() {
      var self = this;
      var tbody = self.tbody = $('<tbody>');
      $('<table class="table ambrose-view-table">'
        + '<thead><tr>'
        + '<th>#</th>'
        + '<th>Identifier</th>'
        + '<th>Status</th>'
        + '<th>Aliases</th>'
        + '<th>Features</th>'
        + '<th>Duration</th>'
        + '<th>Mappers</th>'
        + '<th>Reducers</th>'
        + '</tr></thead>'
        + '</table>')
        .appendTo(self.container.empty())
        .append(tbody);
    },

    selectRows: function(jobs) {
      // select rows and bind to jobs data
      return d3.select(this.tbody.get(0)).selectAll('tr')
        .data(jobs, function(job) { return job.name; });
    },

    removeRows: function(tr) {
      // remove rows for missing jobs data
      tr.exit().remove();
    },

    createRows: function(tr) {
      var self = this;
      // create rows for new jobs data
      tr = tr.enter().append('tr');
      tr.append('td').attr('class', 'job-num')
        .text(function(job) { return 1 + (job.topologicalIndex || job.index); });
      tr.append('td').attr('class', 'job-id')
        .append('a').attr('class', 'job-url')
        .attr('target', '_blank')
        .attr('href', 'javascript:void(0);')
        .on('click', function() {
          d3.event.stopPropagation();
        });
      tr.append('td').attr('class', 'job-status');
      tr.append('td').attr('class', 'job-aliases');
      tr.append('td').attr('class', 'job-features');
      tr.append('td').attr('class', 'job-time');
      tr.append('td').attr('class', 'job-mappers');
      tr.append('td').attr('class', 'job-reducers');
      tr.on('mouseover', function(job) { self.workflow.mouseOverJob(job); })
        .on('mouseout', function(job) { self.workflow.mouseOverJob(null); })
        .on('click', function(job) { self.workflow.selectJob(job); });
    },

    updateRows: function(tr) {
      var self = this;

      // update mutable row properties
      tr.classed('selected', function(job) {
        return self.workflow.current.selected === job;
      });
      tr.classed('hover', function(job) {
        return self.workflow.current.mouseover === job;
      });

      // join the array elements by comma
      function commaDelimit(array) {
        if (array == null) return '';
        return array.join(', ');
      }

      function taskProgressMessage(totalTasks, taskProgress, completedTasks) {
        if (totalTasks == null || taskProgress == null) return '';

        if (completedTasks != null && totalTasks != null && taskProgress != null) {
          return completedTasks + ' / ' + totalTasks + ' (' +
            (Math.round(Number(taskProgress) * 10000, 0)) / 100 + '%)';
        }
        return totalTasks + ' (' + (Math.round(Number(taskProgress) * 10000, 0)) / 100 + '%)';
      }

      function setJobTime(status, jobStartTime, jobLastUpdateTime) {
        if (status == null || jobStartTime == null || jobStartTime == 0) return '';
        var tooltip = 'B: ' + jobStartTime.formatTimestamp();
        if (status == 'COMPLETE' || status == 'FAILED') {
          tooltip += '<br/>E: ' + jobLastUpdateTime.formatTimestamp();
        }
        var elapsedTime = Ambrose.calculateElapsedTime(jobStartTime, jobLastUpdateTime);
        return '<div title="' + tooltip + '">' + elapsedTime + '</div>';
      }

      // update all other params normally
      tr.selectAll('a.job-url').attr('href', function(job) {
        var mrState = job.mapReduceJobState || {};
        return mrState.trackingURL || 'javascript:void(0);';
      }).text(function(job) { return job.id || ''; });

      tr.selectAll('td.job-status').html(function (job) {
        var status = job.status || 'PENDING';
        var style = 'label-default';
        switch (status) {
        case 'RUNNING': style = 'label-primary'; break;
        case 'COMPLETE': style = 'label-success'; break;
        case 'FAILED': style = 'label-danger'; break;
        }
        return '<span class="label ' + style + '">' + status.toLowerCase() + '</span>';
      });

      tr.selectAll('td.job-aliases').text(function (job) { return commaDelimit(job.aliases); });

      tr.selectAll('td.job-features').text(function (job) { return commaDelimit(job.features); });

      tr.selectAll('td.job-time').html(function (job) {
        var mrState = job.mapReduceJobState || {};
        return setJobTime(job.status, mrState.jobStartTime, mrState.jobLastUpdateTime);
      });

      self.tbody.find('td.job-time > div').tooltip({ html: true });

      tr.selectAll('td.job-mappers').text(function (job) {
        var mrState = job.mapReduceJobState || {};
        return taskProgressMessage(mrState.totalMappers, mrState.mapProgress, mrState.finishedMappersCount);
      });

      tr.selectAll('td.job-reducers').text(function (job) {
        var mrState = job.mapReduceJobState || {};
        return taskProgressMessage(mrState.totalReducers, mrState.reduceProgress, mrState.finishedReducersCount);
      });
    }
  };

  // bind prototype to ctor
  Table.fn.init.prototype = Table.fn;
  return Table;
});
