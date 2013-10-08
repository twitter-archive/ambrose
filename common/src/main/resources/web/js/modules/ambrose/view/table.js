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
define(['lib/jquery', 'lib/d3', '../core', './core'], function($, d3, Ambrose, View) {
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
      this.workflow = workflow;
      this.container = $(container);
      this.initTable();
      this.params = $.extend(true, {}, View.Theme, params);
      var self = this;
      workflow.on('jobsLoaded', function(event, jobs) {
        self.loadTable(jobs);
      });
      workflow.on('jobStarted jobProgress jobComplete jobFailed', function(event, job) {
        self.updateTableRows([job], 350);
      });
      workflow.on('jobSelected jobMouseOver', function(event, job, prev) {
        var jobs = [];
        if (prev) jobs.push(prev);
        if (job) jobs.push(job);
        self.updateTableRows(jobs);
      });
    },

    initTable: function() {
      var self = this;
      var tbody = this.tbody = $('<tbody/>');
      $('<table class="table ambrose-view-table">'
        + '<thead><tr>'
        + '<th>#</th>'
        + '<th>Identifier</th>'
        + '<th>Status</th>'
        + '<th>Aliases</th>'
        + '<th>Features</th>'
        + '<th>Time</th>'
        + '<th>Mappers</th>'
        + '<th>Reducers</th>'
        + '</tr></thead>'
        + '</table>')
        .appendTo(this.container.empty())
        .append(tbody);
    },

    loadTable: function(jobs) {
      var tr = this.selectRows(jobs);
      this.removeRows(tr);
      this.createRows(tr);
      this.updateRows(tr);
    },

    updateTableRows: function(jobs, duration) {
      this.updateRows(this.selectRows(jobs), duration);
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
      // create rows for new jobs data
      tr = tr.enter().append('tr');
      tr.style('background-color', 'white');
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
      var self = this;
      tr.on('mouseover', function(job) { self.workflow.mouseOverJob(job); })
        .on('mouseout', function(job) { self.workflow.mouseOverJob(null); })
        .on('click', function(job) { self.workflow.selectJob(job); });
    },

    updateRows: function(tr, duration) {
      // update mutable row properties
      var colors = this.params.colors;
      if (duration) {
        // rows updated due to event; transition background color gradually
        tr.transition().duration(duration).filter(function(job) {
          // don't update rows whose jobs are selected or mouseover
          return !(job.mouseover || job.selected);
        }).style('background-color', function(job) {
          var status = job.status || '';
          return colors[status.toLowerCase()] || 'white';
        });

      } else {
        // rows updated due to user interaction; rapidly update background color
        tr.style('background-color', function(job) {
          if (job.mouseover) return colors.mouseover;
          if (job.selected) return colors.selected;
          var status = job.status || '';
          return colors[status.toLowerCase()] || 'white';
        });
      }

      function commaDelimit(array) {
        if (array == null) return '';
        return array.join(', ');
      }

      function taskProgressMessage(totalTasks, taskProgress, completedTasks) {
        if (totalTasks == null || taskProgress == null) return 'N/A';

        if (completedTasks != null && totalTasks != null && taskProgress != null) {
            return completedTasks + " / " + totalTasks + ' (' +
            (Math.round(Number(taskProgress) * 10000, 0)) / 100 + '%)';
        }
        return totalTasks + ' (' + (Math.round(Number(taskProgress) * 10000, 0)) / 100 + '%)';
      }

      function setJobTime(status, mapperStartTime, mapperEndTime, reducerStartTime, reducerEndTime) {
        if (status == null || mapperStartTime == null || mapperStartTime == 0) { return '---'; }

        // Return mapper start/end time once ready.
        if (reducerEndTime == null || status == 'RUNNING' || reducerEndTime == 0) {
          return "Started at: <br>" + formatTimestamp(mapperStartTime);
        } else if (status == 'COMPLETE' || status == 'FAILED') {
          return "Started at: <br>" + formatTimestamp(mapperStartTime) + "<br>"
            + "Ended at: <br>" + formatTimestamp(reducerEndTime) + "<br>"
            + "Elapsed Time: <br>" + calculateElapsedTime(mapperStartTime, reducerEndTime);
        }
      }

      function calculateElapsedTime(start, end) {
          var ms = Number(end) - Number(start);

          var d, h, m, s;
          var elapsedTime = "";
          s = Math.floor(ms / 1000);
          m = Math.floor(s / 60);
          s = s % 60;
          h = Math.floor(m / 60);
          m = m % 60;
          d = Math.floor(h / 24);
          h = h % 24;

          if (d != 0 && d != null) elapsedTime += ", " + d + " days";
          if (h != 0 && h != null) elapsedTime += ", " + h + " hours";
          if (m != 0 && m != null) elapsedTime += ", " + m + " mins";
          if (s != 0) elapsedTime += ", " + s + " sec";

          return elapsedTime.substring(2);
      };

      function pad(number) {
          var r = String(number);
          if (r.length === 1) r = '0' + r;
          return r;
      }

      function formatTimestamp(timestamp) {
        var time = Number(timestamp);
        var date = new Date(time);
        var timezoneOffsetHours = date.getTimezoneOffset() / 60;
        var timezoneSeparator = timezoneOffsetHours >= 0 ? '-' : '+';
        timezoneOffsetHours = Math.abs(timezoneOffsetHours);

        return date.getFullYear() + '-'
        + pad(date.getMonth() + 1) + '-'
        + pad(date.getDate()) + ' '
        + pad(date.getHours()) + ':'
        + pad(date.getMinutes()) + ':'
        + pad(date.getSeconds())
        + ' UTC' + timezoneSeparator + pad(timezoneOffsetHours);
      }

      // update all other params normally
      tr.selectAll('a.job-url')
        .attr('href', function(job) {
          var mrState = job.mapReduceJobState || {};
          return mrState.trackingURL || 'javascript:void(0);';
        })
        .text(function(job) { return job.id || 'N/A'; });
      tr.selectAll('td.job-status')
        .text(function (job) { return job.status || 'PENDING'; });
      tr.selectAll('td.job-aliases')
        .text(function (job) { return commaDelimit(job.aliases); });
      tr.selectAll('td.job-features')
        .text(function (job) { return commaDelimit(job.features); });
      tr.selectAll('td.job-time').html(function (job) {
          var mrState = job.mapReduceJobState || {};
          return setJobTime(
                  job.status,
                  mrState.mapStartTime,
                  mrState.mapEndTime,
                  mrState.reduceStartTime,
                  mrState.reduceEndTime);
        });
      tr.selectAll('td.job-mappers').text(function (job) {
        var mrState = job.mapReduceJobState || {};
        return taskProgressMessage(
          mrState.totalMappers,
          mrState.mapProgress,
          mrState.finishedMappersCount);
      });
      tr.selectAll('td.job-reducers').text(function (job) {
        var mrState = job.mapReduceJobState || {};
        return taskProgressMessage(
          mrState.totalReducers,
          mrState.reduceProgress,
          mrState.finishedReducersCount);
      });
    },
  };

  // bind prototype to ctor
  Table.fn.init.prototype = Table.fn;
  return Table;
});
