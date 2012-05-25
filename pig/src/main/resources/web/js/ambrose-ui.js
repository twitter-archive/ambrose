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

// "static" helper utilities
AMBROSE.util = function() {
  return {
    comma_join: function(array) {
      if  (array != null) {
        return array.join(", ");
      }
      return '';
    },
    task_progress_string: function(totalTasks, taskProgress) {
      if (totalTasks == null || taskProgress == null) {
        return ''
      }
      return totalTasks + ' (' + d3.round(taskProgress * 100, 0) + '%)'
    },
    value: function(value) {
      if (value == null) {
        return '';
      }
      return value;
    }
  };
}();

AMBROSE.ui = function(name, age) {
  // storage for job data and lookup
  var ui = this;
  var url = window.location.href;

  var jobs = [];
  var selectedJob;
  var jobsByName = {}, jobsByJobId = {}, indexByName = {}, nameByIndex = {};
  var scriptProgress = 0;

  var loadDagIntervalId, pollIntervalId;
  var MAX_NULL_EVENTS = 10;
  var consecutiveNullEvents = 0;
  var lastProcessedEventId = -1;

  // handle demo data
  if (url.indexOf('?localdata=small') != -1) {
    dagURL = "data/small-dag.json";
    eventsURL = "data/small-events.json";
  } else if (url.indexOf('?localdata=large') != -1) {
    dagURL = "data/large-dag.json";
    eventsURL = "data/large-events.json";
  } else {
    dagURL = "dag";
    eventsURL = "events";
  }

  $.fn.initialize = function() {
    loadDagTimeoutId = setTimeout('$(ui).loadDag()', 500);
  }

  /**
   * Retrieves snapshot of current DAG of scopes from the server
   */
  $.fn.loadDag = function() {
    // load dag data and initialize
    d3.json(dagURL, function(data) {
      if (data == null) {
        alert("Failed to load dag data");
        return;
      }
      jobs = data;

      buildJobIndex(jobs);

      $(ui).trigger( "dagLoaded", {"jobs": jobs} );
      $(ui).trigger( "jobSelected", {"job": jobs[0], "jobs": jobs} );
      clearTimeout(loadDagIntervalId);
      $(ui).startEventPolling();
    });
  }

  $.fn.stopEventPolling = function() {
    clearInterval(pollIntervalId);
    return pollIntervalId;
  }

  $.fn.startEventPolling = function() {
    pollIntervalId = setInterval('$(ui).pollEvents()', 1000);
    return pollIntervalId;
  }

  // select a job
  $.fn.selectJob = function(job) {
    selectedJob = job;
    $(ui).trigger( "jobSelected", {"job": job} );
  }

  // get the selected job
  $.fn.selectedJob = function() {
    return selectedJob;
  }

  // get the selected job
  $.fn.totalJobs = function() {
    return jobs.length;
  }

  // is job selected?
  $.fn.isSelected = function(job) {
    return job === selectedJob;
  }

  // display an error
  $.fn.error = function(msg) {
    d3.select('#scriptStatusDialog').text(msg);
  }

  // display info
  $.fn.info = function(msg) {
    d3.select('#scriptStatusDialog').text(msg);
  }

  /**
   * Poll the server for new events
   */
  $.fn.pollEvents = function() {
    // are we there yet?
    var scriptDone = false;
    if (scriptProgress == 100) {
      scriptDone = true;
      for (var i = 0; i < jobs.length; i++) {
        var job = jobs[i];
        if(job.status != "COMPLETE" && job.status != "FAILED") {
          scriptDone = false;
          break;
        }
      }
    }

    // stop polling for events if all jobs are done
    if (scriptDone) {
      $(ui).info("script finished");
      $(ui).stopEventPolling();
      return;
    }

    d3.json(eventsURL + "?lastEventId=" + lastProcessedEventId, function(events) {
      // test for error
      if (events == null) {
        consecutiveNullEvents = consecutiveNullEvents + 1;
        if (consecutiveNullEvents >= MAX_NULL_EVENTS) {
            $(ui).stopEventPolling();
            $(ui).error(MAX_NULL_EVENTS + " consecutive requests for events have failed. Stopping event polling.");
        }
        else {
            $(ui).error("No events found")
        }
        return
      }
      consecutiveNullEvents = 0;
      var eventsHandledCount = 0;
      events.forEach(function(event) {
          var eventId = event.eventId;
          if (eventId <= lastProcessedEventId || eventsHandledCount > 0) {
              return;
          }

          if (!event.eventData || !event.eventType) {
            $(ui).error("Invalid event data returned from the server: " + event);
            return;
          }

          var data = {"event": event};
          if (event.eventType.indexOf('JOB_') == 0) {
            // job complete and job failed return a jobData object
            if (event.eventData.jobId == null && event.eventData.jobData.jobId != null) {
              event.eventData.jobId = event.eventData.jobData.jobId;
            }
            data["job"] = updateJobData(event.eventData);
          }

          $(ui).trigger( event.eventType, data);
          lastProcessedEventId = eventId;
          eventsHandledCount++;
      });
    });
  }

  function buildJobIndex(jobs) {
    // Compute a unique index for each job name
    n = 0;
    jobs.forEach(function(job) {
      jobsByName[job.name] = job;
      if (!(job.name in indexByName)) {
        nameByIndex[n] = job.name;
        indexByName[job.name] = job.index = n++;
      }
    });
  }

  /**
   * Looks up job with data.name or data.jobId and updates contents of job with
   * fields from data.
   */
  function updateJobData(data) {
    // get job associated with data
    var job, id;
    if (data.name != null) {
      id = data.name;
      job = jobsByName[id];
    } else {
      id = data.jobId;
      job = jobsByJobId[id];
    }

    // check for job retrieval failure
    if (job == null) {
      alert("Job with id '" + id + "' not found");
      return;
    }

    // copy data into job
    $.each(data, function(key, value) {
      job[key] = value;
    });
    return job
  }

  // these are the events the ui supports that can be bound to as follows
  $(this).bind( "dagLoaded", function(event, data) { });   // data: { "jobs": jobs }
  $(this).bind( "jobUpdated", function(event, data) { });  // data: { "job": job, "jobs": jobs }
  $(this).bind( "jobSelected", function(event, data) { }); // data: { "job": job }

  // these are the events that are returned from the server that can be bounded to as follows
  // data: { "event": event}
  $(this).bind( "WORKFLOW_PROGRESS", function(event, data) {
    scriptProgress = data.event.eventData.scriptProgress;
    $(ui).info('script progress: ' + scriptProgress + '%');
    $('#progressbar div').width(scriptProgress + '%')
  });

  // data: { "event": event, "job": job}
  $(this).bind( "JOB_STARTED", function(event, data) {
    var job = data.job;
    if (job == null) return;
    $(ui).info(job.jobId + ' started');
    job.jobId = data.event.eventData.jobId;
    job.status = "RUNNING";
    jobsByJobId[job.jobId] = job;
    $(ui).selectJob(job);
  });

  $(this).bind( "JOB_PROGRESS", function(event, data) {
    var job = data.job;
    if (job == null) return;
    if (job.isComplete == "true") {
      if (job.isSuccessful == "true") {
        job.status = "COMPLETE";
      } else {
        job.status = "FAILED";
      }
    }
    jobsByJobId[job.jobId] = job;
  });

  $(this).bind( "JOB_FINISHED", function(event, data) {
    var job = data.job;
    if (job == null) return;
    $(ui).info(job.jobId + ' complete');
    job.status = "COMPLETE";

//    TODO: Andy, what's this for?
//    var i = job.index + 1;
//    if (i < jobs.length) {
//      $(ui).selectJob(jobs[i]);
//    }
  });

  $(this).bind( "JOB_FAILED", function(event, data) {
    var job = data.job;
    if (job == null) return;
    $(ui).info(job.jobId + ' failed');
    job.status = "FAILED";
  });
}

AMBROSE.chord = function(ui) {
  this.ui = ui;

  var jobsByName = {}, indexByName = {}, nameByIndex = {};
  var matrix = [];

  // group angle initialized once we know the number of jobs
  var ga = 0, ga2 = 0, gap = 0;

  // radii of svg figure
  var r1 = 400 / 2;
  var r0 = r1 - 60;

  // color palette
  var fill, successFill, errorFill;
  var jobSelectedColor = d3.rgb(98, 196, 98);

  // job dependencies are visualized by chords
  var chord, groups, chords;

  // the actual svg graphic
  var svg;

  // returns start angle for a chord group
  function groupStartAngle(d) {
    return  ga * d.index - ga2;
  }

  // returns end angle for a chord group
  function groupEndAngle(d) {
    return groupStartAngle(d) + ga - gap;
  }

  /**
   * @param d chord data
   * @param f boolean flag indicating chord is out-link
   * @param i chord in- / out-link index within current group
   * @param n in- / out-degree of current group
   */
  function chordAngle(d, f, i, n) {
    var g = groups[d.index];
    var s = g.startAngle;
    var e = g.endAngle;
    var r = (e - s) / 2;
    var ri = r / n;
    return s + r * (f ? 0 : 1) + ri * i;
  }

  // returns color for job arc and chord
  function jobColor(d) {
    if (isMouseOver(d.job)) {
      return d3.rgb(jobMouseOverColor);
    } if ($(this.ui).isSelected(d.job)) {
      return d3.rgb(jobSelectedColor).brighter();
    } if (d.job.status == "RUNNING") {
      return d3.rgb(jobSelectedColor);
    } if (d.job.status == "COMPLETE") {
      return successFill(d.index);
    } if (d.job.status == "FAILED") {
      return errorFill(d.index);
    }
    return d3.hsl(fill(d.index)).darker(0.5);
  }

  // more color funcs
  function chordStroke(d) { return d3.rgb(jobColor(d.source)).darker(); }
  function chordFill(d) { return jobColor(d.source); }

  // mouse over job
  var jobMouseOver;
  var jobMouseOverColor = d3.rgb(20, 155, 223);

  function handleArcMouseOver(d, i) {
    jobMouseOver = d.job;
    refreshDisplay();
  }

  function handleChartMouseOut(d, i) {
    jobMouseOver = null;
    refreshDisplay();
  }

  function isMouseOver(job) {
    return job === jobMouseOver;
  }

  function handleArcClick(d, i) {
    selectJob(d.job);
    refreshDisplay();
  }

  /**
   * Refreshes the visual elements based on current (updated) state.
   */
  function refreshDisplay() {
    // update path.arc elements
    svg.selectAll("path.arc")
      .transition()
      .style("fill", jobColor)
      .style("stroke", jobColor);

    // update path.chord elements
    svg.selectAll("path.chord")
      .transition()
      .style("stroke", chordStroke)
      .style("fill", chordFill);

    /*
    // spin svg to selected job
    var a = (-ga * jobSelected.index) * 180 / Math.PI + 360;
    svg.transition()
      .duration(1000)
      .attr("transform", "rotate(" + a + ")");
    */
  }

  function currentRotation() {
    return svg.attr("transform").match(/rotate\(([^\)]+)\)/i)[0];
  }

  function initialize(jobs) {
    // jobs themselves are arc segments around the edge of the chord diagram
    var arcMouse = d3.svg.arc()
      .innerRadius(50)
      .outerRadius(r0 + 300)
      .startAngle(groupStartAngle)
      .endAngle(groupEndAngle);
    var arc = d3.svg.arc()
      .innerRadius(r0)
      .outerRadius(r0 + 10)
      .startAngle(groupStartAngle)
      .endAngle(groupEndAngle);

    // set up canvas
    svg = d3.select("#chart")
      .append("svg:svg")
      .attr("width", r1 * 3)
      .attr("height", r1 * 2)
      .on('mouseout', handleChartMouseOut)
      .append("svg:g")
      .attr("transform", "translate(" + (r1 * 1.5) + "," + r1 + ")rotate(90)")
      .append("svg:g")
      .attr("transform", "rotate(0)");

    var chord = d3.layout.chord();

    // initialize color palette
    var n = jobs.length;
    if (n > 7) n = 7;
    fill = d3.scale.ordinal().range(colorbrewer.Greys[n]);
    successFill = d3.scale.ordinal().range(colorbrewer.Greens[n]);
    errorFill = d3.scale.ordinal().range(colorbrewer.Reds[n]);

    // initialize group angle
    ga = 2 * Math.PI / jobs.length;
    gap = ga * 0.1;
    ga2 = (ga - gap) / 2;

    // update state
    $(this.ui).selectJob(jobs[0]);

    // Compute a unique index for each job name
    n = 0;
    jobs.forEach(function(j) {
      jobsByName[j.name] = j;
      if (!(j.name in indexByName)) {
        nameByIndex[n] = j.name;
        indexByName[j.name] = j.index = n++;
      }
    });

    // Add predecessor and successor index maps to all jobs
    jobs.forEach(function (j) {
      j.predecessorIndices = {};
      j.successorIndices = {};
    });

    // Construct a square matrix counting dependencies
    for (var i = -1; ++i < n;) {
      var row = matrix[i] = [];
      for (var j = -1; ++j < n;) {
        row[j] = 0;
      }
    }
    jobs.forEach(function(j) {
      var p = indexByName[j.name];
      j.successorNames.forEach(function(n) {
        var s = indexByName[n];
        matrix[s][p]++;

        // initialize predecessor and successor indices
        j.successorIndices[s] = d3.keys(j.successorIndices).length;
        var sj = jobsByName[n];
        sj.predecessorIndices[p] = d3.keys(sj.predecessorIndices).length;
      });
    });

    chord.matrix(matrix);

    // override start and end angles for groups and chords
    groups = chord.groups();
    chords = chord.chords();

    // initialize groups
    for (var i = 0; i < groups.length; i++) {
      var d = groups[i];

      // associate group with job
      d.job = jobs[i];

      // angles
      d.startAngle = groupStartAngle(d);
      d.endAngle = groupEndAngle(d);
    }

    // initialize begin / end angles for chord source / target
    for (var i = 0; i < chords.length; i++) {
      var d = chords[i];
      var s = d.source;
      var t = d.target;

      // associate jobs with chord source and target objects
      var sj = jobsByName[nameByIndex[s.index]];
      var tj = jobsByName[nameByIndex[t.index]];
      s.job = sj;
      t.job = tj;

      // determine chord source and target indices
      var si = sj.predecessorIndices[t.index];
      var ti = tj.successorIndices[s.index];

      // determine chord source out-degree and target in-degree
      var sn = d3.keys(sj.predecessorIndices).length;
      var tn = d3.keys(tj.successorIndices).length;
      s.startAngle = chordAngle(s, true, si, sn);
      s.endAngle = chordAngle(s, true, si + 1, sn);
      t.startAngle = chordAngle(t, false, ti, tn);
      t.endAngle = chordAngle(t, false, ti + 1, tn);
    }

    // select an svg g element for each group
    var g = svg.selectAll("g.group")
      .data(groups)
      .enter()
      .append("svg:g")
      .attr("class", "group");

    // add background arc to each g.group to support mouse interaction
    g.append("svg:path")
      .attr("class", "arc-mouse")
      .style("fill", "white")
      .style("stroke", "white")
      .attr("d", arcMouse)
      .on('mouseover', handleArcMouseOver)
      .on('click', handleArcClick);

    // add visual arc to each g.group
    g.append("svg:path")
      .attr("class", "arc")
      .style("fill", jobColor)
      .style("stroke", jobColor)
      .attr("d", arc);

    // add a label to each g.group
    g.append("svg:text")
      .each(function(d) { d.angle = (d.startAngle + d.endAngle) / 2; })
      .attr("dy", ".35em")
      .attr("text-anchor", null)
      .attr("transform", function(d) {
        return "rotate(" + (d.angle * 180 / Math.PI - 90) + ")"
          + "translate(" + (r0 + 26) + ")";
      })
      .text(function(d) { return d.index + 1; });

    // add chords
    svg.selectAll("path.chord")
      .data(chords)
      .enter()
      .append("svg:path")
      .attr("class", "chord")
      .style("stroke", chordStroke)
      .style("fill", chordFill)
      .attr("d", d3.svg.chord().radius(r0));
  }

  // once the dag is loaded we can initialize
  $( this.ui ).bind( "dagLoaded", function(event, data) {
    initialize(data.jobs);
  })

  /**
   * Select the given job and update global state.
   */
  $( this.ui ).bind( "jobSelected,JOB_STARTED", function(event, data) {
    refreshDisplay();
  })
}


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

AMBROSE.tableView = function (ui) {
  this.ui = ui;

  function loadTable(jobs) {
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
      updateTableRow(job);
    });
  }

  function updateTableRow(job) {
    var row = $('#row-num-' + job.index);
    $('.job-jt-url', row).text(job.jobId).attr('href', job.trackingUrl);
    $('.row-job-status', row).text(AMBROSE.util.value(job.status));
    $('.row-job-alias', row).text(AMBROSE.util.comma_join(job.aliases));
    $('.row-job-feature', row).text(AMBROSE.util.comma_join(job.features));
    $('.row-job-mappers', row).text(AMBROSE.util.task_progress_string(job.totalMappers, job.mapProgress));
    $('.row-job-reducers', row).text(AMBROSE.util.task_progress_string(job.totalReducers, job.reduceProgress));
  }

  $( this.ui ).bind( "dagLoaded", function(event, data) {
    loadTable(data.jobs);
  })

  $( this.ui ).bind( "jobSelected JOB_STARTED JOB_PROGRESS JOB_FAILED JOB_FINISHED", function(event, data) {
    updateTableRow(data.job);
  })
}


// TODO: the code below would be in-line in the HTML
var ui = new AMBROSE.ui();
new AMBROSE.chord(ui);
new AMBROSE.detailView(ui);
new AMBROSE.tableView(ui);

$(document).ready(function() {
  $(ui).initialize();
});
