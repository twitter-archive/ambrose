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
 * Ambrose module "chord" provides a chord diagram of the job graph.
 */
(function($, d3, colorbrewer, ambrose) {
  var chord = ambrose.chord = function(ui) {
    return new ambrose.chord.fn.init(ui);
  }

  // private reference to parent ui
  var _ui;

  // the svg graphic element, child of view
  var _svg;

  // job dependencies are visualized by chords
  var _diagram, _groups, _chords;

  // matrix which encodes job dependencies
  var _matrix = [];

  // group angle initialized once we know the number of jobs
  var _ga = 0, _ga2 = 0, _gap = 0;

  // radii of svg figure
  // TODO(Andy Schlaikjer): radius should be infered from parent element to
  // allow resizing of the figure when parent is resized
  var _r1 = 400 / 2;
  var _r0 = _r1 - 60;

  // color palette
  var _fill, _successFill, _errorFill;
  var _jobSelectedColor = d3.rgb(98, 196, 98);

  // returns start angle for a chord group
  function _groupStartAngle(d) {
    return  _ga * d.index - _ga2;
  }

  // returns end angle for a chord group
  function _groupEndAngle(d) {
    return _groupStartAngle(d) + _ga - _gap;
  }

  /**
   * @param d chord data
   * @param f boolean flag indicating chord is out-link
   * @param i chord in- / out-link index within current group
   * @param n in- / out-degree of current group
   */
  function _chordAngle(d, f, i, n) {
    var g = _groups[d.index];
    var s = g.startAngle;
    var e = g.endAngle;
    var r = (e - s) / 2;
    var ri = r / n;
    return s + r * (f ? 0 : 1) + ri * i;
  }

  // returns color for job arc and chord
  function _jobColor(d) {
    if (_isMouseOver(d.job)) {
      return d3.rgb(_jobMouseOverColor);
    } if (_ui.isSelected(d.job)) {
      return d3.rgb(_jobSelectedColor).brighter();
    } if (d.job.status == "RUNNING") {
      return d3.rgb(_jobSelectedColor);
    } if (d.job.status == "COMPLETE") {
      return _successFill(d.index);
    } if (d.job.status == "FAILED") {
      return _errorFill(d.index);
    }
    return d3.hsl(_fill(d.index)).darker(0.5);
  }

  // more color funcs
  function _chordStroke(d) { return d3.rgb(_jobColor(d.source)).darker(); }
  function _chordFill(d) { return _jobColor(d.source); }

  // mouse over job
  var _jobMouseOver;
  var _jobMouseOverColor = d3.rgb(20, 155, 223);

  function _handleArcMouseOver(d, i) {
    _jobMouseOver = d.job;
    this.refresh();
  }

  function _handleChartMouseOut(d, i) {
    _jobMouseOver = null;
    this.refresh();
  }

  function _isMouseOver(job) {
    return job === _jobMouseOver;
  }

  function _handleArcClick(d, i) {
    this.ui.selectJob(d.job);
    this.refresh();
  }

  function _handleJobStarted(event, data) {
    // TODO(Andy Schlaikjer): highlight the started job
  }

  function _handleJobFinished(event, data) {
    // TODO(Andy Schlaikjer): highlight the finished job
  }

  function _handleJobFailed(event, data) {
    // TODO(Andy Schlaikjer): highlight the failed job
  }

  function _handleJobSelected(event, data) {
    // TODO(Andy Schlaikjer): highlight the selected job
  }

  chord.fn = chord.prototype = $.extend(ambrose.chart(), {
    init: function(ui) {
      ambrose.chart.fn.init.call(this, ui, "chordView", "Chord");
      _ui = ui;
    },

    initChart: function(jobs) {
      var self = this;

      $('#chordView').append('<div class=\'row\'><div class=\'span6\' id=\'chordViewViz\'></div><div class=\'span6\'><table id="job-props" class="table"><thead></thead><tbody></tbody></table></div></div>');

      // jobs themselves are arc segments around the edge of the chord diagram
      var arcMouse = d3.svg.arc()
        .innerRadius(50)
        .outerRadius(_r0 + 300)
        .startAngle(_groupStartAngle)
        .endAngle(_groupEndAngle);
      var arc = d3.svg.arc()
        .innerRadius(_r0)
        .outerRadius(_r0 + 10)
        .startAngle(_groupStartAngle)
        .endAngle(_groupEndAngle);

      // set up canvas
      // TODO(Andy Schlaikjer): Is this safe in the presence of multiple view
      // impls which may want to add children to #chart element? Should this
      // instead reference the 'view' var?
      _svg = d3.select("#chordViewViz")
        .append("svg:svg")
        .attr("width", _r1 * 3)
        .attr("height", _r1 * 2)
        .on('mouseout', function(d, i) {
          _handleChartMouseOut.call(self, d, i);
        })
        .append("svg:g")
        .attr("transform", "translate(" + (_r1 * 1.5) + "," + _r1 + ")rotate(90)")
        .append("svg:g")
        .attr("transform", "rotate(0)");

      // initialize color palettes
      var n = jobs.length;
      if (n > 7) n = 7;
      _fill = d3.scale.ordinal().range(colorbrewer.Greys[n]);
      _successFill = d3.scale.ordinal().range(colorbrewer.Greens[n]);
      _errorFill = d3.scale.ordinal().range(colorbrewer.Reds[n]);

      // initialize group angles
      _ga = 2 * Math.PI / jobs.length;
      _gap = _ga * 0.1;
      _ga2 = (_ga - _gap) / 2;

      // localize some utility methods
      var findJobIndexByName = this.ui.findJobIndexByName;
      var findJobByName = this.ui.findJobByName;
      var findJobByIndex = this.ui.findJobByIndex;

      // Add predecessor and successor index maps to all jobs
      jobs.forEach(function (job) {
        job.predecessorIndices = {};
        job.successorIndices = {};
      });

      // Construct a square matrix counting dependencies
      for (var i = -1; ++i < jobs.length;) {
        var row = _matrix[i] = [];
        for (var j = -1; ++j < jobs.length;) {
          row[j] = 0;
        }
      }
      jobs.forEach(function(j) {
        var p = findJobIndexByName(j.name);
        j.successorNames.forEach(function(n) {
          var s = findJobIndexByName(n);
          _matrix[s][p]++;

          // initialize predecessor and successor indices
          j.successorIndices[s] = d3.keys(j.successorIndices).length;
          var sj = findJobByName(n);
          sj.predecessorIndices[p] = d3.keys(sj.predecessorIndices).length;
        });
      });

      // initialize chord diagram with job dependency matrix
      _diagram = d3.layout.chord();
      _diagram.matrix(_matrix);

      // override start and end angles for groups and chords
      _groups = _diagram.groups();
      _chords = _diagram.chords();

      // initialize groups
      for (var i = 0; i < _groups.length; i++) {
        var d = _groups[i];

        // associate group with job
        d.job = jobs[i];

        // angles
        d.startAngle = _groupStartAngle(d);
        d.endAngle = _groupEndAngle(d);
      }

      // initialize begin / end angles for chord source / target
      for (var i = 0; i < _chords.length; i++) {
        var d = _chords[i];
        var s = d.source;
        var t = d.target;

        // associate jobs with chord source and target objects
        var sj = findJobByIndex(s.index);
        var tj = findJobByIndex(t.index);
        s.job = sj;
        t.job = tj;

        // determine chord source and target indices
        var si = sj.predecessorIndices[t.index];
        var ti = tj.successorIndices[s.index];

        // determine chord source out-degree and target in-degree
        var sn = d3.keys(sj.predecessorIndices).length;
        var tn = d3.keys(tj.successorIndices).length;
        s.startAngle = _chordAngle(s, true, si, sn);
        s.endAngle = _chordAngle(s, true, si + 1, sn);
        t.startAngle = _chordAngle(t, false, ti, tn);
        t.endAngle = _chordAngle(t, false, ti + 1, tn);
      }

      // select an svg g element for each group
      var g = _svg.selectAll("g.group")
        .data(_groups)
        .enter()
        .append("svg:g")
        .attr("class", "group");

      // add background arc to each g.group to support mouse interaction
      g.append("svg:path")
        .attr("class", "arc-mouse")
        .style("fill", "white")
        .style("stroke", "white")
        .attr("d", arcMouse)
        .on('mouseover', function(d, i) {
          _handleArcMouseOver.call(self, d, i);
        })
        .on('click', function(d, i) {
          _handleArcClick.call(self, d, i);
        });

      // add visual arc to each g.group
      g.append("svg:path")
        .attr("class", "arc")
        .style("fill", _jobColor)
        .style("stroke", _jobColor)
        .attr("d", arc);

      // add a label to each g.group
      g.append("svg:text")
        .each(function(d) { d.angle = (d.startAngle + d.endAngle) / 2; })
          .attr("dy", ".35em")
        .attr("text-anchor", null)
        .attr("transform", function(d) {
          return "rotate(" + (d.angle * 180 / Math.PI - 90) + ")"
            + "translate(" + (_r0 + 26) + ")";
        })
        .text(function(d) { return d.index + 1; });

      // add chords
      _svg.selectAll("path.chord")
        .data(_chords)
        .enter()
        .append("svg:path")
        .attr("class", "chord")
        .style("stroke", _chordStroke)
        .style("fill", _chordFill)
        .attr("d", d3.svg.chord().radius(_r0));
    },

    refresh: function(event, data) {
      // update path.arc elements
      _svg.selectAll("path.arc")
        .transition()
        .style("fill", _jobColor)
        .style("stroke", _jobColor);

      // update path.chord elements
      _svg.selectAll("path.chord")
        .transition()
        .style("stroke", _chordStroke)
        .style("fill", _chordFill);
    }
  });

  // set the init function's prototype for later instantiation
  chord.fn.init.prototype = chord.fn;

}(jQuery, d3, colorbrewer, AMBROSE));
